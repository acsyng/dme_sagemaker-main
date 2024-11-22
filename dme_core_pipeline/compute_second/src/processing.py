import argparse
import json
import os
import boto3

import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructField,
    StructType,
    DoubleType,
    IntegerType,
    StringType,
    BooleanType,
    NullType,
)

import libs.dme_statistical_tests as dme
from libs.config.config_vars import CONFIG
from libs.dme_pyspark_sql_queries import (
    merge_trial_h2h,
    merge_trial_cpifl,
    merge_trial_config,
    merge_pvs_cpifl,
    merge_pvs_cpifl_regression,
    merge_pvs_config,
    merge_pvs_regression_input,
)
from libs.event_bridge.event import error_event, create_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.metric_utils import (
    create_empty_out,
    prepare_rating_metric_input,
    run_metrics,
)
from libs.regression_utils import reg_adjust_parallel_rm_pyspark


def main():
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument(
        "--s3_input_pvs_input",
        type=str,
        help="s3 input pvs_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_trial_checks",
        type=str,
        help="s3 input trial_check bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_metric_config",
        type=str,
        help="s3 input metric_config bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_regression_cfg",
        type=str,
        help="s3 input regression_cfg bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_pvs_output",
        type=str,
        help="s3 input pvs_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_trial_text_input",
        type=str,
        help="s3 input trial_text_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_trial_numeric_input",
        type=str,
        help="s3 input trial_numeric_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_trial_numeric_output",
        type=str,
        help="s3 output trial_numeric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_trial_text_output",
        type=str,
        help="s3 output trial_text_output bucket",
        required=True,
    )
    args = parser.parse_args()
    with open(
        "/opt/ml/processing/input/input_variables/query_variables.json", "r"
    ) as f:
        data = json.load(f)
        ap_data_sector = data["ap_data_sector"]
        analysis_year = data["analysis_year"]
        analysis_run_group = data["analysis_run_group"]
        pipeline_runid = data["target_pipeline_runid"]
        current_source_ids = data["source_ids"]
        logger = CloudWatchLogger.get_logger()
        logger.info(f"compute_second query_variables.json: {data}")
        try:
            spark = SparkSession.builder.appName("PySparkApp").getOrCreate()
            # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "mapred.output.committer.class",
                "org.apache.hadoop.mapred.FileOutputCommitter",
            )
            spark.sparkContext._jsc.hadoopConfiguration().setBoolean(
                "fs.s3a.sse.enabled", True
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.server-side-encryption-algorithm", "SSE-KMS"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.sse.kms.keyId", CONFIG["output_kms_key"]
            )

            n_part = (current_source_ids.count(",") + 1) * 2

            create_metric_cfg(analysis_year, ap_data_sector, args, logger, spark)

            compute_pvs_output(
                analysis_year,
                ap_data_sector,
                n_part,
                logger,
                spark,
                args,
            )
            compute_trial_text_output(logger, spark, args)
            compute_trial_numeric_output(n_part, logger, spark, args)
        except Exception as e:
            logger.error(e)
            error_event(
                ap_data_sector,
                analysis_year,
                pipeline_runid,
                str(e),
                analysis_run_group,
            )
            raise e


def create_metric_cfg(analysis_year, ap_data_sector, args, logger, spark):
    metric_cfg_df = spark.read.csv(
        args.s3_input_metric_config, inferSchema=True, header=True
    )
    metric_cfg_df = metric_cfg_df.filter(
        "ap_data_sector = '{0}' AND analysis_year = {1}".format(
            ap_data_sector, analysis_year
        )
    ).drop_duplicates()
    metric_cfg_df.createOrReplaceTempView("metric_cfg")
    logger.info(
        f"compute_second: args.s3_input_metric_config ="
        f"{args.s3_input_metric_config}"
    )
    logger.info(f"compute_second: metric_cfg_df count ={metric_cfg_df.count()}")


def compute_trial_numeric_output(n_partitions, logger, spark, args):
    logger.info(
        "compute_trial_numeric_output:====START compute_trial_numeric_output===="
    )
    ### Read recipe inputs
    cpifl_table_df = spark.read.parquet(args.s3_input_trial_checks)
    logger.info(
        f"compute_trial_numeric_output: cpifl_table_df data count ={cpifl_table_df.count()}"
    )

    regression_cfg_df = spark.read.option("header", True).csv(
        args.s3_input_regression_cfg
    )
    regression_cfg_df.createOrReplaceTempView("regression_cfg")

    # check if a file exists on s3. fpath = s3 path after bucket
    # fpath ='uat/dme/performance/reformatting_performance_pipeline_temp_out/data/SOY_BRAZIL_SUMMER/adv_model_training_data.csv' for example
    # no '/' at front of fpath (NOT 'uat'/dme/...)
    # apparently works with folders
    def check_if_file_exists_s3(fname, bucket=None):
        # fpath is the full file name, not including the bucket.
        s3_client = boto3.client("s3")
        res = s3_client.list_objects_v2(Bucket=bucket, Prefix=fname, MaxKeys=1)
        return "Contents" in res

    s = args.s3_input_trial_numeric_input
    bucket = CONFIG["bucket"]
    s_to_check = s.split(bucket)[-1][1:]

    # if Path(args.s3_input_trial_numeric_input).exists():
    if check_if_file_exists_s3(fname=s_to_check, bucket=bucket):
        trial_numeric_df = spark.read.parquet(args.s3_input_trial_numeric_input)

        ### Set recipe variables
        alpha = 0.3

        gr_cols = ["ap_data_sector", "analysis_year", "trial_id", "x", "y"]
        cols = [
            "ap_data_sector",
            "analysis_year",
            "trial_id",
            "be_bid",
            "year",
            "experiment_id",
            "x",
            "y",
            "function",
            "plot_barcode",
            "trait",
            "prediction_x",
            "prediction",
            "analysis_target_y",
            "trial_pts",
            "analysis_pts",
            "adjusted_prediction",
            "adj_model",
            "adj_outlier",
            "p_value",
            "slope1",
            "slope2",
            "intercept",
            "residual",
            "adjusted",
        ]

        col_partitions = [
            "ap_data_sector",
            "analysis_year",
            "decision_group",
            "breakout_level",
            "breakout_level_value",
        ]

        metric_input_cols = [
            "ap_data_sector",
            "analysis_year",
            "analysis_type",
            "decision_group",
            "be_bid",
            "material_type",
            "breakout_level",
            "breakout_level_value",
            "trial_id",
            "trait",
            "result_numeric_value",
            "metric_name",
            "cpifl",
            "chkfl",
            "distribution_type",
            "direction",
            "threshold_factor",
            "spread_factor",
            "weight",
            "adv_weight",
            "incl_pctchk",
            "adv_incl_pctchk",
        ]

        gr_cols2 = [
            "ap_data_sector",
            "analysis_year",
            "analysis_type",
            "decision_group",
            "be_bid",
            "material_type",
            "breakout_level",
            "breakout_level_value",
            "trait",
            "metric_name",
            "distribution_type",
            "direction",
        ]

        logger.info(
            f"compute_trial_numeric_output: trial_numeric_df data "
            f"count ={trial_numeric_df.count()}"
        )

        ## Bypass regression
        trial_numeric_df.repartition(
            n_partitions, col_partitions
        ).createOrReplaceTempView("tr_data1")
        cpifl_table_df.createOrReplaceTempView("cpifl_table")

        # Use cpifl table to get parentage, and then create trial pheno data for parents and append to entry-level data
        trial_metric_input_df = merge_trial_cpifl(spark, "numeric")
        logger.info(
            f"compute_trial_numeric_output: after merge_trial_cpifl trial_metric_input_df "
            f"count={trial_metric_input_df.count()}"
        )

        trial_window = Window.partitionBy("trial_id", "breakout_level")
        trial_metric_input_df = (
            trial_metric_input_df.withColumn(
                "mincpi", F.min("cpifl").over(trial_window)
            )
            .where(False == F.col("mincpi"))
            .drop("mincpi")
        )

        trial_metric_input_df.createOrReplaceTempView("trc_data3")

        logger.info(
            f"===compute_trial_numeric_output: BEFORE metric config merge,source ids="
            f'{trial_metric_input_df.select("decision_group").distinct().collect()}'
        )

        # Apply metric config
        trial_metric_input_df = merge_trial_config(spark, "numeric")

        logger.info(
            f"===compute_trial_numeric_output: AFTER metric config merge,source ids="
            f'{trial_metric_input_df.select("decision_group").distinct().collect()}'
        )

        trial_metric_input_df.createOrReplaceTempView("trial_pheno_metric_input")

        # Create metric_type = threshold/rating input
        # Rating metric input
        trial_rating_metric_input_df = prepare_rating_metric_input(
            trial_metric_input_df, metric_input_cols, gr_cols2
        )
        logger.info(
            f"compute_trial_numeric_output: After prepare_rating_metric_input count="
            f"{trial_rating_metric_input_df.count()}"
        )

        trial_metric_input_df = trial_metric_input_df.filter(
            "distribution_type != 'rating'"
        ).persist(StorageLevel.MEMORY_AND_DISK)

        # Create metric_type = pct_check output
        # create h2h structure
        h2h_input = merge_trial_h2h(spark, trial_metric_input_df)
        logger.info(
            f"compute_trial_numeric_output: After merge_trial_h2h, count={h2h_input.count()}"
        )

        h2h_input = h2h_input.unionByName(
            trial_rating_metric_input_df, allowMissingColumns=True
        )
        logger.info(
            f"compute_trial_numeric_output: After unionByName, count={h2h_input.count()}"
        )

        if (h2h_input.count() == 0) | (trial_numeric_df.count() < 10):
            logger.info(
                "compute_trial_numeric_output: inside IF shape size is zero during metric compute"
            )
            h2h_output = create_empty_out(spark).pandas_api()
        else:
            logger.info(
                "compute_trial_numeric_output: inside ELSE shape size is NOT zero during metric compute"
            )

            h2h_output = (
                h2h_input.pandas_api()
                .pandas_on_spark.apply_batch(run_metrics)
                .dropna(axis=1, how="all")
            )

        logger.info(f"compute_trial_numeric_output: result_count:{h2h_output.shape[0]}")
        logger.info(
            f"compute_trial_numeric_output: result decision_group_count:{h2h_output.decision_group.nunique()}"
        )

        h2h_output.to_parquet(args.s3_output_trial_numeric_output)

        # Cleanup
        spark.catalog.dropTempView("trial_pheno_metric_input")
        trial_metric_input_df.unpersist()
        spark.catalog.dropTempView("trc_data3")
        spark.catalog.dropTempView("tr_data2")
        spark.catalog.dropTempView("tr_data1")

        logger.info(
            "compute_trial_numeric_output:====END compute_trial_numeric_output===="
        )
    else:
        h2h_output = create_empty_out(spark).pandas_api()
        h2h_output.to_parquet(args.s3_output_trial_numeric_output)


def compute_trial_text_output(logger, spark, args):
    logger.info("compute_trial_text_output:====START compute_trial_text_output====")
    ### Read recipe inputs
    logger.info(f"s3_input_trial_text_input={args.s3_input_trial_text_input}")
    trial_text_df = spark.read.options(header=True, inferSchema=True).csv(
        args.s3_input_trial_text_input
    )
    trial_text_df.createOrReplaceTempView("tr_data1")
    logger.info(
        f"compute_trial_text_output: input_trial_text_input count:{trial_text_df.count()}"
    )

    cpifl_table_df = spark.read.parquet(args.s3_input_trial_checks)
    cpifl_table_df.createOrReplaceTempView("cpifl_table")

    logger.info("compute_trial_text_output:read cpifl data")
    # Run metrics
    if trial_text_df.count() == 0:
        logger.info(
            "compute_trial_text_output:INSIDE IF when  trial_text_df count is zero"
        )
        df = create_empty_out(spark)
        logger.info(
            f"compute_trial_text_output: if condition result_count:{df.count()}"
        )
        # Write recipe outputs
        df.write.mode("overwrite").parquet(args.s3_output_trial_text_output)
    else:
        df = merge_trial_cpifl(spark, "alpha")
        logger.info(f"compute_trial_text_output: merge_trial_cpifl count={df.count()}")

        df.createOrReplaceTempView("trc_data3")
        df = merge_trial_config(spark, "alpha")
        logger.info(f"compute_trial_text_output: merge_trial_config count={df.count()}")

        def run_metrics(df):
            (
                df["pctchk"],
                df["statistic"],
                df["sem"],
                df["metric_value"],
            ) = dme.text_tests(
                df["result_alpha_value"].array,
                df["text_factor"].array,
                df["direction"].array,
            )

            df["metric_method"] = "text_bool"
            return df

        df = df.pandas_api().pandas_on_spark.apply_batch(run_metrics)
        logger.info("compute_trial_text_output:after execute run_metrics")

        if df.shape[0] == 0:
            logger.info(
                "compute_trial_text_output:INSIDE IF when  trial_text_df count is zero"
            )
            df = create_empty_out(spark)
            logger.info(
                f"compute_trial_text_output: if condition result_count:{df.count()}"
            )
            # Write recipe outputs
            df.write.mode("overwrite").parquet(args.s3_output_trial_text_output)
        else:

            logger.info(
                f"compute_trial_text_output: after run_metrics result_count:{df.shape[0]}"
            )
            df.to_parquet(args.s3_output_trial_text_output)

        # Cleanup
        spark.catalog.dropTempView("trc_data3")
        spark.catalog.dropTempView("tr_data2")
        spark.catalog.dropTempView("tr_data1")
        logger.info("compute_trial_text_output:====END compute_trial_text_output====")


def compute_pvs_output(
    analysis_year, ap_data_sector, n_partitions, logger, spark, args
):
    # Set variables
    alpha = 0.3

    gr_cols = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "decision_group",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "x",
        "y",
    ]

    gr_cols2 = [
        "ap_data_sector",
        "analysis_type",
        "analysis_year",
        "decision_group",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trait",
    ]

    id_cols = ["be_bid", "count", "prediction", "stddev", "chkfl"]

    col_partitions = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "decision_group",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trait",
    ]

    # Read inputs
    logger.info("compute_pvs_output:Read s3_input_pvs_input")
    pvs_input_df = spark.read.parquet(args.s3_input_pvs_input).repartition(
        n_partitions, col_partitions
    )
    pvs_input_df.createOrReplaceTempView("pvs_input")
    logger.info(f"compute_pvs_output: pvs_input_df count={pvs_input_df.count()}")

    logger.info("compute_pvs_output:Read s3_input_trial_checks")
    trial_checks_df = spark.read.parquet(args.s3_input_trial_checks)
    trial_checks_df.createOrReplaceTempView("cpifl_table")
    logger.info(f"compute_pvs_output: checks_df count={trial_checks_df.count()}")

    logger.info("compute_pvs_output:Read s3_input_regression_cfg")
    regression_cfg_df = spark.read.option("header", True).csv(
        args.s3_input_regression_cfg
    )
    regression_cfg_df = regression_cfg_df.filter(
        "ap_data_sector = '{0}' AND analysis_year = {1}".format(
            ap_data_sector, analysis_year
        )
    )
    regression_cfg_df.createOrReplaceTempView("regression_cfg")
    logger.info(
        f"compute_pvs_output: regression_cfg_df count={regression_cfg_df.count()}"
    )

    # Debug code
    # pvs_input_df = pvs_input_df.filter(pvs_input_df.decision_group == "2023_LT_LATE_STG5_MG7_2_HEATSTRESS_SNYR")

    if regression_cfg_df.count() > 10000:
        logger.info("compute_pvs_output: merge_pvs_regression_input")
        regression_input = merge_pvs_regression_input(spark)
        regression_input = regression_input.pandas_api()

        pvs_regression_output_df = regression_input.groupby(gr_cols).apply(
            reg_adjust_parallel_rm_pyspark, alpha=alpha
        )

        if pvs_regression_output_df.shape[0] > 0:
            logger.info("compute_pvs_output: run regression")
            pvs_regression_output_df = pvs_regression_output_df.loc[
                pvs_regression_output_df.adjusted == "Yes"
            ].to_spark()
            pvs_regression_output_df.createOrReplaceTempView("pvs_reg_output")
            pvs_metric_raw_df = merge_pvs_cpifl_regression(spark)
            spark.catalog.dropTempView("pvs_reg_output")
        else:
            logger.info("compute_pvs_output: skip regression")
            pvs_metric_raw_df = merge_pvs_cpifl(spark)
    else:
        logger.info("compute_pvs_output: inside else")
        pvs_metric_raw_df = merge_pvs_cpifl(spark)

    pvs_window = Window.partitionBy("decision_group", "breakout_level")
    pvs_window2 = Window.partitionBy("decision_group")
    pvs_metric_raw_df = pvs_metric_raw_df.withColumns(
        {
            "cpifl": F.when(
                (F.max(pvs_metric_raw_df.cpifl).over(pvs_window) == False)
                & (F.max(pvs_metric_raw_df.cpifl).over(pvs_window2) == True)
                & (pvs_metric_raw_df.material_type == "entry"),
                True,
            ).otherwise(pvs_metric_raw_df.cpifl),
            "chkfl": F.when(
                (F.max(pvs_metric_raw_df.chkfl).over(pvs_window) == False)
                & (F.max(pvs_metric_raw_df.chkfl).over(pvs_window2) == True)
                & (pvs_metric_raw_df.material_type == "entry"),
                pvs_metric_raw_df.cpifl,
            ).otherwise(pvs_metric_raw_df.chkfl),
        }
    )

    # Compute recipe outputs
    pvs_metric_raw_df.createOrReplaceTempView("pvs_metric_raw")

    logger.info(
        f"compute_pvs_output: BEFORE metric_config merge,source id count="
        f'{pvs_metric_raw_df.select("decision_group").distinct().count()}'
    )
    logger.info(
        f"compute_pvs_output: BEFORE metric_config merge,row count="
        f"{pvs_metric_raw_df.count()}"
    )

    # Apply metric config
    pvs_df = merge_pvs_config(spark, pvs_metric_raw_df, gr_cols2)
    pvs_df.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(
        f"compute_pvs_output: AFTER metric_config merge,source id count="
        f'{pvs_df.select("decision_group").distinct().count()}'
    )
    logger.info(
        f"compute_pvs_output: AFTER metric_config merge,row count={pvs_df.count()}"
    )

    # Prepare h2h + rating format
    pvs_rating_df = pvs_df.filter(pvs_df.distribution_type == "rating").alias(
        "pvs_rating_df"
    )

    pvs_df_check = (
        pvs_df.filter((pvs_df.cpifl == True) & (pvs_df.distribution_type != "rating"))
        .select(gr_cols2 + id_cols)
        .withColumnsRenamed(
            {
                "be_bid": "check_be_bid",
                "count": "check_count",
                "prediction": "check_prediction",
                "stddev": "check_stddev",
                "chkfl": "check_chkfl",
            }
        )
        .alias("pvs_df_check")
    )

    pvs_df = pvs_df.filter(pvs_df.distribution_type != "rating").join(
        pvs_df_check,
        on=gr_cols2,
        how="inner",
    )

    pvs_df = pvs_df.unionByName(pvs_rating_df, allowMissingColumns=True)
    logger.info(f"compute_pvs_output: after h2h unionbyname count={pvs_df.count()}")

    # compute final metrics
    logger.info("compute_pvs_output: compute metrics")
    if pvs_df.count() == 0:
        logger.info(
            "compute_pvs_output: inside IF shape size is zero during metric compute"
        )
        pvs_df = create_empty_out(spark).pandas_api()
    else:
        logger.info(
            "compute_pvs_output: inside ELSE shape size is NOT zero during metric compute"
        )

        pvs_df = pvs_df.pandas_api().pandas_on_spark.apply_batch(run_metrics)

    logger.info(f"compute_pvs_output: result_count:{pvs_df.shape[0]}")
    logger.info(
        f"compute_pvs_output: result decision_group_count:{pvs_df.decision_group.nunique()}"
    )

    for col_name in pvs_df.columns:
        if (pvs_df[col_name].count() == 0) & (col_name == "check_be_bid"):
            pvs_df[col_name] = ""
        if (pvs_df[col_name].count() == 0) & (col_name == "check_chkfl"):
            pvs_df[col_name] = False

    pvs_df.to_parquet(args.s3_output_pvs_output)

    # Cleanup
    spark.catalog.dropTempView("pvs_metric_raw")
    spark.catalog.dropTempView("pvs_input")
    spark.catalog.dropTempView("regression_cfg")

    logger.info("compute_pvs_output:====END compute_pvs_output====")


if __name__ == "__main__":
    main()
