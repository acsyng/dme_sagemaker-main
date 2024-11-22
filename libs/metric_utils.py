import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    BooleanType,
)
import pandas as pd, numpy as np, polars as pl
import libs.dme_statistical_tests as dme
import pyspark.pandas as ps


def create_empty_out(spark):
    """Creates empty pyspark df for any metric workflow

    Creates an empty pyspark df with the appropriate schema to use as a stand-in
    when there is no input data for a metric workflow (pvs/trial numeric / trial text)

    args:
    - spark: spark instance

    return:
    - df: empty df with appropriate column headers & types

    ACTIVE 20240621
    """
    list_of_cols = [
        StructField("ap_data_sector", StringType(), False),
        StructField("analysis_type", StringType(), False),
        StructField("analysis_year", IntegerType(), False),
        StructField("decision_group", StringType(), False),
        StructField("material_type", StringType(), False),
        StructField("breakout_level", StringType(), False),
        StructField("breakout_level_value", StringType(), False),
        StructField("be_bid", StringType(), False),
        StructField("trait", StringType(), False),
        StructField("count", IntegerType(), False),
        StructField("prediction", FloatType(), False),
        StructField("stddev", FloatType(), False),
        StructField("cpifl", BooleanType(), False),
        StructField("chkfl", BooleanType(), False),
        StructField("metric_name", StringType(), False),
        StructField("weight", FloatType(), False),
        StructField("adv_weight", FloatType(), False),
        StructField("incl_pctchk", IntegerType(), False),
        StructField("adv_incl_pctchk", IntegerType(), False),
        StructField("check_be_bid", StringType(), False),
        StructField("check_count", IntegerType(), False),
        StructField("check_prediction", FloatType(), False),
        StructField("check_stddev", FloatType(), False),
        StructField("check_chkfl", BooleanType(), False),
        StructField("pctchk", FloatType(), False),
        StructField("statistic", FloatType(), False),
        StructField("sem", FloatType(), False),
        StructField("metric_value", FloatType(), False),
        StructField("metric_method", StringType(), True),
    ]
    schema = StructType(list_of_cols)
    df = spark.createDataFrame([], schema=schema)

    return df


def prepare_rating_metric_input(input_df, metric_input_cols, gr_cols):
    """Performs groupby on trial-level data to prepare for applying t-tests

    args:
    - input_df: spark df
    - metric_input_cols: Columns used for grouping or calculations
    - gr_cols: Grouping columns

    return: output_df: spark df that can then be used in run_metrics()
    """
    input_df = input_df.filter("distribution_type = 'rating'")
    output_df = (
        input_df.select(metric_input_cols)
        .groupBy(gr_cols)
        .agg(
            F.count("result_numeric_value").alias("count"),
            F.mean("result_numeric_value").alias("prediction"),
            F.stddev_samp("result_numeric_value").alias("stddev"),
            F.max("cpifl").alias("cpifl"),
            F.max("chkfl").alias("chkfl"),
            F.round(F.mean("threshold_factor"), 4).alias("threshold_factor"),
            F.round(F.mean("spread_factor"), 4).alias("spread_factor"),
            F.round(F.mean("weight"), 4).alias("weight"),
            F.round(F.mean("adv_weight"), 4).alias("adv_weight"),
            F.max("incl_pctchk").alias("incl_pctchk"),
            F.max("adv_incl_pctchk").alias("adv_incl_pctchk"),
        )
    )

    return output_df


def create_check_df(analysis_run_group: str, checks_df: pd.DataFrame) -> pd.DataFrame:
    """Converts check usage report into check flags for entries and parents

    args:
    - analysis_run_group: Type of analysis being run in this DME job
    - checks_df: Check data pulled from db

    return: checks_df: postprocessed check df
    """

    check_cols = ["cpifl", "cperf", "cagrf", "cmatf", "cregf", "crtnf"]

    group_cols = ["ap_data_sector", "analysis_year", "decision_group", "material_type"]
    group_cols2 = ["ap_data_sector", "analysis_year", "decision_group", "be_bid"]

    # If GCA analysis is present, generate parent check information
    if (analysis_run_group in ["genomic_prediction", "late_phenogca"]) & (
        checks_df.shape[0] > 0
    ):
        par_checks_df = pl.concat(
            [
                checks_df.filter(
                    (pl.col("par1_be_bid") != "")
                    & (pl.col("be_bid") != pl.col("par1_be_bid"))
                )
                .drop(["be_bid", "par2_be_bid"])
                .rename({"par1_be_bid": "be_bid"})
                .group_by(
                    group_cols2,
                )
                .max()
                .with_columns(
                    pl.col("material_type_par1").alias("material_type"),
                    pl.lit("").alias("par1_be_bid"),
                    pl.lit("").alias("par2_be_bid"),
                ),
                checks_df.filter(
                    (pl.col("par2_be_bid") != "")
                    & (pl.col("be_bid") != pl.col("par2_be_bid"))
                )
                .drop(["be_bid", "par1_be_bid"])
                .rename({"par2_be_bid": "be_bid"})
                .group_by(
                    group_cols2,
                )
                .max()
                .with_columns(
                    pl.col("material_type_par2").alias("material_type"),
                    pl.lit("").alias("par1_be_bid"),
                    pl.lit("").alias("par2_be_bid"),
                ),
            ],
        )

        # if no ltb is defined for a pool within a decision group, copy the other pool's values over to make sure we have SOME checks.
        ltb_mask_fp = par_checks_df.select(
            (pl.col("fp_ltb").max().over(group_cols) == False).alias("ltb_mask_fp")
        ).get_column("ltb_mask_fp")

        ltb_mask_mp = par_checks_df.select(
            (pl.col("mp_ltb").max().over(group_cols) == False).alias("ltb_mask_mp")
        ).get_column("ltb_mask_mp")

        ltb_mask_bp = par_checks_df.select(
            (
                (pl.col("mp_ltb").max().over(group_cols) == False)
                & (pl.col("fp_ltb").max().over(group_cols) == False)
            ).alias("ltb_mask_bp")
        ).get_column("ltb_mask_bp")

        # convert hybrid-level ltb info to parental line ltb
        par_checks_df = par_checks_df.with_columns(
            pl.when(ltb_mask_bp)
            .then(pl.col("cpifl"))
            .when(ltb_mask_fp)
            .then(pl.col("mp_ltb"))
            .otherwise(pl.col("fp_ltb"))
            .alias("fp_ltb"),
            pl.when(ltb_mask_bp)
            .then(pl.col("cpifl"))
            .when(ltb_mask_mp)
            .then(pl.col("fp_ltb"))
            .otherwise(pl.col("mp_ltb"))
            .alias("mp_ltb"),
        ).with_columns(
            pl.when(pl.col("material_type").is_in(["female", "pool1"]))
            .then(pl.col("fp_ltb"))
            .when(pl.col("material_type").is_in(["male", "pool2"]))
            .then(pl.col("mp_ltb"))
            .otherwise(False)
            .alias("ltb")
        )

        par_checks_df = par_checks_df.with_columns(
            (pl.col(c) & pl.col("ltb")).alias(c) for c in check_cols
        )

        par_checks_df = par_checks_df.drop(["ltb", "fp_ltb", "mp_ltb"])

        # Generate untested entries if that option has been specified for Genopred
        untested_ent_par1_df = (
            par_checks_df.filter(
                (pl.col("untested_entry_display"))
                & (pl.col("material_type") == "pool1")
            )
            .select(group_cols2)
            .rename({"be_bid": "par1_be_bid"})
        )

        untested_ent_par2_df = (
            par_checks_df.filter(
                (pl.col("untested_entry_display"))
                & (pl.col("material_type") == "pool2")
            )
            .select(group_cols2)
            .rename({"be_bid": "par2_be_bid"})
        )

        untested_ent_df = untested_ent_par1_df.join(
            untested_ent_par2_df,
            on=["ap_data_sector", "analysis_year", "decision_group"],
        ).with_columns(
            pl.concat_str(
                [pl.col("par1_be_bid"), pl.col("par2_be_bid")], separator="/"
            ).alias("be_bid"),
            pl.lit(False).alias("cpifl"),
            pl.lit(False).alias("cperf"),
            pl.lit(False).alias("cagrf"),
            pl.lit(False).alias("cmatf"),
            pl.lit(False).alias("cregf"),
            pl.lit(False).alias("crtnf"),
            pl.lit("untested_entry").alias("material_type"),
        )

        untested_ent_df = (
            untested_ent_df.with_columns(pl.lit(True).alias("untested"))
            .join(
                checks_df.with_columns(pl.lit(True).alias("existing")).select(
                    [
                        "ap_data_sector",
                        "analysis_year",
                        "decision_group",
                        "par1_be_bid",
                        "par2_be_bid",
                        "existing",
                    ]
                ),
                on=[
                    "ap_data_sector",
                    "analysis_year",
                    "decision_group",
                    "par1_be_bid",
                    "par2_be_bid",
                ],
                how="outer",
                coalesce=True,
            )
            .filter(pl.col("untested") & (pl.col("existing").not_()))
            .drop(["untested", "existing"])
        )

        checks_df = checks_df.drop(
            [
                "fp_ltb",
                "mp_ltb",
                "untested_entry_display",
                "material_type_par1",
                "material_type_par2",
            ]
        )
        par_checks_df = par_checks_df.drop(
            ["untested_entry_display", "material_type_par1", "material_type_par2"]
        )

        checks_df = pl.concat(
            [
                checks_df,
                par_checks_df,
                untested_ent_df.select(
                    (
                        group_cols2
                        + check_cols
                        + ["material_type", "par1_be_bid", "par2_be_bid"]
                    )
                ),
            ],
            how="diagonal",
        )

    else:  # No GCA analyses
        checks_df = checks_df.drop(
            [
                "fp_ltb",
                "mp_ltb",
                "untested_entry_display",
            ]
        )

    # correct for no-check cases
    checks_df = checks_df.with_columns(
        pl.when(
            (pl.col("cpifl").max().over(group_cols) == False)
            & (pl.col("cpifl").count().over(group_cols) < 500)
            & (pl.col("material_type") == "entry")
        )
        .then(True)
        .otherwise(pl.col("cpifl"))
        .alias("cpifl")
    ).with_columns(
        pl.when(
            (pl.col(c).max().over(group_cols) == False)
            & (pl.col("material_type") == "entry")
        )
        .then(pl.col("cpifl"))
        .otherwise(pl.col(c))
        .alias(c)
        for c in check_cols
    )

    return checks_df


def run_metrics(
    df,
):
    """Runs statistical tests on trial-generated input, then formats output"""
    # -> ps.DataFrame[
    #     "ap_data_sector":str,
    #     "analysis_year":int,
    #     "analysis_type":str,
    #     "decision_group":str,
    #     "material_type":str,
    #     "stage":float,
    #     "decision_group_rm":float,
    #     "breakout_level":str,
    #     "breakout_level_value":str,
    #     "be_bid":str,
    #     "trait":str,
    #     "check_be_bid":str,
    #     "count":int,
    #     "check_count":float,
    #     "prediction":float,
    #     "check_prediction":float,
    #     "stddev":float,
    #     "check_stddev":float,
    #     "cpifl":boolean,
    #     "chkfl":boolean,
    #     "check_chkfl":int,
    #     "metric_name":str,
    #     "weight":float,
    #     "adv_weight":float,
    #     "pctchk":float,
    #     "statistic":float,
    #     "sem":float,
    #     "metric_value":float,
    #     "metric_method":str,
    # ]:
    cols_to_round = [
        "prediction",
        "check_prediction",
        "stddev",
        "check_stddev",
    ]
    cols_to_drop = [
        "distribution_type",
        "direction",
        "threshold_factor",
        "spread_factor",
    ]
    metric_cols = [
        "pctchk",
        "statistic",
        "sem",
        "metric_value",
    ]

    (
        df["pctchk"],
        df["statistic"],
        df["sem"],
        df["metric_value"],
        df["metric_method"],
    ) = dme.trial_mean_tests(
        df.prediction.values,
        df.check_prediction.values,
        df.stddev.values,
        df.check_stddev.values,
        df["count"].values,
        df.check_count.values,
        df["metric_name"].values,
        df.threshold_factor.values,
        df.spread_factor.values,
        df["direction"].values,
        df["distribution_type"].values,
    )

    df[cols_to_round] = df[cols_to_round].round(4)
    df[metric_cols] = df[metric_cols].round(8)
    df = df.drop(labels=cols_to_drop, axis=1)
    df[["analysis_year", "count"]] = df[["analysis_year", "count"]].astype("int")
    return df
