import argparse
import json
import os
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, lit, mean, stddev, when, col, sum
import boto3
from libs.config.config_vars import CONFIG, S3_BUCKET
from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_grid_performance', type=str,
                        help='s3 input grid performance', required=True)
    parser.add_argument('--s3_input_grid_classification', type=str,
                        help='s3 input grid classification', required=True)
    parser.add_argument('--s3_output_temporal_aggregate_query', type=str,
                        help='s3 output temporal aggregate query', required=True)

    args = parser.parse_args()
    print('args collected ')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

        # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
        spark.sparkContext._jsc.hadoopConfiguration().set('mapred.output.committer.class',
                                                          'org.apache.hadoop.mapred.FileOutputCommitter')
        spark.sparkContext._jsc.hadoopConfiguration().setBoolean('fs.s3a.sse.enabled', True)
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.sse.kms.keyId', CONFIG['output_kms_key'])

        # years = ['2023']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            maturity_list = ['MG0', 'MG1', 'MG2', 'MG3', 'MG4', 'MG5', 'MG6', 'MG7', 'MG8']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            maturity_list = ['6C', '6L', '6M', '7C', '7L', '7M', '8C', '5L', '8M', '5M', '5C']  # , '4L']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            s3 = boto3.resource('s3')
            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)
            fp_list = list()
            file_p = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_grid_performance/data/grid_performance',
                                  DKU_DST_ap_data_sector, str(DKU_DST_analysis_year), pipeline_runid)
            for obj in bucket.objects.filter(Prefix=file_p):
                fp = obj.key.replace(os.path.join(file_p, ''), '').split('/')[0]
                fp_list.append(fp)

            maturity_list = list(set(fp_list))
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7']
        # if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
        #     maturity_list = ['1', '2', '3', '4', '5', '6', '7']

        for input_year in years:
            for maturity in maturity_list:
                file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                         'compute_temporal_aggregate_query/data/temporal_aggregate_query',
                                         DKU_DST_ap_data_sector,
                                         input_year, maturity, 'temporal_aggregate_query.parquet')

                # check_file_exists = check_if_file_exists_s3(file_path)
                # if check_file_exists is False:
                print('Creating file in the following location: ', file_path)
                # regional_aggregate_function(DKU_DST_ap_data_sector, input_year, maturity, pipeline_runid, args=args)
                # print('File created')
                #    print()

                # else:
                #    print('File exists here: ', file_path)
                try:
                    # Read recipe inputs
                    print('args.s3_input_grid_performance: ', args.s3_input_grid_performance)
                    grid_performance_df = spark.read.parquet(
                        os.path.join(args.s3_input_grid_performance, DKU_DST_ap_data_sector, input_year, pipeline_runid,
                                     maturity, 'grid_performance.parquet'))
                    grid_performance_df.createOrReplaceTempView("grid_performance")

                    dg_be_bids_df = spark.read.parquet(
                        os.path.join(args.s3_input_grid_classification, DKU_DST_ap_data_sector, input_year,
                                     'dg_be_bids.parquet'))
                    dg_be_bids_df.createOrReplaceTempView("dg_be_bids")

                    grid_df = grid_performance_df.drop('stage')

                    if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                        target_market_days = int(maturity[2]) * 5 + 80
                    if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                        target_market_days = int(maturity)
                    if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                        target_market_days = int(maturity[0])
                    if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                        target_market_days = int(maturity)
                    if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                        target_market_days = int(maturity)

                    # dg_be_bids_df = dg_be_bids_df.loc[dg_be_bids_df["decision_group_rm"] == target_market_days, ['be_bid', 'stage']]
                    dg_be_bids_df = dg_be_bids_df.filter(
                        dg_be_bids_df["decision_group_rm"] == target_market_days).select('be_bid', 'stage')
                    # Get the number of rows and columns
                    row_count = dg_be_bids_df.count()
                    column_count = len(dg_be_bids_df.columns)
                    print('dg_be_bids_df after target market days filtering')
                    print("Number of rows: ", row_count)
                    print("Number of columns: ", column_count)

                    # grid_df = grid_df.merge(dg_be_bids_df, left_on = 'entry_id', right_on = 'be_bid').drop(columns = ['be_bid'])

                    grid_df = grid_df.join(dg_be_bids_df, grid_df["entry_id"] == dg_be_bids_df["be_bid"], "inner").drop(
                        'be_bid')

                    # Get the number of rows and columns
                    # row_count = grid_df.count()
                    # column_count = len(grid_df.columns)
                    # print('grid_df after merge with dg_be_bids_df')
                    # print("Number of rows: ", row_count)
                    # print("Number of columns: ", column_count)

                    ####
                    def run_aggregate_metrics(df, data_sector, groupvar="place_id"):
                        if groupvar == "place_id":
                            # agg_levels = [groupvar, "maturity", "stage", "density", "irrigation", "previous_crop_corn"]

                            # agg_base_df = df[agg_levels + ["entry_id", "cperf", "predict_ygsmn"]] \
                            #    .groupby(agg_levels + ["entry_id", "cperf"], as_index=False) \
                            #    .agg(count_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="count"),
                            #         predict_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="mean"),
                            #         std_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="std"))

                            if data_sector == 'SOY_NA_SUMMER':
                                agg_levels_plus_selected = [groupvar, "maturity", "stage", "density", "irrigation",
                                                            "previous_crop_soy", "entry_id", "cperf",
                                                            "predict_ygsmn", "YGSMNp_10", "YGSMNp_50", "YGSMNp_90",
                                                            "env_input_outlier_ind"]
                                agg_levels_plus_groupby = [groupvar, "maturity", "stage", "density", "irrigation",
                                                           "previous_crop_soy", "entry_id", "cperf"]
                            else:
                                agg_levels_plus_selected = [groupvar, "maturity", "stage", "density", "irrigation",
                                                            "previous_crop_corn", "entry_id", "cperf",
                                                            "predict_ygsmn", "YGSMNp_10", "YGSMNp_50", "YGSMNp_90",
                                                            "env_input_outlier_ind"]
                                agg_levels_plus_groupby = [groupvar, "maturity", "stage", "density", "irrigation",
                                                           "previous_crop_corn", "entry_id", "cperf"]

                            agg_base_df = df.select(agg_levels_plus_selected) \
                                .groupby(agg_levels_plus_groupby).agg(count('*').alias('count_ygsmn'),
                                                                      mean('predict_ygsmn').alias('predict_ygsmn'),
                                                                      stddev('predict_ygsmn').alias('std_ygsmn'),
                                                                      mean('YGSMNp_10').alias('YGSMNp_10'),
                                                                      mean('YGSMNp_50').alias('YGSMNp_50'),
                                                                      mean('YGSMNp_90').alias('YGSMNp_90'),
                                                                      mean(when(col('env_input_outlier_ind') == 'False',
                                                                                col('predict_ygsmn'))).alias(
                                                                          'predict_ygsmn_env'),
                                                                      sum(when(col("env_input_outlier_ind"), 1).otherwise(0)).alias("env_input_outlier_count"))
                        else:
                            print('initial columns:', df.columns)
                            # df = df.rename(columns = {groupvar: "region_name"})
                            df = df.withColumnRenamed(groupvar, "region_name")
                            print('after rename columns:', df.columns)
                            # df["region_type"] = groupvar
                            df = df.withColumn('region_type', lit(groupvar))
                            print('after region type rename columns:', df.columns)

                            # agg_levels = ["maturity", "stage", "region_name", "region_type", "density", "irrigation", "previous_crop_corn"]
                            agg_levels = ["maturity", "stage", "region_name", "region_type", "density", "irrigation",
                                          "previous_crop_corn"]

                            # agg_base_df = df[agg_levels + ["entry_id", "cperf", "predict_ygsmn", "maturity_acres"]] \
                            #    .groupby(agg_levels + ["entry_id", "cperf"], as_index=False) \
                            #    .apply(lambda x: pd.Series({'count_ygsmn': x['predict_ygsmn'].shape[0],
                            #                                'predict_ygsmn': np.average(x['predict_ygsmn'], weights=x['maturity_acres']),
                            #                                'std_ygsmn': np.sqrt(
                            #                                    np.cov(x['predict_ygsmn'], aweights=x['maturity_acres']))}))

                            weighted_avg = expr('sum(predict_ygsmn * maturity_acres) / sum(maturity_acres)')

                            agg_levels_plus_selected = ["maturity", "stage", "region_name", "region_type", "density",
                                                        "irrigation", "previous_crop_corn", "entry_id", "cperf",
                                                        "predict_ygsmn", "maturity_acres"]
                            agg_levels_plus_groupby = ["maturity", "stage", "region_name", "region_type", "density",
                                                       "irrigation", "previous_crop_corn", "entry_id", "cperf"]

                            weighted_mean_df = df.select(agg_levels_plus_selected) \
                                .groupby(agg_levels_plus_groupby).agg(count('*').alias('count_ygsmn'),
                                                                      expr(
                                                                          "sum(predict_ygsmn * maturity_acres) / sum(maturity_acres)").alias(
                                                                          "WeightedMean"))

                            # Join the weighted mean back to the original DataFrame and calculate the weighted covariance
                            grouped_df = df.join(weighted_mean_df, agg_levels_plus_groupby).groupBy(
                                agg_levels_plus_groupby).agg(
                                expr(
                                    "sum((predict_ygsmn - WeightedMean) * (predict_ygsmn - WeightedMean) * maturity_acres) / sum(maturity_acres)").alias(
                                    "std_ygsmn"))

                            agg_base_df = weighted_mean_df.join(grouped_df, agg_levels_plus_groupby)
                            agg_base_df = agg_base_df.withColumnRenamed("WeightedMean", "predict_ygsmn")

                        # row_count = agg_base_df.count()
                        # column_count = len(agg_base_df.columns)
                        # print('agg_base_df size of: ', groupvar)
                        # print("Number of rows: ", row_count)
                        # print("Number of columns: ", column_count)
                        return agg_base_df

                    out_df = run_aggregate_metrics(grid_df, DKU_DST_ap_data_sector, 'place_id')

                    temporal_aggregate_query_dir_path = os.path.join(args.s3_output_temporal_aggregate_query,
                                                                     DKU_DST_ap_data_sector,
                                                                     input_year, pipeline_runid, maturity)

                    temporal_aggregate_query_data_path = os.path.join(temporal_aggregate_query_dir_path,
                                                                      'temporal_aggregate_query.parquet')

                    print('temporal_aggregate_query_data_path: ', temporal_aggregate_query_data_path)
                    isExist = os.path.exists(temporal_aggregate_query_dir_path)
                    if not isExist:
                        # Create a new directory because it does not exist
                        os.makedirs(temporal_aggregate_query_dir_path)

                    # Write recipe outputs
                    out_df.write.mode('overwrite').parquet(temporal_aggregate_query_dir_path)

                except Exception as e:
                    logger.error(e)
                    error_event(DKU_DST_ap_data_sector, input_year, pipeline_runid, str(e))
                    message = f'Placement temporal_aggregate_query error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {input_year}'
                    print('message: ', message)
                    teams_notification(message, None, pipeline_runid)
                    print('teams message sent')
                    raise e


if __name__ == '__main__':
    main()
