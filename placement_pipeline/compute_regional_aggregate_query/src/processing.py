import argparse
import json
import os

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, lit, sum, when, col

from libs.config.config_vars import CONFIG, S3_BUCKET
from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.placement_spark_sql_recipes import regional_aggregate_query_sql_query_NA_CORN, \
    regional_aggregate_query_sql_query_SOY_BRAZIL_SUMMER, regional_aggregate_query_sql_query_CORN_BRAZIL, \
    regional_aggregate_query_sql_query_NA_SOY, regional_aggregate_query_sql_query_CORNGRAIN_EAME


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_grid_performance', type=str,
                        help='s3 input grid performance', required=True)
    parser.add_argument('--s3_input_grid_classification', type=str, help='s3 input grid classification', required=True)
    # parser.add_argument('--s3_input_grid_market_maturity', type=str, help='s3 input grid market maturity', required=True)
    parser.add_argument('--s3_output_regional_aggregate_query', type=str,
                        help='s3 output regional aggregate query', required=True)

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
                                         'compute_regional_aggregate_query/data/regional_aggregate_query',
                                         DKU_DST_ap_data_sector,
                                         input_year, maturity, 'regional_aggregate_query.parquet')

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

                    # print('args.s3_input_grid_market_maturity: ', args.s3_input_grid_market_maturity)
                    grid_market_maturity_df = spark.read.parquet(
                        os.path.join(args.s3_input_grid_classification, DKU_DST_ap_data_sector, input_year,
                                     'grid_market_maturity.parquet'))
                    grid_market_maturity_df.createOrReplaceTempView("grid_market_maturity")

                    grid_classification_df = spark.read.parquet(
                        os.path.join(args.s3_input_grid_classification, DKU_DST_ap_data_sector, input_year,
                                     'grid_classification.parquet'))
                    grid_classification_df.createOrReplaceTempView("grid_classification")

                    if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                        grid_df = regional_aggregate_query_sql_query_NA_CORN(spark, maturity, input_year)
                    if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                        grid_df = regional_aggregate_query_sql_query_CORN_BRAZIL(spark, maturity, input_year,
                                                                                 DKU_DST_ap_data_sector)
                    if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                        grid_df = regional_aggregate_query_sql_query_SOY_BRAZIL_SUMMER(spark, maturity, input_year)
                    if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                        grid_df = regional_aggregate_query_sql_query_NA_SOY(spark, maturity, input_year)
                    if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                        grid_df = regional_aggregate_query_sql_query_CORNGRAIN_EAME(spark, maturity, input_year,
                                                                                    DKU_DST_ap_data_sector)

                    # grid_df = regional_aggregate_query_sql_query(spark, maturity, input_year)
                    # Get the number of rows and columns
                    row_count = grid_df.count()
                    column_count = len(grid_df.columns)
                    print('grid_df after regional_aggregate_query_sql_query')
                    print("Number of rows: ", row_count)
                    print("Number of columns: ", column_count)

                    dg_be_bids_df = spark.read.parquet(
                        os.path.join(args.s3_input_grid_classification, DKU_DST_ap_data_sector, input_year,
                                     'dg_be_bids.parquet'))
                    dg_be_bids_df.createOrReplaceTempView("dg_be_bids")

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
                    row_count = grid_df.count()
                    column_count = len(grid_df.columns)
                    print('grid_df after merge with dg_be_bids_df')
                    print("Number of rows: ", row_count)
                    print("Number of columns: ", column_count)

                    ####
                    def run_aggregate_metrics(df, data_sector, groupvar="place_id"):
                        print('inital columns:', df.columns)
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
                        if data_sector == 'SOY_NA_SUMMER':
                            agg_levels_plus_selected = ["maturity", "stage", "region_name", "region_type", "density",
                                                        "irrigation", "previous_crop_soy", "entry_id", "cperf",
                                                        "predict_ygsmn", "maturity_acres", "YGSMNp_10", "YGSMNp_50",
                                                        "YGSMNp_90", "env_input_outlier_ind"]
                            agg_levels_plus_groupby = ["maturity", "stage", "region_name", "region_type", "density",
                                                       "irrigation", "previous_crop_soy", "entry_id", "cperf"]
                        else:
                            agg_levels_plus_selected = ["maturity", "stage", "region_name", "region_type", "density",
                                                        "irrigation", "previous_crop_corn", "entry_id", "cperf",
                                                        "predict_ygsmn", "maturity_acres", "YGSMNp_10", "YGSMNp_50",
                                                        "YGSMNp_90", "env_input_outlier_ind"]
                            agg_levels_plus_groupby = ["maturity", "stage", "region_name", "region_type", "density",
                                                       "irrigation", "previous_crop_corn", "entry_id", "cperf"]

                        env_df = df.select(agg_levels_plus_selected).groupby(agg_levels_plus_groupby).agg(
                            sum(when(col('env_input_outlier_ind') == 'False',
                                     col('predict_ygsmn') * col('maturity_acres'))).alias('weighted_sum'),
                            sum(when(col('env_input_outlier_ind') == 'False', col('maturity_acres'))).alias(
                                'total_weight')
                        )

                        env_df = env_df.withColumn('weighted_average', env_df['weighted_sum'] / env_df['total_weight'])

                        weighted_mean_df = df.select(agg_levels_plus_selected) \
                            .groupby(agg_levels_plus_groupby).agg(count('*').alias('count_ygsmn'),
                                                                  expr(
                                                                      "sum(predict_ygsmn * maturity_acres) / sum(maturity_acres)").alias(
                                                                      "WeightedMean"),
                                                                  expr(
                                                                      "sum(YGSMNp_10 * maturity_acres) / sum(maturity_acres)").alias(
                                                                      "YGSMNp_10"),
                                                                  expr(
                                                                      "sum(YGSMNp_50 * maturity_acres) / sum(maturity_acres)").alias(
                                                                      "YGSMNp_50"),
                                                                  expr(
                                                                      "sum(YGSMNp_90 * maturity_acres) / sum(maturity_acres)").alias(
                                                                      "YGSMNp_90")
                                                                  )

                        # Join the weighted mean back to the original DataFrame and calculate the weighted covariance
                        grouped_df = df.join(weighted_mean_df, agg_levels_plus_groupby).groupBy(
                            agg_levels_plus_groupby).agg(
                            expr(
                                "sum((predict_ygsmn - WeightedMean) * (predict_ygsmn - WeightedMean) * maturity_acres) / sum(maturity_acres)").alias(
                                "std_ygsmn"))

                        agg_base_df = weighted_mean_df.join(grouped_df, agg_levels_plus_groupby)
                        agg_base_df = agg_base_df.withColumnRenamed("WeightedMean", "predict_ygsmn")

                        agg_base_df = agg_base_df.join(env_df, agg_levels_plus_groupby)
                        agg_base_df = agg_base_df.withColumnRenamed("weighted_average", "predict_ygsmn_env")

                        # row_count = agg_base_df.count()
                        # column_count = len(agg_base_df.columns)
                        # print('agg_base_df size of: ', groupvar)
                        # print("Number of rows: ", row_count)
                        # print("Number of columns: ", column_count)
                        return agg_base_df

                    out_list = []
                    if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                        aggregate_levels = ['wce', 'tpp', 'market_segment']
                    if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                        aggregate_levels = ['tpp', 'meso', 'maturity_group']
                    if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                        aggregate_levels = ['tpp', 'rec', 'maturity_group']
                    if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                        aggregate_levels = ['ecw', 'acs']
                    if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                        aggregate_levels = ['country', 'mst']

                    for colname in aggregate_levels:
                        df = run_aggregate_metrics(grid_df, DKU_DST_ap_data_sector, groupvar=colname)
                        df = df.withColumn('ap_data_sector', lit(DKU_DST_ap_data_sector))
                        df = df.withColumn('analysis_year', lit(int(input_year)))

                        df = df.drop('id')
                        out_list.append(df)

                    combined_df = None
                    for df in out_list:
                        if combined_df is None:
                            combined_df = df
                        else:
                            combined_df = combined_df.unionAll(df)

                    regional_aggregate_query_dir_path = os.path.join(args.s3_output_regional_aggregate_query,
                                                                     DKU_DST_ap_data_sector,
                                                                     input_year, pipeline_runid, maturity)

                    regional_aggregate_query_data_path = os.path.join(regional_aggregate_query_dir_path,
                                                                      'regional_aggregate_query.parquet')

                    print('regional_aggregate_query_data_path: ', regional_aggregate_query_data_path)
                    isExist = os.path.exists(regional_aggregate_query_dir_path)
                    if not isExist:
                        # Create a new directory because it does not exist
                        os.makedirs(regional_aggregate_query_dir_path)

                    # Write recipe outputs
                    combined_df.write.mode('overwrite').parquet(regional_aggregate_query_dir_path)

                except Exception as e:
                    logger.error(e)
                    error_event(DKU_DST_ap_data_sector, input_year, pipeline_runid, str(e))
                    message = f'Placement regional_aggregate_query error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {input_year}'
                    print('message: ', message)
                    teams_notification(message, None, pipeline_runid)
                    print('teams message sent')
                    raise e


if __name__ == '__main__':
    main()
