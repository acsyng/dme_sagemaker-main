import argparse
import json
import os

from pyspark.sql import SparkSession

from libs.config.config_vars import CONFIG
from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.placement_s3_functions import get_s3_prefix
from libs.placement_spark_sql_recipes import get_comparison_set_soy_spark


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    #parser.add_argument('--s3_input_grid_performance', type=str,
    #                    help='s3 input grid performance', required=True)
    #parser.add_argument('--s3_input_grid_classification', type=str, help='s3 input grid classification', required=True)
    # parser.add_argument('--s3_input_grid_market_maturity', type=str, help='s3 input grid market maturity', required=True)
    #parser.add_argument('--s3_output_regional_aggregate_query', type=str,
    #                    help='s3 output regional aggregate query', required=True)
    parser.add_argument('--s3_output_soy_comparison_set', type=str,
                        help='s3 output soy comparison set', required=True)
    args = parser.parse_args()
    print('args collected ')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        pipeline_runid = data['target_pipeline_runid']
        spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

        # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
        spark.sparkContext._jsc.hadoopConfiguration().set('mapred.output.committer.class',
                                                          'org.apache.hadoop.mapred.FileOutputCommitter')
        spark.sparkContext._jsc.hadoopConfiguration().setBoolean('fs.s3a.sse.enabled', True)
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.sse.kms.keyId', CONFIG['output_kms_key'])

        years = ['2022']  # , '2023']

        file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                 'compute_soy_comparison_set/data/soy_comparison_set',
                                 DKU_DST_ap_data_sector,
                                 'soy_comparison_set.parquet')

        # check_file_exists = check_if_file_exists_s3(file_path)
        # if check_file_exists is False:
        print('Creating file in the following location: ', file_path)
        # regional_aggregate_function(DKU_DST_ap_data_sector, input_year, maturity, pipeline_runid, args=args)
        # print('File created')
        #    print()
        # else:
        #    print('File exists here: ', file_path)
        try:
            soy_comparison_set = get_comparison_set_soy_spark(spark)

            soy_comparison_set_dir_path = os.path.join(args.s3_output_soy_comparison_set,
                                                       DKU_DST_ap_data_sector)

            soy_comparison_set_data_path = os.path.join(soy_comparison_set_dir_path,
                                                        'soy_comparison_set.parquet')

            print('soy_comparison_set_data_path: ', soy_comparison_set_data_path)
            isExist = os.path.exists(soy_comparison_set_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(soy_comparison_set_dir_path)

            # Write recipe outputs
            soy_comparison_set.write.mode('overwrite').parquet(soy_comparison_set_dir_path)

        except Exception as e:
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
