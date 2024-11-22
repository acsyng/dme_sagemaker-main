import argparse
import json
import os

from pyspark.sql import SparkSession

from libs.config.config_vars import CONFIG
from libs.event_bridge.event import error_event
from libs.placement_spark_sql_recipes import results_joined_query
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_tops_data_2', type=str,
                        help='s3 input tops data 2', required=True)
    parser.add_argument('--s3_output_tops_data_query_3', type=str,
                        help='s3 output tops data query 3', required=True)
    args = parser.parse_args()
    print('args collected ')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

        # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
        spark.sparkContext._jsc.hadoopConfiguration().set('mapred.output.committer.class',
                                                          'org.apache.hadoop.mapred.FileOutputCommitter')
        spark.sparkContext._jsc.hadoopConfiguration().setBoolean('fs.s3a.sse.enabled', True)
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.sse.kms.keyId', CONFIG['output_kms_key'])

        years = ['2023']  # , '2023']
        for input_year in years:

            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_tops_data_query_3/data/tops_data_query_3',
                                     DKU_DST_ap_data_sector,
                                     input_year, 'tops_data_query_3.parquet')

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
                print('args.s3_input_tops_data_2: ', args.s3_input_tops_data_2)
                results_joined_df = spark.read.parquet(os.path.join(args.s3_input_tops_data_2, DKU_DST_ap_data_sector, input_year, 'tops_data_query_2.parquet'))
                results_joined_df.createOrReplaceTempView("results_joined")

                results_final_df = results_joined_query(spark)

                tops_data_query_3_dir_path = os.path.join(args.s3_output_tops_data_query_3, DKU_DST_ap_data_sector, input_year)

                tops_data_query_3_data_path = os.path.join(tops_data_query_3_dir_path, 'tops_data_query_3.parquet')
                print('tops_data_query_3_data_path: ', tops_data_query_3_data_path)
                isExist = os.path.exists(tops_data_query_3_dir_path)
                if not isExist:
                    # Create a new directory because it does not exist
                    os.makedirs(tops_data_query_3_dir_path)

                # Write recipe outputs
                results_final_df.write.mode('overwrite').parquet(tops_data_query_3_dir_path)

            except Exception as e:
                error_event(DKU_DST_ap_data_sector, input_year, pipeline_runid, str(e))
                raise e


if __name__ == '__main__':
    main()
