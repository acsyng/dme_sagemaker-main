import argparse
import json

import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType
from pyspark.sql.window import Window

from libs.config.config_vars import CONFIG
from libs.event_bridge.event import error_event

from libs.cmt_lib import cmt_spark_sql_recipes
from libs.cmt_lib import cmt_sql_recipes

def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_material_info_sp', type=str,
                        help='s3 input location', required=True)
    parser.add_argument('--s3_output_material_prepared_joined', type=str,
                        help='s3 output location', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        target_pipeline_runid = data['target_pipeline_runid']

        try:
            spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

            # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
            spark.sparkContext._jsc.hadoopConfiguration().set('mapred.output.committer.class',
                                                              'org.apache.hadoop.mapred.FileOutputCommitter')
            spark.sparkContext._jsc.hadoopConfiguration().setBoolean('fs.s3a.sse.enabled', True)
            spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
            spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.sse.kms.keyId', CONFIG['output_kms_key'])

            ### Read recipe inputs
            # df = cmt_sql_recipes.get_material_info_sp() # default is corn
            # material_info_sp_df = spark.createDataFrame(df)
            print("retrieving data from "+args.s3_input_material_info_sp)
            material_info_sp_df = spark.read.format("parquet").load(args.s3_input_material_info_sp, 
                                                                    header='true', 
                                                                    inferSchema='true')
            material_info_sp_df.createOrReplaceTempView("material_info_sp")

            # get materials prepared by different identifiers
            material_prepared_by_pre_line_code_df = cmt_spark_sql_recipes.compute_material_prepared_by_pre_line_code(
                spark)
            material_prepared_by_pre_line_code_df.createOrReplaceTempView("material_prepared_by_pre_line_code")

            # need pre_line_code before doing line_code
            material_prepared_by_line_code_df = cmt_spark_sql_recipes.compute_material_prepared_by_line_code(spark)
            material_prepared_by_line_code_df.createOrReplaceTempView("material_prepared_by_line_code")

            # get material by bebid
            material_prepared_by_bebid_df = cmt_spark_sql_recipes.compute_material_prepared_by_bebid(spark)
            material_prepared_by_bebid_df.createOrReplaceTempView("material_prepared_by_bebid")

            # join all together, output join
            material_prepared_joined_df = cmt_spark_sql_recipes.compute_material_prepared_joined(spark)

            # Write recipe outputs
            material_prepared_joined_df.write.mode(
                'overwrite'
            ).parquet(args.s3_output_material_prepared_joined)

        except Exception as e:
            error_event('cmt', 'cmt', target_pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
