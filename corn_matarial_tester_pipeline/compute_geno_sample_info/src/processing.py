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

def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_geno_sample_list', type=str,
                        help='s3 input geno sample list bucket', required=True)
    parser.add_argument('--s3_input_material_prepared_joined', type=str,
                        help='s3 input material info sp bucket', required=True)
    parser.add_argument('--s3_output_geno_sample_info', type=str,
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
            material_prepared_joined_df = spark.read.parquet(args.s3_input_material_prepared_joined)
            material_prepared_joined_df.createOrReplaceTempView("material_prepared_joined")

            geno_sample_list_df = spark.read.option('header', True).csv(args.s3_input_geno_sample_list)
            geno_sample_list_df.createOrReplaceTempView("geno_sample_list")

            geno_sample_info_df = cmt_spark_sql_recipes.compute_geno_sample_info(spark)

            # Write recipe outputs
            geno_sample_info_df.write.mode(
                'overwrite'
            ).parquet(args.s3_output_geno_sample_info)

        except Exception as e:
            error_event('cmt', 'cmt', target_pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
