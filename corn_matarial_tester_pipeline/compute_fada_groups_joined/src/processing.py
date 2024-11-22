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
    parser.add_argument('--s3_input_material_prepared_joined', type=str,
                        help='s3 input material info sp bucket', required=True)
    parser.add_argument('--s3_input_fada_ss_nss', type=str,
                        help='s3 input fada ss nss bucket', required=True)
    parser.add_argument('--s3_input_genetic_group_classification', type=str,
                        help='s3 genetic group classification bucket', required=True)
    parser.add_argument('--s3_output_fada_groups_joined', type=str,
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

            fada_ss_nss_df = spark.read.option('header', True).csv(args.s3_input_fada_ss_nss)
            fada_ss_nss_df.createOrReplaceTempView("fada_ss_nss")

            # load genetic group classifications fada
            rv_v_p_genetic_group_classif_fada_df = spark.read.option('header', True).csv(args.s3_input_genetic_group_classification)
            rv_v_p_genetic_group_classif_fada_df.createOrReplaceTempView("rv_v_p_genetic_group_classif_fada")

            # generate fada groups
            fada_groups_df = cmt_spark_sql_recipes.compute_fada_groups(spark)
            fada_groups_df.createOrReplaceTempView("fada_groups")

            print("sql call 2")

            # separate groups by id
            fada_groups_by_id_df = cmt_spark_sql_recipes.compute_fada_groups_by_id(spark)
            fada_groups_by_id_df.createOrReplaceTempView("fada_groups_by_id")

            print("sql call 3")

            # join groups
            fada_groups_joined_df = cmt_spark_sql_recipes.compute_fada_groups_joined(spark)

            print("sql call 4")

            # Write recipe outputs
            fada_groups_joined_df.write.mode(
                'overwrite'
            ).parquet(args.s3_output_fada_groups_joined)

        except Exception as e:
            error_event('cmt', 'cmt', target_pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
