import argparse
import json

import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType
from pyspark.sql.window import Window

from libs.config.config_vars import CONFIG
from libs.event_bridge.event import error_event
from libs.helper.parameters_helper import ParametersHelper

from libs.cmt_lib import cmt_spark_sql_recipes
from sqlalchemy import create_engine
from libs.postgres.postgres_connection import PostgresConnection


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_fada_groups_joined', type=str,
                        help='s3 input fada groups bucket', required=True)
    parser.add_argument('--s3_input_material_prepared_joined', type=str,
                        help='s3 input material prepared bucket', required=True)
    parser.add_argument('--s3_input_geno_sample_info', type=str,
                        help='s3 input geno sample info bucket', required=True)
    parser.add_argument('--s3_output_filled_material_tester_calculated_rough', type=str,
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

            # Read fada inputs
            fada_groups_joined_pyspark_df = spark.read.parquet(args.s3_input_fada_groups_joined)
            fada_groups_joined_pyspark_df.createOrReplaceTempView("fada_groups_joined")

            # split fada by identifiers
            fada_by_bebid_df = cmt_spark_sql_recipes.compute_fada(spark, level = "be_bid")
            fada_by_chassis_df = cmt_spark_sql_recipes.compute_fada(spark, level = "chassis")
            fada_by_family_df = cmt_spark_sql_recipes.compute_fada_by_family(spark)
            fada_by_lbg_bid_df = cmt_spark_sql_recipes.compute_fada(spark, level = "lbg_bid")
            fada_by_lincd_df = cmt_spark_sql_recipes.compute_fada(spark, level = "line_code")
            fada_by_precd_df = cmt_spark_sql_recipes.compute_fada(spark, level = "pre_line_code")

            fada_by_bebid_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_bebid")
            fada_by_chassis_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_chassis")
            fada_by_family_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_family")
            fada_by_lbg_bid_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_lbg_bid")
            fada_by_lincd_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_lincd")
            fada_by_precd_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("fada_by_precd")

            # read geno inputs
            geno_sample_info_pyspark_df = spark.read.parquet(args.s3_input_geno_sample_info)
            geno_sample_info_pyspark_df.createOrReplaceTempView("geno_sample_info")

            # split geno by identifiers
            sample_precode_df = cmt_spark_sql_recipes.compute_sample(spark, level = "pre_line_code")
            sample_bebid_df = cmt_spark_sql_recipes.compute_sample(spark, level = "be_bid")
            sample_chassis_df = cmt_spark_sql_recipes.compute_sample(spark, level = "chassis")
            sample_linecode_df = cmt_spark_sql_recipes.compute_sample(spark, level = "line_code")

            sample_precode_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("sample_precode")
            sample_bebid_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("sample_bebid")
            sample_chassis_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("sample_chassis")
            sample_linecode_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("sample_linecode")

            
            spark.catalog.dropTempView("fada_groups_joined")
            spark.catalog.dropTempView("geno_sample_info")

            # load in material_prepared_joined data
            material_prepared_joined_pyspark_df = spark.read.parquet(args.s3_input_material_prepared_joined)
            material_prepared_joined_pyspark_df.createOrReplaceTempView("material_prepared_joined")

            bebid_cpi_df = cmt_spark_sql_recipes.compute_bebid_cpi(spark)
            bebid_cpi_df.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("be_bid_cpi")

            # merge all splits with material_prepared_joined
            filled_material_tester_df = cmt_spark_sql_recipes.compute_fmtc_rough(spark)

            # Write recipe outputs
            filled_material_tester_df.write.mode(
                'overwrite'
            ).parquet(args.s3_output_filled_material_tester_calculated_rough)

            # write final dataframe to postgres table:
            # """
            # # get database parameters
            # pc = PostgresConnection()
            # pc.persist(
            #     df='filled_material_tester_df', 
            #     table_name='corn_material_tester', 
            #     schema_name='dme'
            # )
            # """

        except Exception as e:
            error_event('cmt', 'cmt', target_pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
