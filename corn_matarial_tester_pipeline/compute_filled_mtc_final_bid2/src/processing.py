import argparse
import json

import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType
from pyspark.sql.window import Window

from libs.config.config_vars import CONFIG
from libs.event_bridge.event import error_event
from libs.postgres.postgres_connection import PostgresConnection

from libs.cmt_lib import cmt_spark_sql_recipes

INSERT_SQL_STR = '''
INSERT INTO dme.corn_material_tester
(be_bid,
lbg_bid,
abbr_code,
family,
gencd,
pedigree,
highname,
gmo,
gna_pedigree,
primary_genetic_family_lid,
secondary_genetic_family_lid,
fada_group,
cgenes,
fp_abbr_code,
fp_gencd,
fp_precd,
fp_highname,
fp_gna_pedigree,
fp_linecode,
fp_be_bid,
fp_lbg_bid,
fp_fp_be_bid,
fp_mp_be_bid,
fp_het_group,
fp_fada_group,
fp_sample,
mp_abbr_code,
mp_gencd,
mp_precd,
mp_highname,
mp_gna_pedigree,
mp_linecode,
mp_be_bid,
mp_lbg_bid,
mp_fp_be_bid,
mp_mp_be_bid,
mp_het_group,
mp_fada_group,
mp_sample,
female,
male,
insect_cgenes,
calc_tester,
tester_role,
fp_het_pool,
mp_het_pool,
pool1_be_bid,
pool2_be_bid
)
SELECT
be_bid,
lbg_bid,
SUBSTRING(abbr_code, 1, 1023) AS abbr_code,
SUBSTRING(family, 1, 1023) AS family,
gencd,
SUBSTRING(pedigree, 1, 1023) AS pedigree,
highname,
gmo,
SUBSTRING(gna_pedigree, 1, 1023) AS gna_pedigree,
primary_genetic_family_lid,
secondary_genetic_family_lid,
fada_group,
cgenes,
SUBSTRING(fp_abbr_code, 1, 1023) AS fp_abbr_code,
fp_gencd,
fp_precd,
fp_highname,
SUBSTRING(fp_gna_pedigree, 1, 1023) AS fp_gna_pedigree,
fp_linecode,
fp_be_bid,
fp_lbg_bid,
fp_fp_be_bid,
fp_mp_be_bid,
fp_het_group,
fp_fada_group,
fp_sample,
SUBSTRING(mp_abbr_code, 1, 1023) AS mp_abbr_code,
mp_gencd,
mp_precd,
mp_highname,
SUBSTRING(mp_gna_pedigree, 1, 1023) AS mp_gna_pedigree,
mp_linecode,
mp_be_bid,
mp_lbg_bid,
mp_fp_be_bid,
mp_mp_be_bid,
mp_het_group,
mp_fada_group,
mp_sample,
SUBSTRING(female, 1, 1023) AS female,
SUBSTRING(male, 1, 1023) AS male,
insect_cgenes,
calc_tester,
tester_role,
fp_het_pool,
mp_het_pool,
pool1_be_bid,
pool2_be_bid
FROM public.{0}
'''

def drop_and_insert(insert_sql, table_name, df):
    pc = PostgresConnection()
    temp_table_name = pc.get_temp_table(table_name)
    pc.write_spark_df(df, temp_table_name)
    pc.execute_sql_batch([
        "DELETE FROM dme.corn_material_tester",
        insert_sql.format(temp_table_name),
        f'DROP TABLE {temp_table_name}'
    ])


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_filled_material_tester_calculated_rough', type=str,
                        help='s3 input filled material tester bucket', required=True)
    parser.add_argument('--s3_output_filled_mtc_final_bid2', type=str,
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

            # Read recipe inputs
            filled_material_tester_calculated_rough_pyspark_df = spark.read.parquet(
                args.s3_input_filled_material_tester_calculated_rough
            )
            filled_material_tester_calculated_rough_pyspark_df.createOrReplaceTempView(
                "filled_material_tester_calculated_rough_by_be_bid"
            )

            # perform a series of operations.
            # filled_material_tester_calculated_rough_by_be_bid_df = \
            #     cmt_spark_sql_recipes.compute_filled_material_tester_calculated_rough_by_be_bid(spark)
            # filled_material_tester_calculated_rough_by_be_bid_df.createOrReplaceTempView(
            #     "filled_material_tester_calculated_rough_by_be_bid"
            # )

            filled_material_tester_calculated_rough_by_be_bid_with_parents_df = \
                cmt_spark_sql_recipes.compute_filled_material_tester_calculated_rough_by_be_bid_with_parents(spark)
            filled_material_tester_calculated_rough_by_be_bid_with_parents_df.createOrReplaceTempView(
                "filled_material_tester_calculated_rough_by_be_bid_with_parents"
            )

            bebid_tester_stats_df = cmt_spark_sql_recipes.compute_bebid_tester_stats(spark)
            bebid_tester_stats_df.createOrReplaceTempView("bebid_tester_stats")

            filled_mtc_final_bid_df = cmt_spark_sql_recipes.compute_filled_mtc_final_bid(spark)
            filled_mtc_final_bid_df.createOrReplaceTempView("filled_mtc_final_bid")

            hetpool_review_df = cmt_spark_sql_recipes.compute_hetpool_review(spark)
            hetpool_review_df.createOrReplaceTempView("hetpool_review")

            filled_mtc_final_bid2_df = cmt_spark_sql_recipes.compute_filled_mtc_final_bid2(spark)

            # Write recipe outputs
            filled_mtc_final_bid2_df.write.mode(
                'overwrite'
            ).parquet(args.s3_output_filled_mtc_final_bid2)
            
            drop_and_insert(INSERT_SQL_STR, 'corn_material_tester', filled_mtc_final_bid2_df)

        except Exception as e:
            error_event('cmt', 'cmt', target_pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
