import json

from libs.config.config_vars import CONFIG, S3_BUCKET, S3_DATA_PREFIX
from libs.processing.dme_spark_sql_recipe import DmeSparkSqlRecipe

SQL_TEMPLATE = """SELECT 
    `ap_data_sector`,
    `analysis_year`,
    `source_id`,
    `count`,
    `prediction`,
    `stderr`,
    `entry_id`,
    `cmatf`,
    `decision_group_rm`,
    `technology`,
    `stage`,
    `material_type`,
    `pvs_prediction`,
    `chk_prediction_avg`,
    `chk_prediction_stddev`,
    `feature_score`,
    `feature_min`,
    `feature_max`,
    `feature_score2`,
    `rmtn`
  FROM `pvs_for_erm_prepared`
WHERE `prediction` IS NOT NULL"""


if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        data_path = f's3a://{S3_BUCKET}/{S3_DATA_PREFIX}/{pipeline_runid}/{analysis_type}/'
        input_views = {
            'pvs_for_erm_prepared': f'{data_path}compute_pvs_for_erm_prepared/*parquet'
        }
        dr = DmeSparkSqlRecipe(f'{data_path}compute_erm_test', input_views)
        dr.process(SQL_TEMPLATE)
