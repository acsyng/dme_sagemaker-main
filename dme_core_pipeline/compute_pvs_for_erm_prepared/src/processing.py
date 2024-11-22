import json

from libs.config.config_vars import CONFIG, S3_BUCKET, S3_DATA_PREFIX
from libs.processing.dme_spark_sql_recipe import DmeSparkSqlRecipe

SQL_TEMPLATE = """SELECT
    `query_output`.`ap_data_sector` AS `ap_data_sector`,
    `query_output`.`analysis_year` AS `analysis_year`,
    `query_output`.`source_id` AS `source_id`,
    `query_output`.`count` AS `count`,
    `query_output`.`prediction` AS `prediction`,
    `query_output`.`stderr` AS `stderr`,
    `query_output`.`entry_id` AS `entry_id`,
    `query_output`.`cmatf` AS `cmatf`,
    `query_output`.`decision_group_rm` AS `decision_group_rm`,
    `query_output`.`technology` AS `technology`,
    `query_output`.`stage` AS `stage`,
    `query_output`.`material_type` AS `material_type`,
    COALESCE(`query_output`.`pvs_prediction`, 0) AS `pvs_prediction`,
    `check_stats`.`prediction_avg` AS `chk_prediction_avg`,
    `check_stats`.`prediction_stddev` AS `chk_prediction_stddev`,
    (`query_output`.`prediction` - `check_stats`.`prediction_avg`) AS `feature_score`,
    MIN(`query_output`.`prediction` - `check_stats`.`prediction_avg`) OVER (PARTITION BY `query_output`.`ap_data_sector`, `query_output`.`analysis_year`, `query_output`.`source_id`) AS `feature_min`,
    MAX(`query_output`.`prediction` - `check_stats`.`prediction_avg`) OVER (PARTITION BY `query_output`.`ap_data_sector`, `query_output`.`analysis_year`, `query_output`.`source_id`) AS `feature_max`,
    CASE 
        WHEN `query_output`.`ap_data_sector` LIKE 'SOY%' 
            THEN (`query_output`.`prediction`-115)/5 - IF(`query_output`.`decision_group_rm` < 0, `query_output`.`decision_group_rm`/2, `query_output`.`decision_group_rm`) 
        ELSE pow(`query_output`.`prediction`,2)
    END AS `feature_score2`,
    `query_output`.`RM` AS `rmtn`
FROM `pvs_for_erm` `query_output`
INNER JOIN (
    SELECT 
        `ap_data_sector` AS `ap_data_sector`,
        `analysis_year` AS `analysis_year`,
        `source_id` AS `source_id`,
        MIN(`prediction`) AS `prediction_min`,
        MAX(`prediction`) AS `prediction_max`,
        AVG(`prediction`) AS `prediction_avg`,
        STDDEV(`prediction`) AS `prediction_stddev`,
        `decision_group_rm` AS `decision_group_rm`,
        COUNT(*) AS `count`
      FROM `pvs_for_erm` `query_output`
      WHERE `cmatf` = 1
      GROUP BY 
        `ap_data_sector`,
        `analysis_year`,
        `source_id`,
        `decision_group_rm`
) `check_stats`
ON `query_output`.`ap_data_sector` = `check_stats`.`ap_data_sector`
    AND `query_output`.`analysis_year` = `check_stats`.`analysis_year`
    AND `query_output`.`source_id` = `check_stats`.`source_id`
    AND `query_output`.`decision_group_rm` = `check_stats`.`decision_group_rm`"""

if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        data_path = f's3a://{S3_BUCKET}/{S3_DATA_PREFIX}/{pipeline_runid}/{analysis_type}/'
        input_views = {
            'pvs_for_erm': f'{data_path}compute_pvs_for_erm2/*parquet'
        }
        dr = DmeSparkSqlRecipe(f'{data_path}compute_pvs_for_erm_prepared', input_views)
        dr.process(SQL_TEMPLATE)
