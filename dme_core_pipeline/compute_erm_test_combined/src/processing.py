import json

from libs.config.config_vars import CONFIG, S3_BUCKET, S3_DATA_PREFIX
from libs.processing.dme_spark_sql_recipe import DmeSparkSqlRecipe

SQL_TEMPLATE = """SELECT
    `query_output`.`ap_data_sector` AS `ap_data_sector`,
    `query_output`.`analysis_year` AS `analysis_year`,
    NULL AS `decision_group_name`,
    `query_output`.`entry_id` AS `entry_id`,
    `query_output`.`technology` AS `technology`,
    `query_output`.`material_type` AS `material_type`,
    `query_output`.`rm_avg` AS `original_rm`,
    `query_output`.`cmatf`,
    `query_output`.`decision_group_rm`,
    CASE
        WHEN `query_output`.`ap_data_sector` LIKE 'SOY%'
            THEN ROUND((`query_output`.`prediction` - 80)/5,2)
        WHEN `query_output`.`ap_data_sector` IN ('CORN_BRAZIL_SUMMER', 'CORN_BRAZIL_SAFRINHA')
            THEN ROUND(`query_output`.`prediction`, 1)
        ELSE CAST(ROUND(`query_output`.`prediction`, 0) AS integer)
    END AS `rm_estimate`,
    `query_output`.`count`,
    'regression' AS `rm_model`
FROM (
    SELECT 
        `ap_data_sector` AS `ap_data_sector`,
        `analysis_year` AS `analysis_year`,
        `entry_id` AS `entry_id`,
        `material_type` AS `material_type`,
        `technology` AS `technology`,
        AVG(CAST( (`rmtn`) AS FLOAT)) AS `rm_avg`,
        AVG(`cmatf`) AS `cmatf`,
        ROUND(AVG(`decision_group_rm`),0) AS `decision_group_rm`,
--        ROUND(AVG(CAST( (`ml_rm_estimate`) AS FLOAT)),0) AS `ml_rm_estimate`,
        AVG(`prediction`) AS `prediction`,
        COUNT(*) AS `count`
        FROM `erm_test_scored`
--      WHERE `chk_prediction_stddev` IS NOT NULL
--        AND `prediction` IS NOT NULL
      GROUP BY `ap_data_sector`, `analysis_year`, `entry_id`, `material_type`, `technology`
) `query_output`

UNION ALL
SELECT
    `query_output`.`ap_data_sector` AS `ap_data_sector`,
    `query_output`.`analysis_year` AS `analysis_year`,
    `query_output`.`source_id` AS `decision_group_name`,
    `query_output`.`entry_id` AS `entry_id`,
    `query_output`.`technology` AS `technology`,
    `query_output`.`material_type` AS `material_type`,
    `query_output`.`rm_avg` AS `original_rm`,
    `query_output`.`cmatf`,
    `query_output`.`decision_group_rm`,
    CASE
        WHEN `query_output`.`ap_data_sector` LIKE 'SOY%'
            THEN ROUND((`query_output`.`prediction` - 80)/5,2)
        WHEN `query_output`.`ap_data_sector` IN ('CORN_BRAZIL_SUMMER', 'CORN_BRAZIL_SAFRINHA')
            THEN ROUND(`query_output`.`prediction`, 1)
        ELSE CAST(ROUND(`query_output`.`prediction`, 0) AS integer)
    END AS `rm_estimate`,
    `query_output`.`count`,
    'regression-by-dg' AS `rm_model`
FROM (

    SELECT 
        `ap_data_sector` AS `ap_data_sector`,
        `analysis_year` AS `analysis_year`,
        `source_id` AS `source_id`,
        `entry_id` AS `entry_id`,
        `material_type` AS `material_type`,
        `technology` AS `technology`,
        AVG(CAST( (`rmtn`) AS FLOAT)) AS `rm_avg`,
        AVG(`cmatf`) AS `cmatf`,
        ROUND(AVG(`decision_group_rm`),0) AS `decision_group_rm`,
--        ROUND(AVG(CAST( (`ml_rm_estimate`) AS FLOAT)),0) AS `ml_rm_estimate`,
        AVG(`prediction`) AS `prediction`,
        COUNT(*) AS `count`
        FROM `erm_test_scored`
--      WHERE `chk_prediction_stddev` IS NOT NULL
--        AND `prediction` IS NOT NULL
      GROUP BY `ap_data_sector`, `analysis_year`, `source_id`, `entry_id`, `material_type`, `technology`
) `query_output`"""


if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        data_path = f's3a://{S3_BUCKET}/{S3_DATA_PREFIX}/{pipeline_runid}/{analysis_type}/'
        input_views = {
            'erm_test_scored': f'{data_path}erm_test_scored.csv'
        }
        dr = DmeSparkSqlRecipe(f'{data_path}compute_erm_test_combined', input_views)
        dr.process(SQL_TEMPLATE)
