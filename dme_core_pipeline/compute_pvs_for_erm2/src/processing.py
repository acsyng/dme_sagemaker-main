import json

from sagemaker.workflow.functions import Join

from libs.config.config_vars import CONFIG, S3_DATA_PREFIX, S3_BUCKET
from libs.processing.dme_spark_sql_recipe import DmeSparkSqlRecipe

SQL_TEMPLATE = """SELECT  DISTINCT
    `cpifl_table`.`ap_data_sector` AS `ap_data_sector`,
    `cpifl_table`.`analysis_year` AS `analysis_year`,
    `cpifl_table`.`source_id` AS `source_id`,
    `cpifl_table`.`entry_id` AS `entry_id`,
    CASE 
        WHEN SUM(`cpifl_table`.`cmatf`) OVER (PARTITION BY `cpifl_table`.`ap_data_sector`, `cpifl_table`.`analysis_year`, `cpifl_table`.`source_id`) < 2
            THEN `cpifl_table`.`cpifl`
        ELSE  `cpifl_table`.`cmatf` 
    END AS `cmatf`,
    COALESCE(CASE
        WHEN `trial_pheno_input`.`decision_group_rm` = 0 AND `trial_pheno_input`.`ap_data_sector` = 'SOY_NA_SUMMER'
            THEN -1.5
        WHEN `trial_pheno_input`.`decision_group_rm` = 0.001 AND `trial_pheno_input`.`ap_data_sector` = 'SOY_NA_SUMMER'
            THEN -1
        WHEN `trial_pheno_input`.`decision_group_rm` = 0.01 AND `trial_pheno_input`.`ap_data_sector` = 'SOY_NA_SUMMER'
            THEN -0.5
        WHEN `trial_pheno_input`.`decision_group_rm` = 0.1 AND `trial_pheno_input`.`ap_data_sector` = 'SOY_NA_SUMMER'
            THEN 0
        ELSE `trial_pheno_input`.`decision_group_rm`
    END, 0) AS `decision_group_rm`,
    COALESCE(`trial_pheno_input`.`technology`, 'all') AS `technology`,
    COALESCE(`trial_pheno_input`.`stage`, 6) AS `stage`,
    `cpifl_table`.`material_type` AS `material_type`,
    COALESCE(`pvs_input`.`pvs_prediction`, 0) AS `pvs_prediction`,
    CAST(COALESCE(CASE WHEN ISNAN(`pvs_input`.`count`) THEN NULL ELSE `pvs_input`.`count` END, `trial_pheno_input`.`count`) AS integer) AS `count`,
    COALESCE(CASE WHEN ISNAN(`pvs_input`.`prediction`) THEN NULL ELSE `pvs_input`.`prediction` END, `trial_pheno_input`.`prediction`) AS `prediction`,
    COALESCE(CASE WHEN ISNAN(`pvs_input`.`stderr`) THEN NULL ELSE `pvs_input`.`stderr` END, `trial_pheno_input`.`stderr`, `trial_pheno_input`.`prediction`/10) AS `stderr`,
    `hybrid_decisions_rm`.`rm` AS `RM`
  FROM `trial_checks_py` `cpifl_table`
LEFT JOIN (
    SELECT 
        `ap_data_sector`,
        `analysis_type`,
        `analysis_year`,
        `source_id`,
        `market_seg`,
        `entry_identifier`,
        `material_type`,
        `prediction`,
        `count`,
        `stderr`,
        1 AS `pvs_prediction`
      FROM `pvs_data` `pvs_input`
      WHERE ((`trait` = 'MRTYN' AND `ap_data_sector` LIKE 'SOY_%') 
             OR (`trait` = 'GMSTP' AND `ap_data_sector` NOT LIKE 'SOY_%'))
        AND ((LOWER(`market_seg`) = 'all' AND `analysis_type` IN ('SingleExp','MultiExp','MynoET'))
            OR `analysis_type` = 'GenoPred')  
        AND `material_type` = 'entry'
) `pvs_input`
ON (`pvs_input`.`ap_data_sector` = `cpifl_table`.`ap_data_sector`)
  AND (`pvs_input`.`source_id` = `cpifl_table`.`source_id`)
  AND (`pvs_input`.`analysis_type` = `cpifl_table`.`analysis_type`)
  AND (`pvs_input`.`entry_identifier` = `cpifl_table`.`entry_id`)
  AND (`pvs_input`.`analysis_year` = `cpifl_table`.`analysis_year`)
  AND (`pvs_input`.`material_type` = `cpifl_table`.`material_type`)
LEFT JOIN (
    SELECT DISTINCT
        `analysis_year`,
        `ap_data_sector_name`,
        `entry_id`,
        `number_value` AS `rm`
      FROM `material_trait_data`
        WHERE ABS(`decision_group_rm` - `number_value`) <= 15
             OR (`decision_group_rm` IS NULL)
             OR (`decision_group_rm` < 25)
    
    UNION
    SELECT DISTINCT
        `analysis_year`,
        `ap_data_sector_name`,
        `entry_id`,
        `number_value` AS `rm`
      FROM (
        SELECT DISTINCT 
            `variety_entry_data`.`analysis_year` AS `analysis_year`,
            `variety_entry_data`.`ap_data_sector_name` AS `ap_data_sector_name`,
            `variety_entry_data`.`decision_group` AS `decision_group`,
            `variety_entry_data`.`decision_group_rm` AS `decision_group_rm`,
            `variety_entry_data`.`stage` AS `stage`,
            `variety_entry_data`.`combined` AS `combined`,
            `variety_entry_data`.`technology` AS `technology`,
            `variety_entry_data`.`entry_id` AS `entry_id`,
            `variety_trait_data`.`code` AS `code`,
            `variety_trait_data`.`number_value` AS `number_value`
          FROM `variety_entry_data` `variety_entry_data`
          INNER JOIN `variety_trait_data` `variety_trait_data`
            ON (`variety_entry_data`.`genetic_affiliation_guid` = `variety_trait_data`.`genetic_affiliation_guid`)
              AND (`variety_entry_data`.`crop_guid` = `variety_trait_data`.`crop_guid`)
      )`variety_trait_data_joined`
        WHERE ABS(`decision_group_rm` - `number_value`) <= 15 
             OR (`decision_group_rm` IS NULL)
             OR (`decision_group_rm` < 25)
) `hybrid_decisions_rm`
ON `hybrid_decisions_rm`.`ap_data_sector_name` = `cpifl_table`.`ap_data_sector`
  AND `hybrid_decisions_rm`.`analysis_year` = `cpifl_table`.`analysis_year`
  AND `hybrid_decisions_rm`.`entry_id` = `cpifl_table`.`entry_id`
LEFT JOIN(
    SELECT
        `pheno`.`ap_data_sector`,
        `pheno`.`analysis_year`,
        '${DKU_DST_analysis_type}' AS `analysis_type`,
        `config`.`decision_group` AS `source_id`,
        `config`.`decision_group_rm`,
        `config`.`stage`,
        `config`.`technology`,
        `pheno`.`entry_id`,
        `pheno`.`trait`,
        COUNT(`pheno`.`result_numeric_value`) AS`count`,
        AVG(`pheno`.`result_numeric_value`) AS `prediction`,
        STDDEV(`pheno`.`result_numeric_value`)/SQRT(COUNT(`pheno`.`result_numeric_value`)) AS `stderr`
      FROM `trial_pheno_data` `pheno`
    LEFT JOIN `ap_sector_experiment_config` `config`
      ON `pheno`.`ap_data_sector` = `config`.`ap_data_sector`
        AND `pheno`.`analysis_year` = `config`.`analysis_year`
        AND `pheno`.`experiment_id` = `config`.`experiment_id`
    WHERE (`pheno`.`trait` = 'MRTYN' AND `pheno`.`ap_data_sector` LIKE 'SOY_%') OR 
        (`pheno`.`trait` = 'GMSTP' AND `pheno`.`ap_data_sector` NOT LIKE 'SOY_%')
    GROUP BY
        `pheno`.`ap_data_sector`,
        `pheno`.`analysis_year`,
        `config`.`decision_group`,
        `config`.`decision_group_rm`,
        `config`.`stage`,
        `config`.`technology`,
        `pheno`.`entry_id`,
        `pheno`.`trait`
) `trial_pheno_input`
ON `trial_pheno_input`.`ap_data_sector` = `cpifl_table`.`ap_data_sector`
  AND `trial_pheno_input`.`analysis_year` = `cpifl_table`.`analysis_year`
  AND `trial_pheno_input`.`analysis_type` = `cpifl_table`.`analysis_type`
  AND `trial_pheno_input`.`entry_id` = `cpifl_table`.`entry_id`
  AND `trial_pheno_input`.`source_id` = `cpifl_table`.`source_id`
WHERE `cpifl_table`.`material_type` = 'entry'"""


if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        data_path = f's3a://{S3_BUCKET}/{S3_DATA_PREFIX}/{pipeline_runid}/{analysis_type}/'
        input_views = {
            'material_trait_data': f'{data_path}material_trait_data.csv',
            'pvs_data': f'{data_path}pvs_data.csv',
            'ap_sector_experiment_config': f'{data_path}ap_sector_experiment_config.csv',
            'trial_pheno_data': f'{data_path}trial_pheno_data.csv',
            'variety_trait_data': f'{data_path}variety_trait_data.csv',
            'variety_entry_data': f'{data_path}variety_entry_data.csv',
            'trial_checks_py': f'{data_path}trial_checks.parquet'
        }
        dr = DmeSparkSqlRecipe(f'{data_path}compute_pvs_for_erm2', input_views)
        dr.process(SQL_TEMPLATE)
