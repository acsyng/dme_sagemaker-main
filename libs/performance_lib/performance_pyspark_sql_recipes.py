from pyspark.sql import Window
import pyspark.sql.functions as f

def compute_trial_checks_all_breakout(spark, ap_data_sector, analysis_year, analysis_type):
    output_df = spark.sql("""
            SELECT 
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`entry_id`,
            SUM(`trial_checks`.`cpifl`) AS `cpifl`,
            SUM(`trial_checks`.`cperf`) AS `cperf`,
            SUM(`trial_checks`.`cagrf`) AS `cagrf`,
            SUM(`trial_checks`.`cmatf`) AS `cmatf`,
            SUM(`trial_checks`.`cregf`) AS `cregf`,
            SUM(`trial_checks`.`crtnf`) AS `crtnf`,
            SUM(`trial_checks`.`count`) AS `count`,
            CAST(`trial_checks`.`decision_group_rm` AS double) AS `decision_group_rm`,
            `trial_checks`.`stage`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`,
            `trial_checks`.`female_entry_id`,
            `trial_checks`.`male_entry_id`,
            SUM(`trial_checks`.`cpifl_sum`) AS `cpifl_sum`
        FROM `trial_checks`
        WHERE `market_seg` != 'all'
            AND `ap_data_sector` = {0}
            AND `analysis_year` = {1}
            AND `analysis_type` = {2}
        GROUP BY
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`entry_id`,
            `trial_checks`.`decision_group_rm`,
            `trial_checks`.`stage`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`,
            `trial_checks`.`female_entry_id`,
            `trial_checks`.`male_entry_id`

        UNION ALL
        SELECT
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`entry_id`,
            SUM(`trial_checks`.`cpifl`) AS `cpifl`,
            SUM(`trial_checks`.`cperf`) AS `cperf`,
            SUM(`trial_checks`.`cagrf`) AS `cagrf`,
            SUM(`trial_checks`.`cmatf`) AS `cmatf`,
            SUM(`trial_checks`.`cregf`) AS `cregf`,
            SUM(`trial_checks`.`crtnf`) AS `crtnf`,
            SUM(`trial_checks`.`count`) AS `count`,
            CAST(`trial_checks`.`decision_group_rm` AS double) AS `decision_group_rm`,
            MAX(`trial_checks`.`stage`) AS `stage`,
            `trial_checks`.`technology`,
            'all' AS `market_seg`,
            `trial_checks`.`material_type`,
            `trial_checks`.`female_entry_id`,
            `trial_checks`.`male_entry_id`,
            SUM(`trial_checks`.`cpifl_sum`) AS `cpifl_sum`
        FROM `trial_checks`
        WHERE `ap_data_sector` = {0}
            AND `analysis_year` = {1}
            AND `analysis_type` = {2}
        GROUP BY 
            `ap_data_sector`,
            `analysis_year`,
            `analysis_type`,
            `decision_group_rm`,
            `source_id`,
            `entry_id`,
            `technology`,
            `material_type`,
            `female_entry_id`,
            `male_entry_id`
    """.format("'"+ap_data_sector+"'", analysis_year, "'"+analysis_type+"'"))
    
    return output_df



def compute_cpifl_base(spark):
    output_df = spark.sql("""
        -- Determine check flags with 10% / 25% tolerances for outliers, multiyear & hybrid parents.
        -- If using "old" check flag only (cpifl), project across the other check flags.
        -- If no check flags are set by data, set all to "1" to enable all-to-all comparisons
        SELECT
            `check_totals`.`ap_data_sector`,
            `check_totals`.`analysis_year`,
            `check_totals`.`analysis_type`,
            `check_totals`.`source_id`,
            `check_totals`.`entry_id`,
            IF( ((`cpifl`/`count`) > `check_threshold`) OR `cpifl_sum` = 0, 1, 0) AS `cpifl`,
            CASE
                WHEN `cpifl_sum` = 0
                    THEN 1
                WHEN (`cperf`/`count`) > `check_threshold`
                    THEN 1
                ELSE 0
            END AS `cperf`,
            CASE
                WHEN `cpifl_sum` = 0
                    THEN 1
                WHEN (`cagrf`/`count`) > `check_threshold`
                    THEN 1
                ELSE 0
            END AS `cagrf`,
            CASE
                WHEN `cpifl_sum` = 0
                    THEN 1
                WHEN (`cmatf`/`count`) > `check_threshold`
                    THEN 1
                ELSE 0
            END AS `cmatf`,
            CASE
                WHEN `cpifl_sum` = 0
                    THEN 1
                WHEN (`cregf`/`count`) > `check_threshold`
                    THEN 1
                ELSE 0
            END AS `cregf`,
            CASE
                WHEN `cpifl_sum` = 0
                    THEN 1
                WHEN (`crtnf`/`count`) > `check_threshold`
                    THEN 1
                ELSE 0
            END AS `crtnf`,
            COALESCE(`check_totals`.`decision_group_rm`,0) AS `decision_group_rm`,
            `check_totals`.`stage`,
            `check_totals`.`technology`,
            `check_totals`.`market_seg`,
            `check_totals`.`material_type`,
            `check_totals`.`female_entry_id`,
            `check_totals`.`male_entry_id`
        FROM (
        SELECT
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`entry_id`,
            SUM(`trial_checks`.`cpifl`) AS `cpifl`,
            SUM(`trial_checks`.`cperf`) AS `cperf`,
            SUM(`trial_checks`.`cagrf`) AS `cagrf`,
            SUM(`trial_checks`.`cmatf`) AS `cmatf`,
            SUM(`trial_checks`.`cregf`) AS `cregf`,
            SUM(`trial_checks`.`crtnf`) AS `crtnf`,
            SUM(`trial_checks`.`count`) AS `count`,
            IF(instr(`trial_checks`.`material_type`,'ale')>0,0.25,0.05) AS `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            MAX(`trial_checks`.`stage`) AS `stage`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`,
            FIRST(`trial_checks`.`female_entry_id`) AS `female_entry_id`,
            FIRST(`trial_checks`.`male_entry_id`) AS `male_entry_id`,
            SUM(`trial_checks`.`cpifl_sum`) AS `cpifl_sum`
        FROM `trial_checks_all_breakouts` `trial_checks`
        GROUP BY
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`entry_id`,
            `trial_checks`.`source_id`,
            `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`

        UNION ALL
        --  female parent generation
        SELECT
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`female_entry_id` AS `entry_id`,
            SUM(`trial_checks`.`cpifl`) AS `cpifl`,
            SUM(`trial_checks`.`cperf`) AS `cperf`,
            SUM(`trial_checks`.`cagrf`) AS `cagrf`,
            SUM(`trial_checks`.`cmatf`) AS `cmatf`,
            SUM(`trial_checks`.`cregf`) AS `cregf`,
            SUM(`trial_checks`.`crtnf`) AS `crtnf`,
            SUM(`trial_checks`.`count`) AS `count`,
            0.25 AS `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            MAX(`trial_checks`.`stage`) AS `stage`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            'female' AS `material_type`,
            `trial_checks`.`female_entry_id` AS `female_entry_id`,
            `trial_checks`.`female_entry_id` AS `male_entry_id`,
            SUM(`trial_checks`.`cpifl_sum`) AS `cpifl_sum`
        FROM `trial_checks_all_breakouts` `trial_checks`
        WHERE `trial_checks`.`entry_id` != `trial_checks`.`female_entry_id`
        GROUP BY
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
        `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`,
            `trial_checks`.`female_entry_id`

        UNION ALL
        --  male parent generation
        SELECT
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `trial_checks`.`male_entry_id` AS `entry_id`,
            SUM(`trial_checks`.`cpifl`) AS `cpifl`,
            SUM(`trial_checks`.`cperf`) AS `cperf`,
            SUM(`trial_checks`.`cagrf`) AS `cagrf`,
            SUM(`trial_checks`.`cmatf`) AS `cmatf`,
            SUM(`trial_checks`.`cregf`) AS `cregf`,
            SUM(`trial_checks`.`crtnf`) AS `crtnf`,
            SUM(`trial_checks`.`count`) AS `count`,
            0.25 AS `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            MAX(`trial_checks`.`stage`) AS `stage`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            'male' AS `material_type`,
            `trial_checks`.`male_entry_id` AS `female_entry_id`,
            `trial_checks`.`male_entry_id` AS `male_entry_id`,
            SUM(`trial_checks`.`cpifl_sum`) AS `cpifl_sum`
        FROM `trial_checks_all_breakouts` `trial_checks`
        WHERE `trial_checks`.`entry_id` != `trial_checks`.`male_entry_id`
        GROUP BY
            `trial_checks`.`ap_data_sector`,
            `trial_checks`.`analysis_year`,
            `trial_checks`.`analysis_type`,
            `trial_checks`.`source_id`,
            `check_threshold`,
            `trial_checks`.`decision_group_rm`,
            `trial_checks`.`technology`,
            `trial_checks`.`market_seg`,
            `trial_checks`.`material_type`,
            `trial_checks`.`male_entry_id`
        )`check_totals`
    """)
    
    return output_df


def compute_cpifl_table(spark):
    output_df = spark.sql("""
            SELECT
            `cpifl_base`.`ap_data_sector`,
            `cpifl_base`.`analysis_type`,
            `cpifl_base`.`analysis_year`,
            `cpifl_base`.`source_id`,
            `cpifl_base`.`entry_id`,
            `cpifl_base`.`market_seg`,
            `cpifl_base`.`stage`,
            `cpifl_base`.`decision_group_rm`,
            `cpifl_base`.`technology`,
            `cpifl_base`.`material_type`,
            `cpifl_base`.`female_entry_id`,
            `cpifl_base`.`male_entry_id`,
            `cpifl_base`.`cpifl`,
            `cpifl_base`.`cperf`,
            `cpifl_base`.`cagrf`,
            `cpifl_base`.`cmatf`,
            `cpifl_base`.`cregf`,
            `cpifl_base`.`crtnf`,
            `cpifl_fp`.`material_type` AS `fp_material_type`,
            `cpifl_fp`.`cpifl` AS `fp_cpifl`,
            `cpifl_fp`.`cperf` AS `fp_cperf`,
            `cpifl_fp`.`cagrf` AS `fp_cagrf`,
            `cpifl_fp`.`cmatf` AS `fp_cmatf`,
            `cpifl_fp`.`cregf` AS `fp_cregf`,
            `cpifl_fp`.`crtnf` AS `fp_crtnf`,
            `cpifl_mp`.`material_type` AS `mp_material_type`,
            `cpifl_mp`.`cpifl` AS `mp_cpifl`,
            `cpifl_mp`.`cperf` AS `mp_cperf`,
            `cpifl_mp`.`cagrf` AS `mp_cagrf`,
            `cpifl_mp`.`cmatf` AS `mp_cmatf`,
            `cpifl_mp`.`cregf` AS `mp_cregf`,
            `cpifl_mp`.`crtnf` AS `mp_crtnf`
        FROM `cpifl_base`
        LEFT JOIN `cpifl_base` `cpifl_fp`
        ON `cpifl_base`.`female_entry_id` != `cpifl_base`.`male_entry_id`
            AND `cpifl_fp`.`material_type` = 'female'
            AND `cpifl_base`.`ap_data_sector` = `cpifl_fp`.`ap_data_sector`
            AND `cpifl_base`.`analysis_type` = `cpifl_fp`.`analysis_type`
            AND `cpifl_base`.`analysis_year` = `cpifl_fp`.`analysis_year`
            AND `cpifl_base`.`source_id` = `cpifl_fp`.`source_id`
            AND `cpifl_base`.`female_entry_id` = `cpifl_fp`.`entry_id`
            AND `cpifl_base`.`market_seg` = `cpifl_fp`.`market_seg`
            AND `cpifl_base`.`stage` = `cpifl_fp`.`stage`
            AND `cpifl_base`.`decision_group_rm` = `cpifl_fp`.`decision_group_rm`
            AND `cpifl_base`.`technology` = `cpifl_fp`.`technology`
        LEFT JOIN `cpifl_base` `cpifl_mp`
        ON `cpifl_base`.`female_entry_id` != `cpifl_base`.`male_entry_id`
            AND `cpifl_mp`.`material_type` = 'male'
            AND `cpifl_base`.`ap_data_sector` = `cpifl_mp`.`ap_data_sector`
            AND `cpifl_base`.`analysis_type` = `cpifl_mp`.`analysis_type`
            AND `cpifl_base`.`analysis_year` = `cpifl_mp`.`analysis_year`
            AND `cpifl_base`.`source_id` = `cpifl_mp`.`source_id`
            AND `cpifl_base`.`male_entry_id` = `cpifl_mp`.`entry_id`
            AND `cpifl_base`.`market_seg` = `cpifl_mp`.`market_seg`
            AND `cpifl_base`.`stage` = `cpifl_mp`.`stage`
            AND `cpifl_base`.`decision_group_rm` = `cpifl_mp`.`decision_group_rm`
            AND `cpifl_base`.`technology` = `cpifl_mp`.`technology`
    """)
    
    return output_df