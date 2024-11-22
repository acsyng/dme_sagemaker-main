from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, Window

import pytest

from libs.dme_pyspark_sql_queries import (
    merge_pvs_regression_input,
    merge_pvs_cpifl,
    merge_pvs_cpifl_regression,
    merge_pvs_config,
    merge_regression_results,
    merge_trial_h2h,
    merge_trial_cpifl,
    merge_trial_regression_input,
    merge_trial_config,
)


def test_merge_pvs_regression_input():
    spark = MagicMock()
    merge_pvs_regression_input(spark)
    sql = """
    SELECT 
        `pvs_data`.`ap_data_sector` AS `ap_data_sector`,
        `pvs_data`.`analysis_type` AS `analysis_type`,
        `pvs_data`.`analysis_year` AS `analysis_year`,
        `pvs_data`.`decision_group` AS `decision_group`,
        `pvs_data`.`decision_group_rm`,
        `pvs_data`.`stage`,
        `pvs_data`.`material_type`,
        `pvs_data`.`breakout_level`,
        `pvs_data`.`breakout_level_value`,
        `pvs_data`.`be_bid`,
        `pvs_data`.`count` AS `count`,
        `pvs_data`.`prediction` AS `prediction`,
        `pvs_data`.`stderr` AS `stderr`,
        `pvs_data`.`metric_name` AS `metric_name`,
        `regression_config`.`x` AS `x`,
        `regression_config`.`y` AS `y`,
        `pvs_data_2`.`prediction` AS `prediction_x`,
        1 AS `analysis_target_y`
      FROM `pvs_input` `pvs_data`
      INNER JOIN `regression_cfg` `regression_config`
        ON (`regression_config`.`analysis_year` = `pvs_data`.`analysis_year`)
          AND (`pvs_data`.`ap_data_sector` = `regression_config`.`ap_data_sector`)
          AND (`pvs_data`.`analysis_type` = `regression_config`.`analysis_type`)
          AND (`pvs_data`.`breakout_level_value` = `regression_config`.`market_seg`)
          AND ((`pvs_data`.`material_type` = `regression_config`.`material_type`))
          AND (`pvs_data`.`analysis_year` = `regression_config`.`analysis_year`)
          AND (`pvs_data`.`trait` = `regression_config`.`y`)
      LEFT JOIN `pvs_input` `pvs_data_2`
        ON (`pvs_data_2`.`analysis_year` = `pvs_data`.`analysis_year`)
          AND (`pvs_data_2`.`ap_data_sector` = `pvs_data`.`ap_data_sector`)
          AND (`pvs_data_2`.`dme_reg_x` = true)
          AND (`regression_config`.`ap_data_sector` = `pvs_data_2`.`ap_data_sector`)
          AND (`regression_config`.`market_seg` = `pvs_data_2`.`breakout_level_value`)
          AND ((`pvs_data_2`.`material_type` = `regression_config`.`material_type`))
          AND (`regression_config`.`analysis_type` = `pvs_data_2`.`analysis_type`)
          AND (`regression_config`.`analysis_year` = `pvs_data_2`.`analysis_year`)
          AND (`regression_config`.`x` = `pvs_data_2`.`trait`)
          AND (`pvs_data`.`analysis_type` = `pvs_data_2`.`analysis_type`)
          AND (`pvs_data`.`be_bid` = `pvs_data_2`.`be_bid`)
          AND (`pvs_data`.`decision_group` = `pvs_data_2`.`decision_group`)
          AND (`pvs_data`.`decision_group_rm` = `pvs_data_2`.`decision_group_rm`)
          AND (`pvs_data`.`stage` = `pvs_data_2`.`stage`)
          AND (`pvs_data`.`material_type` = `pvs_data_2`.`material_type`)
          AND (`pvs_data`.`breakout_level` = `pvs_data_2`.`breakout_level`)
          AND (`pvs_data`.`breakout_level_value` = `pvs_data_2`.`breakout_level_value`)
    WHERE (`pvs_data`.`analysis_type` NOT IN ('GenoPred', 'PhenoGCA'))
      AND (`pvs_data`.`material_type` = 'entry')
      AND (`pvs_data_2`.`analysis_type` NOT IN ('GenoPred', 'PhenoGCA'))
      AND (`pvs_data_2`.`material_type` = 'entry')
      AND (`pvs_data`.`dme_reg_y` IS true)
      AND `pvs_data`.`prediction` IS NOT NULL
      AND `pvs_data_2`.`prediction` IS NOT NULL
    """
    spark.sql.assert_called_with(sql)


def test_merge_pvs_cpifl():
    spark = MagicMock()
    merge_pvs_cpifl(spark)
    sql = """
    SELECT 
        `pvs_data`.`ap_data_sector` AS `ap_data_sector`,
        `pvs_data`.`analysis_type` AS `analysis_type`,
        `pvs_data`.`analysis_year` AS `analysis_year`,
        `pvs_data`.`decision_group` AS `decision_group`,
        `pvs_data`.`decision_group_rm` AS `decision_group_rm`,
        `pvs_data`.`stage` AS `stage`,
        `pvs_data`.`material_type` AS `material_type`,
        `pvs_data`.`breakout_level`,
        `pvs_data`.`breakout_level_value`,
        `pvs_data`.`be_bid`,
        `pvs_data`.`trait` AS `trait`,
        `pvs_data`.`count` AS `count`,
        `pvs_data`.`prediction` AS `prediction`,
        `pvs_data`.`stderr` AS `stderr`,
        NULL AS `stddev`,
        `pvs_data`.`metric_name` AS `metric_name`,
        `cpifl_table`.`par1_be_bid`,
        `cpifl_table`.`par2_be_bid`,
        COALESCE(`cpifl_table`.`cpifl`, FALSE) AS `cpifl`,
        COALESCE(CASE 
            WHEN `pvs_data`.`dme_chkfl` = 'cperf'
                THEN `cpifl_table`.`cperf`
            WHEN `pvs_data`.`dme_chkfl` = 'cagrf'
                THEN `cpifl_table`.`cagrf`
            WHEN `pvs_data`.`dme_chkfl` = 'cmatf'
                THEN `cpifl_table`.`cmatf`
            WHEN `pvs_data`.`dme_chkfl` = 'cregf'
                THEN `cpifl_table`.`cregf`
            WHEN `pvs_data`.`dme_chkfl` = 'crtnf'
                THEN `cpifl_table`.`crtnf`
            ELSE `cpifl_table`.`cpifl`
        END, FALSE) AS `chkfl`,
        COALESCE(`cpifl_table`.`cmatf`, FALSE) AS `cmatf`,
        `pvs_data`.`distribution_type`,
        `pvs_data`.`direction`,
        `pvs_data`.`yield_trait`,
        `pvs_data`.`dme_rm_est`,
        `pvs_data`.`dme_weighted_trait`
      FROM `pvs_input` `pvs_data`
      LEFT JOIN (
          SELECT
              `ap_data_sector`,
              `analysis_year`,
              `decision_group`,
              `be_bid`,
              `material_type`,
              `par1_be_bid`,
              `par2_be_bid`,
              `cpifl`,
              `cperf`,
              `cagrf`,
              `cmatf`,
              `cregf`,
              `crtnf`
            FROM `cpifl_table` 
      )`cpifl_table`
        ON (`pvs_data`.`ap_data_sector` = `cpifl_table`.`ap_data_sector`)
          AND (`pvs_data`.`analysis_year` = `cpifl_table`.`analysis_year`)
          AND (`pvs_data`.`decision_group` = `cpifl_table`.`decision_group`)
          AND (`pvs_data`.`be_bid` = `cpifl_table`.`be_bid`)
          AND ((`pvs_data`.`material_type` = `cpifl_table`.`material_type`)
              OR (`pvs_data`.`material_type` = 'female' AND `cpifl_table`.`material_type` = 'pool1')
              OR (`pvs_data`.`material_type` = 'male' AND `cpifl_table`.`material_type` = 'pool2'))
    """
    spark.sql.assert_called_with(sql)


def test_merge_pvs_cpifl_regression():
    spark = MagicMock()
    merge_pvs_cpifl_regression(spark)
    sql = """
    SELECT 
        `pvs_data`.`ap_data_sector` AS `ap_data_sector`,
        `pvs_data`.`analysis_type` AS `analysis_type`,
        `pvs_data`.`analysis_year` AS `analysis_year`,
        `pvs_data`.`decision_group` AS `decision_group`,
        `pvs_data`.`decision_group_rm` AS `decision_group_rm`,
        `pvs_data`.`stage` AS `stage`,
        `pvs_data`.`material_type` AS `material_type`,
        `pvs_data`.`breakout_level`,
        `pvs_data`.`breakout_level_value`,
        `pvs_data`.`be_bid` AS `be_bid`,
        `pvs_data`.`trait` AS `trait`,
        `pvs_data`.`count` AS `count`,
        COALESCE(
            CASE 
                WHEN `pvs_reg_output`.`adjusted` = 'outliers'
                    THEN `pvs_reg_output`.`prediction`
                ELSE `pvs_reg_output`.`adjusted_prediction`
            END,`pvs_data`.`prediction`) AS `prediction`,
        `pvs_data`.`stderr` AS `stderr`,
        NULL AS `stddev`,
        `pvs_data`.`metric_name` AS `metric_name`,
        `cpifl_table`.`par1_be_bid`,
        `cpifl_table`.`par2_be_bid`,
        COALESCE(`cpifl_table`.`cpifl`, FALSE) AS `cpifl`,
        COALESCE(CASE 
            WHEN `pvs_data`.`dme_chkfl` = 'cperf'
                THEN `cpifl_table`.`cperf`
            WHEN `pvs_data`.`dme_chkfl` = 'cagrf'
                THEN `cpifl_table`.`cagrf`
            WHEN `pvs_data`.`dme_chkfl` = 'cmatf'
                THEN `cpifl_table`.`cmatf`
            WHEN `pvs_data`.`dme_chkfl` = 'cregf'
                THEN `cpifl_table`.`cregf`
            WHEN `pvs_data`.`dme_chkfl` = 'crtnf'
                THEN `cpifl_table`.`crtnf`
            ELSE `cpifl_table`.`cpifl`
        END, FALSE) AS `chkfl`,
        COALESCE(`cpifl_table`.`cmatf`,FALSE) AS `cmatf`,
        `pvs_data`.`distribution_type`,
        `pvs_data`.`direction`,
        `pvs_data`.`yield_trait`,
        `pvs_data`.`dme_rm_est`,
        `pvs_data`.`dme_weighted_trait`
      FROM `pvs_input` `pvs_data`
      LEFT JOIN `pvs_reg_output`
        ON (`pvs_data`.`ap_data_sector` = `pvs_reg_output`.`ap_data_sector`)
          AND (`pvs_data`.`analysis_type` = `pvs_reg_output`.`analysis_type`)
          AND (`pvs_data`.`analysis_year` = `pvs_reg_output`.`analysis_year`)
          AND (`pvs_data`.`decision_group` = `pvs_reg_output`.`decision_group`)
          AND (`pvs_data`.`decision_group_rm` = `pvs_reg_output`.`decision_group_rm`)
          AND (`pvs_data`.`stage` = `pvs_reg_output`.`stage`)
          AND (`pvs_data`.`breakout_level` = `pvs_reg_output`.`breakout_level`)
          AND (`pvs_data`.`breakout_level_value` = `pvs_reg_output`.`breakout_level_value`)
          AND (`pvs_data`.`trait` = `pvs_reg_output`.`y`)
          AND (`pvs_data`.`be_bid` = `pvs_reg_output`.`be_bid`)
          AND (`pvs_data`.`material_type` = `pvs_reg_output`.`material_type`)
      INNER JOIN (
          SELECT
              `ap_data_sector`,
              `analysis_year`,
              `decision_group`,
              `be_bid`,
              `material_type`,
              `par1_be_bid`,
              `par2_be_bid`,
              `cpifl`,
              `cperf`,
              `cagrf`,
              `cmatf`,
              `cregf`,
              `crtnf`
            FROM `cpifl_table` 
      )`cpifl_table`
        ON (`pvs_data`.`ap_data_sector` = `cpifl_table`.`ap_data_sector`)
          AND (`pvs_data`.`analysis_year` = `cpifl_table`.`analysis_year`)
          AND (`pvs_data`.`decision_group` = `cpifl_table`.`decision_group`)
          AND (`pvs_data`.`be_bid` = `cpifl_table`.`be_bid`)
          AND ((`pvs_data`.`material_type` = `cpifl_table`.`material_type`)
              OR (`pvs_data`.`material_type` = 'female' AND `cpifl_table`.`material_type` = 'pool1')
              OR (`pvs_data`.`material_type` = 'male' AND `cpifl_table`.`material_type` = 'pool2'))
    """
    spark.sql.assert_called_with(sql)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()


@patch('pyspark.sql.SparkSession.sql')
def test_merge_pvs_config(mock_sql, spark):
    pvs_metric_raw_df = MagicMock()
    pvs_metric_raw_df.filter.return_value.count.return_value = 1  # Mock the count method to return an integer
    mock_sql.return_value = pvs_metric_raw_df  # Mock the SQL execution to return the mocked DataFrame

    gr_cols2 = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "source_id",
        "decision_group_rm",
        "stage",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trait",
    ]
    merge_pvs_config(spark, pvs_metric_raw_df, gr_cols2)
    sql = """
    SELECT DISTINCT
        `pvs_metric_raw`.`ap_data_sector` AS `ap_data_sector`,
        `pvs_metric_raw`.`analysis_type` AS `analysis_type`,
        `pvs_metric_raw`.`analysis_year` AS `analysis_year`,
        `pvs_metric_raw`.`decision_group` AS `decision_group`,
        `pvs_metric_raw`.`material_type` AS `material_type`,
        `pvs_metric_raw`.`breakout_level`,
        `pvs_metric_raw`.`breakout_level_value`,
        `pvs_metric_raw`.`be_bid` AS `be_bid`,
        `pvs_metric_raw`.`trait` AS `trait`,
        CAST(`pvs_metric_raw`.`count` AS integer) AS `count`,
        IF(`pvs_metric_raw`.`material_type` = 'entry' AND `pvs_metric_raw`.`prediction`<0,
           0.0,
           `pvs_metric_raw`.`prediction`
        ) AS `prediction`,
        CASE
            WHEN `pvs_metric_raw`.`stderr` > 1000 AND `pvs_metric_raw`.`distribution_type` = 'zinb'
                THEN NULL
            WHEN `pvs_metric_raw`.`stderr` < 1E-4
                THEN 1E-4
            ELSE `pvs_metric_raw`.`stderr`
        END AS `stderr`,
        CASE
            WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA')
                THEN `pvs_metric_raw`.`stddev`
            WHEN `pvs_metric_raw`.`stderr` > 1000 AND `pvs_metric_raw`.`distribution_type` = 'zinb'
                THEN NULL
            WHEN `pvs_metric_raw`.`stderr` < 1E-4
                THEN 1E-4*SQRT(`pvs_metric_raw`.`count`)
            ELSE `pvs_metric_raw`.`stderr`*SQRT(`pvs_metric_raw`.`count`)
        END AS `stddev`,
        `pvs_metric_raw`.`cpifl` AS `cpifl`,
        `pvs_metric_raw`.`chkfl` AS `chkfl`,
        `pvs_metric_raw`.`cmatf` AS `cmatf`,
        `pvs_metric_raw`.`metric_name`,
        COALESCE(
            metric_cfg_mti.distribution_type,
            metric_cfg_mti_bak.distribution_type,
            pvs_metric_raw.distribution_type
        ) AS distribution_type,
        COALESCE(
            metric_cfg_mti.direction,
            metric_cfg_mti_bak.direction,
            pvs_metric_raw.direction
        ) AS direction,
        CASE
            WHEN pvs_metric_raw.metric_name = 'h2h'
                THEN 1.0
            WHEN metric_cfg_mti.threshold_factor IS NOT NULL
                THEN metric_cfg_mti.threshold_factor
            WHEN metric_cfg_mti_bak.threshold_factor IS NOT NULL
                THEN metric_cfg_mti_bak.threshold_factor
            WHEN `pvs_metric_raw`.`distribution_type` = 'rating'
                THEN 4.0
            ELSE 1.0
        END AS `threshold_factor`,
        CASE WHEN `pvs_metric_raw`.`distribution_type` = 'rating'
            THEN 0
            ELSE 1
        END AS `spread_factor`,
        COALESCE(
            `metric_cfg_mti`.`mn_weight`,
            `metric_cfg_mti_bak`.`mn_weight`,
            CASE WHEN `pvs_metric_raw`.`yield_trait`
                THEN 1.0
                ELSE 0.0
            END) AS `weight`,
        COALESCE(
            `metric_cfg_adv`.`mn_weight`,
            `metric_cfg_adv_bak`.`mn_weight`,
            CASE WHEN `pvs_metric_raw`.`yield_trait` AND `pvs_metric_raw`.`metric_name` = 'performance'
                THEN 0.6
                WHEN `pvs_metric_raw`.`metric_name` = 'risk'
                THEN 0.3
                ELSE 0.0
            END) AS `adv_weight`,
        COALESCE(`metric_cfg_mti`.`incl_pctchk`, `metric_cfg_mti_bak`.`incl_pctchk`, 1) AS `incl_pctchk`,
        COALESCE(`metric_cfg_adv`.`incl_pctchk`, `metric_cfg_adv_bak`.`incl_pctchk`, 1) AS `adv_incl_pctchk`
    FROM `pvs_metric_raw`
    LEFT JOIN (
        SELECT * FROM `metric_cfg`
        WHERE `metric` != 'advancement'
    ) `metric_cfg_mti`
      ON `pvs_metric_raw`.`trait` = `metric_cfg_mti`.`trait`
        AND `pvs_metric_raw`.`ap_data_sector` = `metric_cfg_mti`.`ap_data_sector`
        AND `pvs_metric_raw`.`analysis_year` = `metric_cfg_mti`.`analysis_year`
        AND `pvs_metric_raw`.`stage` >= `metric_cfg_mti`.`stage_min`
        AND `pvs_metric_raw`.`stage` < `metric_cfg_mti`.`stage_max`
        AND `pvs_metric_raw`.`decision_group_rm` >= `metric_cfg_mti`.`pipeline_rm_min`
        AND `pvs_metric_raw`.`decision_group_rm` < `metric_cfg_mti`.`pipeline_rm_max`
        AND `pvs_metric_raw`.`breakout_level` = `metric_cfg_mti`.`breakout_level_1`
        AND `pvs_metric_raw`.`breakout_level_value` = `metric_cfg_mti`.`breakout_level_1_value`
        AND (`pvs_metric_raw`.`material_type` = `metric_cfg_mti`.`material_type`
            OR (`pvs_metric_raw`.`material_type` = 'female' AND `metric_cfg_mti`.`material_type` = 'pool1')
            OR (`pvs_metric_raw`.`material_type` = 'male' AND `metric_cfg_mti`.`material_type` = 'pool2'))
    LEFT JOIN (
        SELECT * FROM `metric_cfg`
        WHERE `metric` != 'advancement'
            AND `material_type` = 'entry'
            AND `breakout_level_1` = 'na'
            AND `breakout_level_1_value` = 'all'
    )`metric_cfg_mti_bak`
      ON `pvs_metric_raw`.`trait` = `metric_cfg_mti_bak`.`trait`
        AND `pvs_metric_raw`.`ap_data_sector` = `metric_cfg_mti_bak`.`ap_data_sector`
        AND `pvs_metric_raw`.`analysis_year` = `metric_cfg_mti_bak`.`analysis_year`
        AND `pvs_metric_raw`.`stage` >= `metric_cfg_mti_bak`.`stage_min`
        AND `pvs_metric_raw`.`stage` < `metric_cfg_mti_bak`.`stage_max`
        AND `pvs_metric_raw`.`decision_group_rm` >= `metric_cfg_mti_bak`.`pipeline_rm_min`
        AND `pvs_metric_raw`.`decision_group_rm` < `metric_cfg_mti_bak`.`pipeline_rm_max`
    LEFT JOIN (
        SELECT * FROM `metric_cfg`
        WHERE `metric` = 'advancement'
    ) `metric_cfg_adv`
      ON `pvs_metric_raw`.`metric_name` = `metric_cfg_adv`.`trait`
        AND `pvs_metric_raw`.`ap_data_sector` = `metric_cfg_adv`.`ap_data_sector`
        AND `pvs_metric_raw`.`analysis_year` = `metric_cfg_adv`.`analysis_year`
        AND `pvs_metric_raw`.`stage` >= `metric_cfg_adv`.`stage_min`
        AND `pvs_metric_raw`.`stage` < `metric_cfg_adv`.`stage_max`
        AND `pvs_metric_raw`.`decision_group_rm` >= `metric_cfg_adv`.`pipeline_rm_min`
        AND `pvs_metric_raw`.`decision_group_rm` < `metric_cfg_adv`.`pipeline_rm_max`
        AND `pvs_metric_raw`.`breakout_level` = `metric_cfg_adv`.`breakout_level_1`
        AND `pvs_metric_raw`.`breakout_level_value` = `metric_cfg_adv`.`breakout_level_1_value`
        AND (`pvs_metric_raw`.`material_type` = `metric_cfg_mti`.`material_type`
            OR (`pvs_metric_raw`.`material_type` = 'female' AND `metric_cfg_mti`.`material_type` = 'pool1')
            OR (`pvs_metric_raw`.`material_type` = 'male' AND `metric_cfg_mti`.`material_type` = 'pool2'))
    LEFT JOIN (
        SELECT * FROM `metric_cfg`
        WHERE `metric` = 'advancement'
            AND `material_type` = 'entry'
            AND `breakout_level_1` = 'na'
            AND `breakout_level_1_value` = 'all'
    ) `metric_cfg_adv_bak`
      ON `pvs_metric_raw`.`metric_name` = `metric_cfg_adv_bak`.`trait`
        AND `pvs_metric_raw`.`ap_data_sector` = `metric_cfg_adv_bak`.`ap_data_sector`
        AND `pvs_metric_raw`.`analysis_year` = `metric_cfg_adv_bak`.`analysis_year`
        AND `pvs_metric_raw`.`stage` >= `metric_cfg_adv_bak`.`stage_min`
        AND `pvs_metric_raw`.`stage` < `metric_cfg_adv_bak`.`stage_max`
        AND `pvs_metric_raw`.`decision_group_rm` >= `metric_cfg_adv_bak`.`pipeline_rm_min`
        AND `pvs_metric_raw`.`decision_group_rm` < `metric_cfg_adv_bak`.`pipeline_rm_max`
    """
    assert mock_sql.called


# @pytest.mark.skip(reason="assert expected value is changed")
@patch('pyspark.sql.SparkSession.sql')
@patch("libs.dme_pyspark_sql_queries.Window")
def test_merge_pvs_config_genopred(mock_window, mock_sql, spark):

    pvs_metric_raw_df = MagicMock()
    pvs_metric_raw_df.filter.return_value.count.return_value = 1
    mock_sql.return_value = pvs_metric_raw_df  # Mock the SQL execution to return the mocked DataFrame

    partition_column = "ap_data_sector"

    # Create a valid WindowSpec object
    partition_window = Window.partitionBy(partition_column)

    # Mock the Window object to return the valid WindowSpec
    mock_window.partitionBy.return_value = partition_window

    gr_cols2 = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "source_id",
        "decision_group_rm",
        "stage",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trait",
    ]

    merge_pvs_config(spark, pvs_metric_raw_df, gr_cols2)
    sql_genopred = """
            SELECT
            `pvs_metric_raw`.`ap_data_sector` AS `ap_data_sector`,
            `pvs_metric_raw`.`analysis_type` AS `analysis_type`,
            `pvs_metric_raw`.`analysis_year` AS `analysis_year`,
            `pvs_metric_raw`.`decision_group` AS `decision_group`,
            `pvs_metric_raw`.`decision_group_rm` AS `decision_group_rm`,
            `pvs_metric_raw`.`stage` AS `stage`,
            `pvs_metric_raw`.`material_type` AS `material_type`,
            `pvs_metric_raw`.`breakout_level`,
            `pvs_metric_raw`.`breakout_level_value`,
            `pvs_metric_raw`.`be_bid` AS `be_bid`,
            `pvs_metric_raw`.`trait` AS `trait`,
            CASE
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`) 
                AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN CAST(`pvs_geno_fp`.`count`+`pvs_geno_mp`.`count` AS integer)
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`)
                    THEN CAST(`pvs_geno_fp`.`count` AS integer)
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') 
                AND `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN CAST(`pvs_geno_mp`.`count` AS integer)
                ELSE CAST(`pvs_metric_raw`.`count` AS integer)
            END AS `count`,
            IF(`pvs_metric_raw`.`material_type` = 'entry' AND `pvs_metric_raw`.`prediction`<0, 
               0.0, 
               `pvs_metric_raw`.`prediction`
            ) AS `prediction`,
            CASE
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` LIKE '%ale%'
                    THEN (`pvs_metric_raw`.`prediction_stddev`/`pvs_metric_raw`.`stderr`)/power(`pvs_metric_raw`.`count`,0.5)
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`)
                 AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN (`pvs_geno_fp`.`prediction_stddev`/`pvs_geno_fp`.`stderr`)/power(`pvs_geno_fp`.`count`,0.5) + 
                        (`pvs_geno_mp`.`prediction_stddev`/`pvs_geno_mp`.`stderr`)/power(`pvs_geno_mp`.`count`,0.5)
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA')
                 AND `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`)
                    THEN (`pvs_geno_fp`.`prediction_stddev`/`pvs_geno_fp`.`stderr`)/power(`pvs_geno_fp`.`count`,0.5)*2
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') 
                AND `pvs_metric_raw`.`material_type` = 'entry'AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN (`pvs_geno_mp`.`prediction_stddev`/`pvs_geno_mp`.`stderr`)/power(`pvs_geno_mp`.`count`,0.5)*2
                ELSE `pvs_metric_raw`.`stderr`
            END AS `stderr`,
            CASE
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`) 
                AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN (`pvs_geno_fp`.`prediction_stddev`/`pvs_geno_fp`.`stderr`) + 
                        (`pvs_geno_mp`.`prediction_stddev`/`pvs_geno_mp`.`stderr`)
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND 
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_fp`.`count`)
                    THEN (`pvs_geno_fp`.`prediction_stddev`/`pvs_geno_fp`.`stderr`)*2
                WHEN `pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') AND
                `pvs_metric_raw`.`material_type` = 'entry' AND !ISNULL(`pvs_geno_mp`.`count`)
                    THEN (`pvs_geno_mp`.`prediction_stddev`/`pvs_geno_mp`.`stderr`)*2
                ELSE `pvs_metric_raw`.`prediction_stddev` / COALESCE(`pvs_metric_raw`.`stderr`,1) *2
            END AS `stddev`,
            `pvs_metric_raw`.`distribution_type`,
            `pvs_metric_raw`.`direction`,
            `pvs_metric_raw`.`yield_trait`,
            `pvs_metric_raw`.`metric_name`,
            `pvs_metric_raw`.`cpifl`,
            `pvs_metric_raw`.`chkfl`,
            `pvs_metric_raw`.`cmatf`
        FROM `pvs_geno_parent` `pvs_metric_raw`
        LEFT JOIN `pvs_geno_parent` `pvs_geno_fp`
          ON (`pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') 
                  AND `pvs_metric_raw`.`material_type` = 'entry' 
                  AND (`pvs_geno_fp`.`material_type` IN ('female', 'pool1'))
                  AND `pvs_geno_fp`.`ap_data_sector` = `pvs_metric_raw`.`ap_data_sector`)
              AND (`pvs_geno_fp`.`analysis_type` = `pvs_metric_raw`.`analysis_type`)
              AND (`pvs_geno_fp`.`analysis_year` = `pvs_metric_raw`.`analysis_year`)
              AND (`pvs_geno_fp`.`decision_group` = `pvs_metric_raw`.`decision_group`)
              AND (`pvs_geno_fp`.`stage` = `pvs_metric_raw`.`stage`)
              AND (`pvs_geno_fp`.`trait` = `pvs_metric_raw`.`trait`)
              AND (`pvs_geno_fp`.`be_bid` = `pvs_metric_raw`.`par1_be_bid`)
              AND (`pvs_geno_fp`.`breakout_level` = `pvs_metric_raw`.`breakout_level`)
              AND (`pvs_geno_fp`.`breakout_level_value` = `pvs_metric_raw`.`breakout_level_value`)
        LEFT JOIN `pvs_geno_parent` `pvs_geno_mp`
          ON (`pvs_metric_raw`.`analysis_type` in ('GenoPred', 'PhenoGCA') 
                  AND `pvs_metric_raw`.`material_type` = 'entry' 
                  AND (`pvs_geno_mp`.`material_type` IN ('male', 'pool2'))
                  AND`pvs_geno_mp`.`ap_data_sector` = `pvs_metric_raw`.`ap_data_sector`)
              AND (`pvs_geno_mp`.`analysis_type` = `pvs_metric_raw`.`analysis_type`)
              AND (`pvs_geno_mp`.`analysis_year` = `pvs_metric_raw`.`analysis_year`)
              AND (`pvs_geno_mp`.`decision_group` = `pvs_metric_raw`.`decision_group`)
              AND (`pvs_geno_mp`.`stage` = `pvs_metric_raw`.`stage`)
              AND (`pvs_geno_mp`.`trait` = `pvs_metric_raw`.`trait`)
              AND (`pvs_geno_mp`.`be_bid` = `pvs_metric_raw`.`par2_be_bid`)
              AND (`pvs_geno_mp`.`breakout_level` = `pvs_metric_raw`.`breakout_level`)
              AND (`pvs_geno_mp`.`breakout_level_value` = `pvs_metric_raw`.`breakout_level_value`)
        """
    actual_sql = mock_sql.call_args_list[0][0][0].strip()
    assert actual_sql == sql_genopred.strip(), f"Expected SQL:\n{sql_genopred.strip()}\n\nActual SQL:\n{actual_sql}"


def test_merge_regression_results():
    spark_obj = MagicMock()
    merge_regression_results(spark_obj)
    sql = """SELECT
        `trial_pheno_data`.`ap_data_sector`,
        `trial_pheno_data`.`analysis_year`,
        `trial_pheno_data`.`trial_id`,
        `trial_pheno_data`.`be_bid`,
        `trial_pheno_data`.`experiment_id`,
        `trial_pheno_data`.`et_value`,
        `trial_pheno_data`.`market_segment`,
        `trial_pheno_data`.`plot_barcode`,
        `trial_pheno_data`.`trait`,
        `trial_pheno_data`.`x_longitude`,
        `trial_pheno_data`.`y_latitude`,
        `trial_pheno_data`.`irrigation`,
        `trial_pheno_data`.`maturity_group`,
        COALESCE(`trial_pheno_regression_output`.`prediction`, `trial_pheno_data`.`result_numeric_value`) AS `result_numeric_value`
    FROM `trial_pheno_data`
    LEFT JOIN `trial_pheno_regression_output`
    ON `trial_pheno_data`.`ap_data_sector` = `trial_pheno_regression_output`.`ap_data_sector`
        AND `trial_pheno_data`.`analysis_year` = `trial_pheno_regression_output`.`analysis_year`
        AND `trial_pheno_data`.`year` = `trial_pheno_regression_output`.`year`
        AND `trial_pheno_data`.`experiment_id` = `trial_pheno_regression_output`.`experiment_id`
        AND `trial_pheno_data`.`trial_id` = `trial_pheno_regression_output`.`trial_id`
        AND `trial_pheno_data`.`be_bid` = `trial_pheno_regression_output`.`be_bid`
        AND `trial_pheno_data`.`plot_barcode` = `trial_pheno_regression_output`.`plot_barcode`
        AND `trial_pheno_data`.`trait` = `trial_pheno_regression_output`.`y`
    WHERE `trial_pheno_data`.`analysis_target` = 1"""
    spark_obj.sql.assert_called_with(sql)


def test_merge_trial_h2h():
    spark_obj = MagicMock()
    trial_comparison_metric_input_df = MagicMock()
    merge_trial_h2h(spark_obj, trial_comparison_metric_input_df)
    sql = """SELECT
        `trial_entry_match`.`ap_data_sector`,
        `trial_entry_match`.`analysis_type`,
        `trial_entry_match`.`analysis_year`,
        `trial_entry_match`.`decision_group`,
        `trial_entry_match`.`material_type`,
        `trial_entry_match`.`breakout_level`,
        `trial_entry_match`.`breakout_level_value`,
        `trial_entry_match`.`be_bid`,
        `trial_entry_match`.`trait`,
        `trial_entry_match`.`check_be_bid`,
        `trial_entry_match`.`result_numeric_value_count` AS `count`,
        `trial_check_match`.`check_result_numeric_value_count` AS `check_count`,
        `trial_entry_match`.`result_numeric_value_avg` AS `prediction`,
        `trial_check_match`.`check_result_numeric_value_avg` AS `check_prediction`,
        `trial_entry_match`.`result_numeric_value_stddev` AS `stddev`,
        `trial_check_match`.`check_result_numeric_value_stddev` AS `check_stddev`,
        `trial_entry_match`.`cpifl`,
        `trial_entry_match`.`chkfl`,
        `trial_check_match`.`check_chkfl`,
        `trial_entry_match`.`metric_name`,
        `trial_entry_match`.`distribution_type`,
        `trial_entry_match`.`direction`,
        `trial_entry_match`.`threshold_factor` AS `threshold_factor`,
        `trial_entry_match`.`spread_factor` AS `spread_factor`,
        `trial_entry_match`.`weight` AS `weight`,
        `trial_entry_match`.`adv_weight` AS `adv_weight`,
        `trial_entry_match`.`incl_pctchk` AS `incl_pctchk`,
        `trial_entry_match`.`adv_incl_pctchk` AS `adv_incl_pctchk`
    FROM (
        SELECT * FROM (
            SELECT
                `trial_pheno_metric_input`.`ap_data_sector`,
                `trial_pheno_metric_input`.`analysis_year`,
                `trial_pheno_metric_input`.`analysis_type`,
                `trial_pheno_metric_input`.`decision_group`,
                `trial_pheno_metric_input`.`material_type`,
                `trial_pheno_metric_input`.`breakout_level`,
                `trial_pheno_metric_input`.`breakout_level_value`,
                `trial_pheno_metric_input`.`be_bid` AS `be_bid`,
                `trial_pheno_metric_input`.`trait`,
                `trial_entry_list`.`be_bid` AS `check_be_bid`,
                COUNT(`trial_pheno_metric_input`.`result_numeric_value`) AS `result_numeric_value_count`,
                AVG(`trial_pheno_metric_input`.`result_numeric_value`) AS `result_numeric_value_avg`,
                STDDEV(`trial_pheno_metric_input`.`result_numeric_value`) AS `result_numeric_value_stddev`,
                CAST(MAX(`trial_pheno_metric_input`.`cpifl`) AS boolean) AS `cpifl`,
                CAST(MAX(`trial_pheno_metric_input`.`chkfl`) AS boolean) AS `chkfl`,
                `trial_pheno_metric_input`.`metric_name`,
                `trial_pheno_metric_input`.`distribution_type`,
                `trial_pheno_metric_input`.`direction`,
                ROUND(AVG(`trial_pheno_metric_input`.`threshold_factor`),4) AS `threshold_factor`,
                ROUND(AVG(`trial_pheno_metric_input`.`spread_factor`),4) AS `spread_factor`,
                ROUND(AVG(`trial_pheno_metric_input`.`weight`),4) AS `weight`,
                ROUND(AVG(`trial_pheno_metric_input`.`adv_weight`),4) AS `adv_weight`,
                MAX(`trial_pheno_metric_input`.`incl_pctchk`) AS `incl_pctchk`,
                MAX(`trial_pheno_metric_input`.`adv_incl_pctchk`) AS `adv_incl_pctchk`
            FROM `trial_pheno_metric_input`
            INNER JOIN `trial_entry_list`
                ON `trial_entry_list`.`ap_data_sector` = `trial_pheno_metric_input`.`ap_data_sector`
                AND `trial_entry_list`.`analysis_year` = `trial_pheno_metric_input`.`analysis_year`
                AND `trial_entry_list`.`analysis_type` = `trial_pheno_metric_input`.`analysis_type`
                AND `trial_entry_list`.`decision_group` = `trial_pheno_metric_input`.`decision_group`
                AND `trial_entry_list`.`trial_id` = `trial_pheno_metric_input`.`trial_id`
                AND `trial_entry_list`.`material_type` = `trial_pheno_metric_input`.`material_type`
                AND `trial_entry_list`.`trait` = `trial_pheno_metric_input`.`trait`
            WHERE `trial_entry_list`.`cpifl`
            GROUP BY
                `trial_pheno_metric_input`.`ap_data_sector`,
                `trial_pheno_metric_input`.`analysis_year`,
                `trial_pheno_metric_input`.`analysis_type`,
                `trial_pheno_metric_input`.`decision_group`,
                `trial_pheno_metric_input`.`material_type`,
                `trial_pheno_metric_input`.`breakout_level`,
                `trial_pheno_metric_input`.`breakout_level_value`,
                `trial_pheno_metric_input`.`be_bid`,
                `trial_pheno_metric_input`.`trait`,
                `trial_entry_list`.`be_bid`,
                `trial_pheno_metric_input`.`metric_name`,
                `trial_pheno_metric_input`.`distribution_type`,
                `trial_pheno_metric_input`.`direction`
        )`trial_entry_match`
        WHERE `trial_entry_match`.`result_numeric_value_count` > 2
    ) `trial_entry_match`
    INNER JOIN (
        SELECT * FROM (
            SELECT
                `trial_pheno_metric_input`.`ap_data_sector`,
                `trial_pheno_metric_input`.`analysis_year`,
                `trial_pheno_metric_input`.`analysis_type`,
                `trial_pheno_metric_input`.`decision_group`,
                `trial_pheno_metric_input`.`material_type`,
                `trial_pheno_metric_input`.`breakout_level`,
                `trial_pheno_metric_input`.`breakout_level_value`,
                `trial_pheno_metric_input`.`be_bid` AS `check_be_bid`,
                `trial_pheno_metric_input`.`trait`,
                `trial_entry_list`.`be_bid` AS `be_bid`,
                COUNT(`trial_pheno_metric_input`.`result_numeric_value`) AS `check_result_numeric_value_count`,
                AVG(`trial_pheno_metric_input`.`result_numeric_value`) AS `check_result_numeric_value_avg`,
                STDDEV(`trial_pheno_metric_input`.`result_numeric_value`) AS `check_result_numeric_value_stddev`,
                CAST(MAX(`trial_pheno_metric_input`.`chkfl`)AS boolean) AS `check_chkfl`
            FROM `trial_pheno_metric_input`
            INNER JOIN `trial_entry_list`
                ON `trial_entry_list`.`ap_data_sector` = `trial_pheno_metric_input`.`ap_data_sector`
                AND `trial_entry_list`.`analysis_year` = `trial_pheno_metric_input`.`analysis_year`
                AND `trial_entry_list`.`analysis_type` = `trial_pheno_metric_input`.`analysis_type`
                AND `trial_entry_list`.`decision_group` = `trial_pheno_metric_input`.`decision_group`
                AND `trial_entry_list`.`trial_id` = `trial_pheno_metric_input`.`trial_id`
                AND `trial_entry_list`.`material_type` = `trial_pheno_metric_input`.`material_type`
                AND `trial_entry_list`.`trait` = `trial_pheno_metric_input`.`trait`
            WHERE `trial_pheno_metric_input`.`cpifl`
            GROUP BY
                `trial_pheno_metric_input`.`ap_data_sector`,
                `trial_pheno_metric_input`.`analysis_year`,
                `trial_pheno_metric_input`.`analysis_type`,
                `trial_pheno_metric_input`.`decision_group`,
                `trial_pheno_metric_input`.`material_type`,
                `trial_pheno_metric_input`.`breakout_level`,
                `trial_pheno_metric_input`.`breakout_level_value`,
                `trial_pheno_metric_input`.`be_bid`,
                `trial_pheno_metric_input`.`trait`,
                `trial_entry_list`.`be_bid`
        ) `trial_entry_check_match`
        WHERE `trial_entry_check_match`.`check_result_numeric_value_count` > 2
    )`trial_check_match`
    ON `trial_entry_match`.`ap_data_sector` = `trial_check_match`.`ap_data_sector`
        AND `trial_entry_match`.`analysis_year` = `trial_check_match`.`analysis_year`
        AND `trial_entry_match`.`analysis_type` = `trial_check_match`.`analysis_type`
        AND `trial_entry_match`.`decision_group` = `trial_check_match`.`decision_group`
        AND `trial_entry_match`.`material_type` = `trial_check_match`.`material_type`
        AND `trial_entry_match`.`breakout_level` = `trial_check_match`.`breakout_level`
        AND `trial_entry_match`.`breakout_level_value` = `trial_check_match`.`breakout_level_value`
        AND `trial_entry_match`.`trait` = `trial_check_match`.`trait`
        AND `trial_entry_match`.`be_bid` = `trial_check_match`.`be_bid`
        AND `trial_entry_match`.`check_be_bid` = `trial_check_match`.`check_be_bid`"""
    spark_obj.sql.assert_called_with(sql)


def test_merge_trial_cpifl():
    spark_obj = MagicMock()
    mock_df = MagicMock()         # Mock the DataFrame returned by the sql method
    spark_obj.sql.return_value = mock_df

    mock_filtered_df = MagicMock()        # Mock the filter method to return another mocked DataFrame
    mock_df.filter.return_value = mock_filtered_df

    mock_filtered_df.count.return_value = 1     # Mock the count method to return a specific integer value

    merge_trial_cpifl(spark_obj, "numeric")

    sql_calls = spark_obj.sql.mock_calls

    sql = """
    SELECT
        tr_data.ap_data_sector,
        tr_data.analysis_year,
        tr_data.analysis_type,
        cpifl_table.decision_group,
        cpifl_table.be_bid,
        cpifl_table.material_type,
        tr_data.trial_id,
        tr_data.decision_group_rm,
        tr_data.stage,
        tr_data.breakout_level,
        tr_data.breakout_level_value,
        tr_data.trait,
        tr_data.result_numeric_value,
        cpifl_table.par1_be_bid,
        cpifl_table.par2_be_bid,
        tr_data.metric_name AS metric_name,
        tr_data.distribution_type,
        tr_data.direction,
        tr_data.yield_trait,
        tr_data.dme_rm_est,
        tr_data.dme_weighted_trait,
        tr_data.dme_chkfl,
        COALESCE(cpifl_table.cpifl, FALSE) AS cpifl,
        COALESCE(CASE 
            WHEN tr_data.dme_chkfl = 'cperf'
                THEN cpifl_table.cperf
            WHEN tr_data.dme_chkfl = 'cagrf'
                THEN cpifl_table.cagrf
            WHEN tr_data.dme_chkfl = 'cmatf'
                THEN cpifl_table.cmatf
            WHEN tr_data.dme_chkfl = 'cregf'
                THEN cpifl_table.cregf
            WHEN tr_data.dme_chkfl = 'crtnf'
                THEN cpifl_table.crtnf
            ELSE cpifl_table.cpifl
        END, FALSE) AS chkfl
        FROM tr_data1 tr_data
        INNER JOIN cpifl_table
            ON tr_data.ap_data_sector = cpifl_table.ap_data_sector
                AND tr_data.analysis_year = cpifl_table.analysis_year
                AND tr_data.be_bid = cpifl_table.be_bid
                AND tr_data.decision_group = cpifl_table.decision_group
                AND cpifl_table.material_type = 'entry'
    """
    assert sql_calls[0].args[0].strip() == sql.strip()


def test_merge_trial_cpifl_not_genopred():
    spark_obj = MagicMock()
    mock_df = MagicMock()

    spark_obj.sql.return_value = mock_df
    mock_df.filter.return_value.count.return_value = 1  # or any integer value you expect

    merge_trial_cpifl(spark_obj, "alpha")

    sql_calls = spark_obj.sql.mock_calls

    sql = """
            SELECT
        tr_data.ap_data_sector,
        tr_data.analysis_year,
        tr_data.analysis_type,
        cpifl_table.decision_group,
        cpifl_table.be_bid,
        cpifl_table.material_type,
        tr_data.trial_id,
        tr_data.decision_group_rm,
        tr_data.stage,
        tr_data.breakout_level,
        tr_data.breakout_level_value,
        tr_data.trait,
        tr_data.result_alpha_value,
        cpifl_table.par1_be_bid,
        cpifl_table.par2_be_bid,
        tr_data.metric_name AS metric_name,
        tr_data.distribution_type,
        tr_data.direction,
        tr_data.yield_trait,
        tr_data.dme_rm_est,
        tr_data.dme_weighted_trait,
        tr_data.dme_chkfl,
        COALESCE(cpifl_table.cpifl, FALSE) AS cpifl,
        COALESCE(CASE 
            WHEN tr_data.dme_chkfl = 'cperf'
                THEN cpifl_table.cperf
            WHEN tr_data.dme_chkfl = 'cagrf'
                THEN cpifl_table.cagrf
            WHEN tr_data.dme_chkfl = 'cmatf'
                THEN cpifl_table.cmatf
            WHEN tr_data.dme_chkfl = 'cregf'
                THEN cpifl_table.cregf
            WHEN tr_data.dme_chkfl = 'crtnf'
                THEN cpifl_table.crtnf
            ELSE cpifl_table.cpifl
        END, FALSE) AS chkfl
        FROM tr_data1 tr_data
        INNER JOIN cpifl_table
            ON tr_data.ap_data_sector = cpifl_table.ap_data_sector
                AND tr_data.analysis_year = cpifl_table.analysis_year
                AND tr_data.be_bid = cpifl_table.be_bid
                AND tr_data.decision_group = cpifl_table.decision_group
                AND cpifl_table.material_type = 'entry'
            """
    assert sql_calls[0].args[0].strip() == sql.strip()


def test_merge_trial_regression_input():
    spark_obj = MagicMock()

    merge_trial_regression_input(spark_obj, "SingleExp", "CORN_NA_SUMMER")
    sql = """
    SELECT * 
      FROM( 
        SELECT 
            `trial_pheno_data`.`ap_data_sector` AS `ap_data_sector`,
            `trial_pheno_data`.`analysis_year` AS `analysis_year`,
            `trial_pheno_data`.`trial_id` AS `trial_id`,
            `trial_pheno_data`.`be_bid` AS `be_bid`,
            `trial_pheno_data`.`year` AS `year`,
            `trial_pheno_data`.`experiment_id` AS `experiment_id`,
            `regression_config`.`x` AS `x`,
            `regression_config`.`y` AS `y`,
            `regression_config`.`function` AS `function`,
            `trial_pheno_data`.`plot_barcode` AS `plot_barcode`,
            `trial_pheno_data`.`trait` AS `trait`,
            `trial_pheno_data_x`.`result_numeric_value` AS `prediction_x`,
            `trial_pheno_data`.`result_numeric_value` AS `prediction`,
            `trial_pheno_data`.`analysis_target` AS `analysis_target_y`,
            MAX(DENSE_RANK(`trial_pheno_data_x`.`result_numeric_value`) OVER (PARTITION BY `trial_pheno_data`.`trial_id` ORDER BY `trial_pheno_data_x`.`result_numeric_value`)) OVER (PARTITION BY `trial_pheno_data`.`trial_id`) AS `trial_pts`,
            SUM(`trial_pheno_data`.`analysis_target`) OVER (PARTITION BY `trial_pheno_data`.`trial_id`) AS `analysis_pts`
        FROM `trial_pheno_numeric_input` `trial_pheno_data`
        INNER JOIN (
            SELECT DISTINCT
                `analysis_year`,
                `ap_data_sector`,
                `x`,
                `y`,
                `function`
            FROM `regression_cfg`
            WHERE `analysis_type` = 'trial'
                AND `market_seg` = 'all'
                AND `analysis_year` = SingleExp
                AND `ap_data_sector` = 'CORN_NA_SUMMER'
        )`regression_config`
        ON (`trial_pheno_data`.`ap_data_sector` = `regression_config`.`ap_data_sector`)
              AND (`trial_pheno_data`.`dme_reg_y` = true)
              AND (`trial_pheno_data`.`analysis_year` = `regression_config`.`analysis_year`)
              AND (`trial_pheno_data`.`trait` = `regression_config`.`y`)
        INNER JOIN (
            SELECT DISTINCT
            `plot_barcode`,
            `trait`,
            `result_numeric_value`
            FROM `trial_pheno_numeric_input`
            WHERE `dme_reg_x` = true
                AND `analysis_year` = SingleExp
                AND `ap_data_sector` = 'CORN_NA_SUMMER' 
        ) `trial_pheno_data_x`
        ON `trial_pheno_data_x`.`plot_barcode` = `trial_pheno_data`.`plot_barcode`
            AND (`trial_pheno_data_x`.`trait` = `regression_config`.`x`)
        WHERE `trial_pheno_data`.`analysis_year` = SingleExp
            AND `trial_pheno_data`.`ap_data_sector` = 'CORN_NA_SUMMER'
    )
    WHERE `trial_pts` > 2 AND `analysis_pts` > 1
    """
    spark_obj.sql.assert_called_with(sql)


def test_merge_trial_config():
    spark_obj = MagicMock()

    merge_trial_config(spark_obj, "numeric")
    sql = """SELECT
        trc_data.ap_data_sector,
        trc_data.analysis_year,
        trc_data.analysis_type,
        trc_data.decision_group,
        trc_data.be_bid,
        trc_data.material_type,
        trc_data.breakout_level,
        trc_data.breakout_level_value,
        trc_data.trial_id,
        trc_data.trait,
        trc_data.result_numeric_value,
        trc_data.metric_name,
        trc_data.dme_rm_est,
        trc_data.dme_weighted_trait,
        trc_data.cpifl,
        trc_data.chkfl,
        COALESCE(metric_cfg_mti.distribution_type,trc_data.distribution_type) AS distribution_type,
        COALESCE(metric_cfg_mti.direction, trc_data.direction) AS direction,
        COALESCE(
                CASE WHEN trc_data.metric_name = 'h2h'
                    THEN 1.0
                    ELSE metric_cfg_mti.threshold_factor
                END, 
                CASE WHEN trc_data.distribution_type = 'rating'
                    THEN 4.0
                    ELSE 1.0
                END) AS threshold_factor,
            COALESCE(metric_cfg_mti.spread_factor, 
                CASE WHEN trc_data.distribution_type = 'rating'
                    THEN 0
                    ELSE 1
                END
            ) AS spread_factor,
        COALESCE(
            CASE WHEN trc_data.metric_name = 'stability'
                THEN metric_cfg_mti.var_weight
                ELSE metric_cfg_mti.mn_weight
            END,
            CASE WHEN trc_data.yield_trait
                THEN 1.0
                ELSE 0.0
            END) AS weight,
        COALESCE(
            CASE WHEN trc_data.metric_name = 'stability'
                THEN metric_cfg_adv.var_weight
                ELSE metric_cfg_adv.mn_weight
            END, 
            CASE WHEN trc_data.yield_trait AND trc_data.metric_name = 'performance'
                THEN 0.6
                WHEN trc_data.yield_trait AND trc_data.metric_name = 'stability'
                THEN 0.1
                WHEN trc_data.metric_name = 'risk'
                THEN 0.3
                ELSE 0.0
            END) AS adv_weight,
        COALESCE(`metric_cfg_mti`.`incl_pctchk`, 1) AS `incl_pctchk`,
        COALESCE(`metric_cfg_adv`.`incl_pctchk`, 1) AS `adv_incl_pctchk`
    FROM trc_data3 trc_data
    LEFT JOIN metric_cfg metric_cfg_mti
      ON metric_cfg_mti.metric != 'advancement'
        AND trc_data.trait = metric_cfg_mti.trait
        AND trc_data.ap_data_sector = metric_cfg_mti.ap_data_sector
        AND trc_data.analysis_year = metric_cfg_mti.analysis_year
        AND trc_data.stage >= metric_cfg_mti.stage_min
        AND trc_data.stage < metric_cfg_mti.stage_max
        AND trc_data.decision_group_rm >= metric_cfg_mti.pipeline_rm_min
        AND trc_data.decision_group_rm < metric_cfg_mti.pipeline_rm_max
        AND trc_data.breakout_level = metric_cfg_mti.breakout_level_1
        AND trc_data.breakout_level_value = metric_cfg_mti.breakout_level_1_value
        AND trc_data.material_type = metric_cfg_mti.material_type
    LEFT JOIN metric_cfg metric_cfg_adv
      ON metric_cfg_adv.metric = 'advancement'
        AND trc_data.metric_name = metric_cfg_adv.trait
        AND metric_cfg_mti.ap_data_sector = metric_cfg_adv.ap_data_sector
        AND metric_cfg_mti.analysis_year = metric_cfg_adv.analysis_year
        AND metric_cfg_mti.material_type = metric_cfg_adv.material_type
        AND trc_data.ap_data_sector = metric_cfg_adv.ap_data_sector
        AND trc_data.analysis_year = metric_cfg_adv.analysis_year
        AND trc_data.stage >= metric_cfg_adv.stage_min
        AND trc_data.stage < metric_cfg_adv.stage_max
        AND trc_data.decision_group_rm >= metric_cfg_adv.pipeline_rm_min
        AND trc_data.decision_group_rm < metric_cfg_adv.pipeline_rm_max
        AND trc_data.breakout_level = metric_cfg_adv.breakout_level_1
        AND trc_data.breakout_level_value = metric_cfg_adv.breakout_level_1_value
        AND trc_data.material_type = metric_cfg_adv.material_type"""
    spark_obj.sql.assert_called_with(sql)
