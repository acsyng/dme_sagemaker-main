import pandas as pd
import polars as pl

from unittest.mock import patch, Mock, MagicMock

import pytest

from libs.dme_sql_queries import (
    get_analysis_types,
    get_breakout_str,
    get_source_ids,
    query_pvs_input,
    get_data_sector_config,
    query_check_entries,
    query_trial_input,
    create_asec_str,
    create_astc_str,
    create_fe_str,
    query_pvs_trait_list,
    query_pvs_material_types,
)


@pytest.mark.parametrize(
    "gen_str,bench_str",
    [
        ("late_phenohybrid1yr", "('SingleExp', 'MultiExp')"),
        ("late_mynoet", "('MynoET')"),
        ("late_phenogca", "('PhenoGCA')"),
        ("genomic_prediction", "('GenoPred')"),
    ],
)
def test_get_analysis_types(gen_str, bench_str):
    ("late_phenohybrid1yr")
    assert get_analysis_types(gen_str) == bench_str


@pytest.fixture()
@patch("libs.dme_sql_queries.CloudWatchLogger")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_get_source_ids_refresh_false(
    rparam_helper_mock, dbdriver_mock, pd_mock, log1_mock, log2_mock
):
    pd_mock.return_value = pd.DataFrame()
    results = get_source_ids(
        "CORN_PHILIPPINES_ALL", 2023, "late_phenohybrid1yr", "wce", "False"
    )

    assert isinstance(results, str)


@pytest.fixture()
@patch("libs.dme_sql_queries.CloudWatchLogger")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_get_source_ids_refresh_true(dbdriver_mock, pd_mock):
    get_source_ids("CORN_PHILIPPINES_ALL", 2023, "late_phenohybrid1yr", "wce", "True")
    sql = """
                SELECT DISTINCT pvs."source_id", pvs.pipeline_runid
                    FROM(
                SELECT "source_id", "trait", MAX("pipeline_runid") AS pipeline_runid
                    FROM rv_ap_all_pvs
                WHERE "ap_data_sector" = 'CORN_PHILIPPINES_ALL'
                    AND CAST("source_year" as integer) IN (2023)
                    AND "analysis_type" in ('SingleExp', 'MultiExp')
                    AND LOWER("loc") = 'all'
                GROUP BY "source_id", "trait"
                ) pvs
            INNER JOIN(
                SELECT "decision_group"
                    FROM rv_ap_sector_experiment_config
                WHERE "ap_data_sector_name" = 'CORN_PHILIPPINES_ALL' 
                    AND CAST("analysis_year" as integer) IN (2023)
                    AND "adapt_display" = 1
            ) asec
                ON asec."decision_group" = pvs."source_id"
                INNER JOIN(
                SELECT "trait"
                    FROM rv_ap_sector_trait_config
                WHERE "ap_data_sector_name" = 'CORN_PHILIPPINES_ALL'
                    AND CAST("analysis_year" as integer) IN (2023)
                    AND LOWER("distribution_type") IN ('normal', 'norm', 'rating', 'zinb', 'text')
                    AND LOWER("direction") IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                    AND LOWER("dme_metric") != 'na'
            ) astc
            ON pvs."trait" = astc."trait" 
            """
    pd_mock.assert_called_with(sql, dbdriver_mock.connect.return_value)


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_query_pvs_input(param_helper_mock, dbdriver_mock, pl_mock):
    query_pvs_input(
        "CORN_NA_SUMMER",
        2023,
        "phenohybrid1yr",
        "('24ABC00001', '24ABC00002', '24ABC00003')",
        "wce",
    )
    sql = """
    SELECT 
        pvs.ap_data_sector,
        pvs.analysis_type,
        pvs.analysis_year,
        pvs.decision_group,
        asec.stage,
        asec.decision_group_rm,
        COALESCE(fe.breakout_level, 'na') AS breakout_level,
        pvs.breakout_level_value,
        pvs.trait,
        pvs.be_bid,
        pvs.material_type,
        COALESCE(CAST(pvs.count AS integer), 0) AS count,
        CASE WHEN pvs.material_type IN ('entry', 'hybrid')
            THEN GREATEST_IGNORE_NULLS(0.0, pvs.prediction)
            ELSE pvs.prediction
        END AS prediction,
        CASE 
            WHEN pvs.stderr IS NULL 
                AND LOWER(pvs.material_type) = 'entry' 
                AND LOWER(astc.distribution_type) LIKE 'norm%'
                THEN pvs.prediction*10
            WHEN pvs.stderr > 1000 
                AND pvs.prediction < 500
                THEN LEAST_IGNORE_NULLS(pvs.stderr, pvs.prediction*10) 
            ELSE pvs.stderr
        END AS stderr,
        astc.distribution_type,
        astc.direction,
        astc.yield_trait,
        astc.metric_name,
        astc.dme_chkfl,
        astc.dme_reg_x,
        astc.dme_reg_y,
        astc.dme_rm_est,
        astc.dme_weighted_trait
      FROM (
        SELECT
            ap_data_sector AS ap_data_sector,
            CAST(source_year AS integer) AS analysis_year,
            analysis_type AS analysis_type,
            source_id AS decision_group,
            pipeline_runid AS pipeline_runid,
            REPLACE(TRIM(LOWER(market_seg)), ' ', '_') AS breakout_level_value,
            trait AS trait,
            entry_identifier AS be_bid,
            COALESCE(LOWER(material_type), 'entry') AS material_type,
            count AS count,
            prediction AS prediction,
            stderr AS stderr,
            ROW_NUMBER() OVER (PARTITION BY source_year, analysis_type, source_id, entry_identifier, market_seg, trait ORDER BY pipeline_runid desc) AS row_number
          FROM rv_ap_all_pvs
        WHERE ap_data_sector = 'CORN_NA_SUMMER'
          AND CAST(source_year AS integer) IN (2023)
          AND analysis_type IN ('SingleExp', 'MultiExp')
          AND source_id IN ('24ABC00001', '24ABC00002', '24ABC00003')
          AND loc = 'ALL'
          AND prediction IS NOT NULL
    ) pvs
    INNER JOIN ( 
    select DISTINCT
        ap_data_sector,
        analysis_year,
        CASE WHEN COUNT(experiment_id) OVER (PARTITION BY decision_group) = 1
            THEN 'SingleExp'
            WHEN MIN(year) OVER (PARTITION BY decision_group) = MAX(year) OVER (PARTITION BY decision_group)
            THEN 'MynoET'
            ELSE 'MultiExp'
        END as analysis_type,
        decision_group,
        
        MAX(CASE WHEN year = max_year then stage else null end) OVER (PARTITION BY decision_group) as stage,
        median(CASE when year = max_year then decision_group_rm else null end) OVER (PARTITION BY decision_group) as decision_group_rm
    from (
        select
            asec.ap_data_sector_name as ap_data_sector,
            CAST(MEDIAN( asec.analysis_year) as INTEGER) as analysis_year,
            asec.decision_group,
            asec.experiment_id,
            round(coalesce(MAX(try_to_number(t.stage)), MAX(asec.stage)),2) as stage,
            round(coalesce(median(asec.decision_group_rm), MEDIAN(try_to_number(t.maturity_group)), 0),3) as decision_group_rm,
            MEDIAN(t.year) as year,
            MAX(MAX(t.year)) OVER (PARTITION BY asec.decision_group) as max_year from 
        rv_ap_sector_experiment_config asec
        join rv_experiments_loc360 ex on asec.experiment_id = ex.code
        join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
        where not ex.invalidated and not t.invalidated AND not t.exclude_from_analysis
            and ap_data_sector_name = 'CORN_NA_SUMMER'
                AND CAST(analysis_year AS integer) IN (2023)
                AND decision_group in ('24ABC00001', '24ABC00002', '24ABC00003')
        group by all
    )
     ) asec
      ON pvs.decision_group = asec.decision_group
      AND pvs.analysis_year = asec.analysis_year
    INNER JOIN ( 
     SELECT
            ap_data_sector,
            analysis_year,
            trait,
            CASE 
                WHEN distribution_type ='norm'
                    THEN 'normal'
                WHEN distribution_type = 'rating' AND metric_name = 'h2h'
                    THEN 'normal'
                ELSE distribution_type 
            END AS distribution_type,
            direction,
            conv_operator,
            conv_factor,
            yield_trait,
            CASE
                WHEN metric_name IN ('agronomic', 'disease')
                    THEN 'risk'
                WHEN metric_name = 'yield' 
                    THEN 'performance'
                ELSE metric_name
            END AS metric_name,
            dme_chkfl,
            dme_reg_x,
            dme_reg_y,
            dme_rm_est,
            
            dme_weighted_trait
        FROM (
            SELECT 
                ap_data_sector_name AS ap_data_sector,
                CAST(analysis_year AS integer) AS analysis_year,
                trait,
                TRIM(LOWER(distribution_type)) AS distribution_type,
                TRIM(LOWER(direction)) AS direction,
                conv_operator,
                conv_factor,
                CASE WHEN yield_trait = '1' AND TRIM(LOWER(dme_metric)) IN ('performance', 'yield') THEN TRUE ELSE FALSE END AS yield_trait,
                TRIM(LOWER(dme_metric)) AS metric_name,
                dme_chkfl,
                dme_reg_x,
                dme_reg_y,
                dme_rm_est,
                dme_weighted_trait,
                
                ROW_NUMBER() OVER (PARTITION BY analysis_year, trait ORDER BY distribution_type, direction) as row_number FROM rv_ap_sector_trait_config
            WHERE ap_data_sector_name = 'CORN_NA_SUMMER'
                AND CAST(analysis_year AS integer) IN (2023)
                AND TRIM(LOWER(distribution_type)) IN ('normal', 'norm', 'rating', 'zinb', 'text')
                AND TRIM(LOWER(direction)) IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                AND LOWER(level) = 'plot'
                AND (dme_metric != 'na' OR dme_metric is not null or dme_metric != '' OR dme_reg_x = true OR dme_reg_y = true OR dme_rm_est != 0 OR dme_weighted_trait != 0)
                
        ) astc
        WHERE row_number = 1
             ) astc
      ON astc.ap_data_sector = asec.ap_data_sector
        AND astc.analysis_year = asec.analysis_year
        AND astc.trait = pvs.trait
    LEFT JOIN ( SELECT DISTINCT
            
            LOWER(feature_name) AS breakout_level,
            LOWER(REGEXP_REPLACE(TRANSLATE(TRIM(TRIM(value), '.'),'-','_'), '[ :;*+/&\_,]+', '_')) AS breakout_level_value
        FROM rv_feature_export
        WHERE LOWER(feature_name) IN ('wce')
            AND market_name = 'CORN_NA_SUMMER' ) fe
    ON pvs.breakout_level_value = fe.breakout_level_value
        AND fe.breakout_level_value != 'all'
    WHERE pvs.row_number = 1"""
    pl_mock.assert_called_with(sql, dbdriver_mock.connect.return_value)


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_query_check_entries(
    param_helper_mock, dbdriver_mock, pd_mock, log1_mock, log2_mock
):
    query_check_entries("CORN_NA_SUMMER", 2023)
    sql = """
        SELECT DISTINCT
        "checks".ap_data_sector AS "ap_data_sector",
        "checks".analysis_year AS "analysis_year",
        "checks".decision_group AS "decision_group",
        "checks".untested_entry_display AS "untested_entry_display",
        "checks".be_bid AS "be_bid",
        CAST(GREATEST("checks".cpifl,
             "checks".cperf, 
             "checks".cagrf, 
             "checks".cmatf,
             "checks".cregf,
             "checks".crtnf) AS boolean) AS "cpifl",
        "checks".cperf AS "cperf",
        "checks".cagrf AS "cagrf",
        "checks".cmatf AS "cmatf",
        "checks".cregf AS "cregf",
        "checks".crtnf AS "crtnf",
        CASE 
            WHEN "cmt".fp_het_pool = 'pool1' 
                THEN "checks".fp_ltb
            WHEN "cmt".mp_het_pool = 'pool1'
                THEN "checks".mp_ltb
            WHEN  "cmt".tester_role = 'M' 
                THEN CAST(GREATEST("checks".cpifl,
                 "checks".cperf, 
                 "checks".cagrf, 
                 "checks".cmatf,
                 "checks".cregf,
                 "checks".crtnf) AS boolean)
             ELSE "checks".fp_ltb
        END AS "fp_ltb",
        CASE 
            WHEN "cmt".mp_het_pool = 'pool2' 
                THEN "checks".mp_ltb
            WHEN "cmt".fp_het_pool = 'pool2'
                THEN "checks".fp_ltb
            WHEN "cmt".tester_role = 'F' 
                THEN CAST(GREATEST("checks".cpifl,
                 "checks".cperf, 
                 "checks".cagrf, 
                 "checks".cmatf,
                 "checks".cregf,
                 "checks".crtnf) AS boolean)
             ELSE "checks".mp_ltb 
        END AS "mp_ltb",
        'entry' AS "material_type",
        CASE 
            WHEN "cmt".fp_het_pool = 'pool1'
                THEN "cmt".fp_be_bid
            WHEN "cmt".mp_het_pool = 'pool1'
                THEN "cmt".mp_be_bid
            ELSE COALESCE("cmt".fp_be_bid, "bbal".receiver_p)
        END AS "par1_be_bid",
        CASE 
            WHEN "cmt".mp_het_pool = 'pool2'
                THEN "cmt".mp_be_bid
            WHEN "cmt".fp_het_pool = 'pool2'
                THEN "cmt".fp_be_bid
            ELSE COALESCE("cmt".mp_be_bid, "bbal".donor_p)
        END AS "par2_be_bid"
    FROM(
        select 
            ec.ap_data_sector_name AS ap_data_sector,
            ec.analysis_year,
            ec.decision_group,
            mbu.be_bid,
            max(ec.untested_entry_display) as untested_entry_display,
            max(CASE WHEN check_agronomic AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cagrf,
            max(CASE WHEN check_competitor AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as ccomp,
            max(CASE WHEN check_flag AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cpifl,
            max(CASE WHEN check_line_to_beat_f AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as fp_ltb,
            max(CASE WHEN check_line_to_beat_m AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as mp_ltb,
            max(CASE WHEN check_maturity AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cmatf,
            max(CASE WHEN check_performance AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cperf,
            max(CASE WHEN check_regional AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cregf,
            max(CASE WHEN check_registration AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as crtnf
        from (
            select distinct 
                ex.code          as experiment_id,
                m.be_bid 					as be_bid,
                coalesce(td.check_agronomic, p.check_agronomic, false) as check_agronomic,
                coalesce(td.check_competitor, p.check_competitor, false) as check_competitor,
                coalesce(td.check_flag, p.check_flag, false) as check_flag,
                coalesce(td.check_line_to_beat_f, p.check_line_to_beat_f, false) as check_line_to_beat_f,
                coalesce(td.check_line_to_beat_m, p.check_line_to_beat_m, false) as check_line_to_beat_m,
                coalesce(td.check_maturity, p.check_maturity, false) as check_maturity,
                coalesce(td.check_performance, p.check_performance, false) as check_performance,
                coalesce(td.check_regional, p.check_regional, false) as check_regional,
                coalesce(td.check_registration, p.check_registration, false) as check_registration
            from rv_experiments_loc360 ex
            join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
            join rv_mapped_plot_stats_loc360 p on p.trial_uuid = t.object_uuid
            join rv_bb_material_daas m on lower(m.material_guid) = p.material_batch_uuid
            left outer join(
                select distinct 
                    experiment_id,
                    be_bid,
                    coalesce(cpi, 0) = 1 as check_flag,
                    coalesce(check_agronomic, 0) = 1 as check_agronomic,
                    coalesce(check_maturity, 0) = 1 as check_maturity,
                    coalesce(check_performance, 0) = 1 as check_performance,
                    coalesce(check_regional, 0) = 1 as check_regional,
                    coalesce(check_registration, 0) = 1 as check_registration,
                    coalesce(check_line_to_beat_f, 0) = 1 as check_line_to_beat_f,
                    coalesce(check_line_to_beat_m, 0) = 1 as check_line_to_beat_m,
                    coalesce(check_competitor, 0) = 1 as check_competitor
                from rv_bb_experiment_trial_entry_daas) td
            on td.experiment_id = ex.code and td.be_bid = m.be_bid
            ) mbu
        inner join (
            select ap_data_sector_name,
                cast(analysis_year as integer) as analysis_year,
                max(cast(substring(experiment_id, 0, 2) as integer)) OVER (partition by decision_group) as max_exp_year,
                cast(substring(experiment_id, 0, 2) as integer) as exp_year,
                experiment_id,
                decision_group,
                untested_entry_display
            from rv_ap_sector_experiment_config
            where decision_group is not null
            and ap_data_sector_name = 'CORN_NA_SUMMER'
            AND CAST(analysis_year as integer) IN (2023)
        ) ec 
        on ec.experiment_id = mbu.experiment_id 
        group by ec.ap_data_sector_name,
        ec.analysis_year,
        ec.decision_group,
        mbu.be_bid
    ) "checks"
    LEFT JOIN rv_corn_material_tester_adapt "cmt"
      ON "checks".be_bid = "cmt".be_bid
        AND "checks".ap_data_sector LIKE 'CORN%'
    LEFT JOIN (
        SELECT *
          FROM rv_be_bid_ancestry
        WHERE receiver_p <> donor_p
        AND (receiver_p IS NOT NULL OR donor_p IS NOT NULL)
    )"bbal"
    ON "checks".be_bid = "bbal".be_bid
        """
    pd_mock.assert_called_with(
        query=sql, connection=dbdriver_mock.connect.return_value, schema_overrides=None
    )


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
@patch("libs.dme_sql_queries.query_pvs_trait_list", Mock(out="(YGSMN)"))
def test_query_trial_input(
    pvs_list_mock,
    param_helper_mock,
    dbdriver_mock,
    pl_mock,
):
    query_trial_input(
        "CORN_INDONESIA_ALL",
        2023,
        "phenohybrid1yr",
        "('sourceid1')",
        "geo_segment",
        "numeric",
    )
    sql = """WITH tpad_base AS (SELECT DISTINCT
            asec.ap_data_sector,
            asec.analysis_year,
            asec.analysis_type,
            asec.decision_group,
            asec.decision_group_rm,
            asec.stage,
            tpad.experiment_id,
            tpad.trial_id,
            tpad.loc_selector,
            tpad.plot_barcode,
            tpad.be_bid,
            tpad.trait_measure_code AS trait,
            tpad.result_alpha_value,
            astc.distribution_type,
            astc.direction,
            astc.yield_trait,
            astc.metric_name,
            astc.dme_chkfl,
            astc.dme_reg_x,
            astc.dme_reg_y,
            astc.dme_rm_est,
            astc.in_pvs,
            astc.dme_weighted_trait
        FROM rv_ap_data_sector_config adsc 
        INNER JOIN rv_trial_pheno_analytic_dataset tpad
          ON LOWER(adsc.spirit_crop_name) = LOWER(tpad.crop)
          AND adsc.spirit_country_code_list LIKE CONCAT('%', tpad.country_code, '%')
        INNER JOIN ( 
    select DISTINCT
        ap_data_sector,
        analysis_year,
        CASE WHEN COUNT(experiment_id) OVER (PARTITION BY decision_group) = 1
            THEN 'SingleExp'
            WHEN MIN(year) OVER (PARTITION BY decision_group) = MAX(year) OVER (PARTITION BY decision_group)
            THEN 'MynoET'
            ELSE 'MultiExp'
        END as analysis_type,
        decision_group,
        experiment_id,
        MAX(CASE WHEN year = max_year then stage else null end) OVER (PARTITION BY decision_group) as stage,
        median(CASE when year = max_year then decision_group_rm else null end) OVER (PARTITION BY decision_group) as decision_group_rm
    from (
        select
            asec.ap_data_sector_name as ap_data_sector,
            CAST(MEDIAN( asec.analysis_year) as INTEGER) as analysis_year,
            asec.decision_group,
            asec.experiment_id,
            round(coalesce(MAX(try_to_number(t.stage)), MAX(asec.stage)),2) as stage,
            round(coalesce(median(asec.decision_group_rm), MEDIAN(try_to_number(t.maturity_group)), 0),3) as decision_group_rm,
            MEDIAN(t.year) as year,
            MAX(MAX(t.year)) OVER (PARTITION BY asec.decision_group) as max_year from 
        rv_ap_sector_experiment_config asec
        join rv_experiments_loc360 ex on asec.experiment_id = ex.code
        join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
        where not ex.invalidated and not t.invalidated AND not t.exclude_from_analysis
            and ap_data_sector_name = 'CORN_INDONESIA_ALL'
                AND CAST(analysis_year AS integer) IN (2023)
                AND decision_group in ('sourceid1')
        group by all
    )
     ) asec
        on asec.experiment_id = tpad.experiment_id
        INNER JOIN ( 
     SELECT
            ap_data_sector,
            analysis_year,
            trait,
            CASE 
                WHEN distribution_type ='norm'
                    THEN 'normal'
                WHEN distribution_type = 'rating' AND metric_name = 'h2h'
                    THEN 'normal'
                ELSE distribution_type 
            END AS distribution_type,
            direction,
            conv_operator,
            conv_factor,
            yield_trait,
            CASE
                WHEN metric_name IN ('agronomic', 'disease')
                    THEN 'risk'
                WHEN metric_name = 'yield' 
                    THEN 'performance'
                ELSE metric_name
            END AS metric_name,
            dme_chkfl,
            dme_reg_x,
            dme_reg_y,
            dme_rm_est,
            in_pvs,
            dme_weighted_trait
        FROM (
            SELECT 
                ap_data_sector_name AS ap_data_sector,
                CAST(analysis_year AS integer) AS analysis_year,
                trait,
                TRIM(LOWER(distribution_type)) AS distribution_type,
                TRIM(LOWER(direction)) AS direction,
                conv_operator,
                conv_factor,
                CASE WHEN yield_trait = '1' AND TRIM(LOWER(dme_metric)) IN ('performance', 'yield') THEN TRUE ELSE FALSE END AS yield_trait,
                TRIM(LOWER(dme_metric)) AS metric_name,
                dme_chkfl,
                dme_reg_x,
                dme_reg_y,
                dme_rm_est,
                dme_weighted_trait,
                CASE WHEN trait IN ('YGSMN') THEN true ELSE false END AS in_pvs,
                ROW_NUMBER() OVER (PARTITION BY analysis_year, trait ORDER BY distribution_type, direction) as row_number FROM rv_ap_sector_trait_config
            WHERE ap_data_sector_name = 'CORN_INDONESIA_ALL'
                AND CAST(analysis_year AS integer) IN (2023)
                AND TRIM(LOWER(distribution_type)) IN ('normal', 'norm', 'rating', 'zinb', 'text')
                AND TRIM(LOWER(direction)) IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                AND LOWER(level) = 'plot'
                AND (dme_metric != 'na' OR dme_metric is not null or dme_metric != '' OR dme_reg_x = true OR dme_reg_y = true OR dme_rm_est != 0 OR dme_weighted_trait != 0)
                AND (trait NOT IN ('YGSMN') OR yield_trait = '1')
                
        ) astc
        WHERE row_number = 1
             ) astc
        on astc.trait = tpad.trait_measure_code
        WHERE adsc.ap_data_sector_name = 'CORN_INDONESIA_ALL'
            AND NOT COALESCE(CAST(tpad.tr_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.psp_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.pr_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.outlier AS boolean), FALSE)
            AND tpad.result_alpha_value IS NOT NULL)
        SELECT tpad_base.*, fet.breakout_level, fet.breakout_level_value FROM tpad_base
        INNER JOIN (SELECT DISTINCT
            source_id,
            LOWER(feature_name) AS breakout_level,
            LOWER(REGEXP_REPLACE(TRANSLATE(TRIM(TRIM(value), '.'),'-','_'), '[ :;*+/&\_,]+', '_')) AS breakout_level_value
        FROM rv_feature_export
        WHERE LOWER(feature_name) IN ('geo_segment')
            AND market_name = 'CORN_INDONESIA_ALL') fet
        ON tpad_base.trial_id = fet.source_id
        UNION All
        SELECT tpad_base.*, fel.breakout_level, fel.breakout_level_value FROM tpad_base
        INNER JOIN (SELECT DISTINCT
            source_id,
            LOWER(feature_name) AS breakout_level,
            LOWER(REGEXP_REPLACE(TRANSLATE(TRIM(TRIM(value), '.'),'-','_'), '[ :;*+/&\_,]+', '_')) AS breakout_level_value
        FROM rv_feature_export
        WHERE LOWER(feature_name) IN ('geo_segment')
            AND market_name = 'CORN_INDONESIA_ALL') fel
        ON tpad_base.loc_selector = fel.source_id
        UNION ALL
        SELECT *, 'na' AS breakout_level, 'all' AS breakout_level_value
        FROM tpad_base"""

    pl_mock.assert_called_with(
        query=sql,
        connection=dbdriver_mock.connect.return_value,
        schema_overrides=None,
        do_lower=True,
    )


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_get_data_sector_config(
    param_helper_mock, dbdriver_mock, pd_mock, log1_mock, log2_mock
):
    get_data_sector_config("CORN_NA_SUMMER")
    sql = """
            select 
                ap_data_sector_name,
                spirit_crop_guid
            from rv_ap_data_sector_config
                where ap_data_sector_name = 'CORN_NA_SUMMER' """
    pd_mock.assert_called_with(sql, dbdriver_mock.connect.return_value)


@pytest.fixture()
def test_create_asec_str():
    gen_str = create_asec_str(
        "CORN_NA_SUMMER", 2023, "phenohybrid1yr", "('sourceid1')", False
    )

    bench_str = """
    select DISTINCT
        ap_data_sector,
        analysis_year,
        CASE WHEN COUNT(experiment_id) OVER (PARTITION BY decision_group) = 1
            THEN 'SingleExp'
            WHEN MIN(year) OVER (PARTITION BY decision_group) = MAX(year) OVER (PARTITION BY decision_group)
            THEN 'MynoET'
            ELSE 'MultiExp'
        END as analysis_type,
        decision_group,
        
        MAX(CASE WHEN year = max_year then stage else null end) OVER (PARTITION BY decision_group) as stage,
        median(CASE when year = max_year then decision_group_rm else null end) OVER (PARTITION BY decision_group) as decision_group_rm
    from (
        select
            asec.ap_data_sector_name as ap_data_sector,
            CAST(MEDIAN( asec.analysis_year) as INTEGER) as analysis_year,
            asec.decision_group,
            asec.experiment_id,
            round(coalesce(MAX(try_to_number(t.stage)), MAX(asec.stage)),2) as stage,
            round(coalesce(median(asec.decision_group_rm), MEDIAN(try_to_number(t.maturity_group)), 0),3) as decision_group_rm,
            MEDIAN(t.year) as year,
            MAX(MAX(t.year)) OVER (PARTITION BY asec.decision_group) as max_year from 
        rv_ap_sector_experiment_config asec
        join rv_experiments_loc360 ex on asec.experiment_id = ex.code
        join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
        where not ex.invalidated and not t.invalidated AND not t.exclude_from_analysis
            and ap_data_sector_name = 'CORN_NA_SUMMER'
                AND CAST(analysis_year AS integer) IN (2023)
                AND decision_group in ('sourceid1')
        group by all
    )
    """

    assert gen_str == bench_str


@pytest.fixture()
def test_create_astc_str():
    gen_str = create_astc_str("CORN_NA_SUMMER", 2023, "('sourceid1')", False)

    bench_str = """
     SELECT
            ap_data_sector,
            analysis_year,
            trait,
            CASE 
                WHEN distribution_type ='norm'
                    THEN 'normal'
                WHEN distribution_type = 'rating' AND metric_name = 'h2h'
                    THEN 'normal'
                ELSE distribution_type 
            END AS distribution_type,
            direction,
            conv_operator,
            conv_factor,
            yield_trait,
            CASE
                WHEN metric_name IN ('agronomic', 'disease')
                    THEN 'risk'
                WHEN metric_name = 'yield' 
                    THEN 'performance'
                ELSE metric_name
            END AS metric_name,
            dme_chkfl,
            dme_reg_x,
            dme_reg_y,
            dme_rm_est,
            
            dme_weighted_trait
        FROM (
            SELECT 
                ap_data_sector_name AS ap_data_sector,
                CAST(analysis_year AS integer) AS analysis_year,
                trait,
                TRIM(LOWER(distribution_type)) AS distribution_type,
                TRIM(LOWER(direction)) AS direction,
                conv_operator,
                conv_factor,
                CASE WHEN yield_trait = '1' AND TRIM(LOWER(dme_metric)) IN ('performance', 'yield') THEN TRUE ELSE FALSE END AS yield_trait,
                TRIM(LOWER(dme_metric)) AS metric_name,
                dme_chkfl,
                dme_reg_x,
                dme_reg_y,
                dme_rm_est,
                dme_weighted_trait,
                
                ROW_NUMBER() OVER (PARTITION BY analysis_year, trait ORDER BY distribution_type, direction) as row_number FROM rv_ap_sector_trait_config
            WHERE ap_data_sector_name = 'CORN_NA_SUMMER'
                AND CAST(analysis_year AS integer) IN (2023)
                AND TRIM(LOWER(distribution_type)) IN ('normal', 'norm', 'rating', 'zinb', 'text')
                AND TRIM(LOWER(direction)) IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                AND LOWER(level) = 'plot'
                AND (dme_metric != 'na' OR dme_metric is not null or dme_metric != '' OR dme_reg_x = true OR dme_reg_y = true OR dme_rm_est != 0 OR dme_weighted_trait != 0)
                
        ) astc
        WHERE row_number = 1
            """

    assert gen_str == bench_str


@pytest.fixture()
def test_create_fe_str():
    gen_str = create_fe_str("CORN_NA_SUMMER", "wce", False)

    bench_str = """SELECT DISTINCT
            
            LOWER(feature_name) AS breakout_level,
            LOWER(REGEXP_REPLACE(TRANSLATE(TRIM(TRIM(value), '.'),'-','_'), '[ :;*+/&\\_,]+', '_')) AS breakout_level_value
        FROM rv_feature_export
        WHERE LOWER(feature_name) IN ('wce')
            AND market_name = 'CORN_NA_SUMMER'"""

    assert gen_str == bench_str


@pytest.fixture()
@pytest.mark.parametrize(
    "gen_str,bench_str",
    [
        ("wce", "('wce')"),
        (None, "('')"),
        ("", "('')"),
        ("null", "('')"),
        ("''", "('')"),
    ],
)
def test_get_breakout_str(gen_str, bench_str):
    assert get_breakout_str(gen_str) == bench_str


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_query_pvs_trait_list(
    param_helper_mock,
    dbdriver_mock,
    pd_mock,
):
    query_pvs_trait_list("CORN_NA_SUMMER", 2022, "('source_id1')")
    sql = """SELECT DISTINCT trait from rv_ap_all_pvs WHERE ap_data_sector = 'CORN_NA_SUMMER'
                AND CAST(source_year AS integer) IN (2022) AND source_id IN ('source_id1')"""
    pd_mock.assert_called_with(sql, dbdriver_mock.connect.return_value)


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pl.read_database")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_query_pvs_material_types(
    param_helper_mock,
    dbdriver_mock,
    pd_mock,
):
    query_pvs_material_types(
        "CORN_NA_SUMMER", 2022, "genomic_prediction", "('source_id1')", False
    )
    sql = """SELECT DISTINCT 
                  source_id as decision_group, 
                  entry_identifier as be_bid, 
                  material_type
                from rv_ap_all_pvs WHERE ap_data_sector = 'CORN_NA_SUMMER'
                AND CAST(source_year AS integer) IN (2022)
                AND analysis_type IN ('GenoPred')
                AND source_id IN ('source_id1')
                AND LOWER(material_type) not like '%entry'"""
    pd_mock.assert_called_with(sql, dbdriver_mock.connect.return_value)
