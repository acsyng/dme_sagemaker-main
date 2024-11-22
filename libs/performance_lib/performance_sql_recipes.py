from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
import pandas as pd, numpy as np
from libs.performance_lib import performance_helper


def merge_trial_check_entries(ap_data_sector, analysis_year, analysis_type, crop_guid, season=None):
    # Note: this query uses the do_lower capability.
    with SnowflakeConnection() as dc:
        output_df = dc.get_data(
        """
            WITH experiment_config_backpop AS (
                SELECT
                    ap_data_sector_name,
                    analysis_year,
                    experiment_year,
                    experiment_id,
                    decision_group,
                    MAX(display_name) as display_name,
                    MAX(decision_group_rm) as decision_group_rm,
                    MAX(stg) as stage,
                    MAX(combined) as combined,
                    MAX(technology) as technology,
                    MAX(rootexpid) as rootexpid,
                    MAX(adapt_display) as adapt_display,
                    MAX(trial_design) as trial_design,
                    MAX(gse_decision_group_market) as gse_decision_group_market,
                    MAX(is_gse_analysis_required) as is_gse_analysis_required,
                    MAX(untested_entry_display) as untested_entry_display,
                    MIN(season) as season
                FROM (
                        SELECT MIN(year) as analysis_year,
                               NULL as ap_data_sector_id,
                               MIN(ap_data_sector) as ap_data_sector_name,
                               MIN(year) as experiment_year,
                               experiment_id as experiment_id,
                               experiment_id as decision_group,
                               experiment_id as display_name,
                               ROUND((AVG(maturity_group)*5 + 80)*2,0)/2  as decision_group_rm, -- round to nearest 5
                               0 as stg,
                               0 as combined,
                               NULL as technology,
                               NULL as rootexpid,
                               1 as adapt_display,
                               NULL as trial_design,
                               NULL as gse_decision_group_market, 
                               False as is_gse_analysis_required,
                               False as untested_entry_display,
                               UPPER(MIN(season)) as season
                            FROM rv_trial_pheno_analytic_dataset
                        WHERE ap_data_sector = {0}
                            AND year = {1}
                        GROUP BY experiment_id
                    ) all_experiments
                GROUP BY ap_data_sector_name, 
                    analysis_year, 
                    experiment_year,
                    experiment_id,
                    decision_group
            ), query_output AS (
                SELECT
                    asec.ap_data_sector_name AS ap_data_sector,
                    CAST(asec.analysis_year AS integer) AS analysis_year,
                    asec.season as season,
                    {2} AS analysis_type,
                    bb_ete.experiment_id,
                    asec.decision_group,
                    CASE WHEN asec.untested_entry_display THEN 1 ELSE 0 END AS untested_entry_display,
                    bb_ete.be_bid as entry_id,
                    bb_ete.be_bid,
                    SUM(CAST(ROUND(bb_ete.cpi,0) AS integer)) AS cpifl,
                    SUM(COALESCE(CAST(ROUND(bb_ete.check_performance,0) AS integer),
                            CAST(ROUND(bb_ete.cpi,0) AS integer))) AS cperf,
                    SUM(COALESCE(CAST(ROUND(bb_ete.check_agronomic,0) AS integer),
                            CAST(ROUND(bb_ete.cpi,0) AS integer))) AS cagrf,
                    SUM(COALESCE(CAST(ROUND(bb_ete.check_maturity,0) AS integer),
                            CAST(ROUND(bb_ete.cpi,0) AS integer))) AS cmatf,
                    SUM(COALESCE(CAST(ROUND(bb_ete.check_regional,0) AS integer),
                            CAST(ROUND(bb_ete.cpi,0) AS integer))) AS cregf,
                    SUM(COALESCE(CAST(ROUND(bb_ete.check_registration,0) AS integer),
                            CAST(ROUND(bb_ete.cpi,0) AS integer))) AS crtnf,
                    SUM(CAST(ROUND(bb_ete.check_line_to_beat_f,0) AS integer)) AS fp_ltb,
                    SUM(CAST(ROUND(bb_ete.check_line_to_beat_m,0) AS integer)) AS mp_ltb,
                    COUNT(bb_ete.trial_id) AS n
                FROM RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS as bb_ete
                INNER JOIN experiment_config_backpop as asec
                    ON (
                        ({2} = 'SingleExp' AND asec.combined = 0)
                         OR ({2} = 'MultiExp' AND asec.combined = 1)
                         OR ({2} = 'MynoET' AND asec.combined = 2)
                         OR ({2} = 'GenoPred' AND 
                            (asec.is_gse_analysis_required OR ({0} IN ('CORN_BRAZIL_SUMMER', 'CORN_LAS_SUMMER', 'CORN_NA_SUMMER', 'CORN_EAME_SUMMER', 'CORNGRAIN_APAC_1') AND asec.stage< 4.2)))
                             --  AND asec.stage< 4.2))) 
                        )
                        AND bb_ete.experiment_id = asec.experiment_id
                WHERE bb_ete.crop_guid = {3}
                GROUP BY
                    asec.ap_data_sector_name,
                    asec.analysis_year,
                    asec.season,
                    bb_ete.experiment_id,
                    asec.decision_group,
                    asec.untested_entry_display,
                    bb_ete.be_bid
            )
            SELECT DISTINCT
                rvt.ap_data_sector AS ap_data_sector,
                rvt.analysis_year AS analysis_year,
                rvt.season as season,
                rvt.analysis_type AS analysis_type,
                rvt.source_id AS source_id,
                rvt.untested_entry_display AS untested_entry_display,
                rvt.entry_id AS entry_id,
                CAST(GREATEST(rvt.cpifl,
                     rvt.cperf, 
                     rvt.cagrf, 
                     rvt.cmatf,
                     rvt.cregf,
                     rvt.crtnf) AS integer) AS cpifl,
                rvt.cperf AS cperf,
                rvt.cagrf AS cagrf,
                rvt.cmatf AS cmatf,
                rvt.cregf AS cregf,
                rvt.crtnf AS crtnf,
                rvt.n AS result_count,
                'entry' AS material_type
            FROM (
                SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    query_output.analysis_type AS analysis_type,
                    query_output.season as season,
                    query_output.decision_group AS source_id,
                    query_output.untested_entry_display,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.fp_ltb) AS fp_ltb,
                    SUM(query_output.mp_ltb) AS mp_ltb,
                    SUM(query_output.n) AS n
                FROM query_output
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.analysis_type,
                    query_output.season,
                    query_output.decision_group,
                    query_output.untested_entry_display,
                    query_output.entry_id,
                    query_output.be_bid
                UNION ALL
                SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    query_output.analysis_type AS analysis_type,
                    query_output.season as season,
                    query_output.experiment_id AS source_id,
                    query_output.untested_entry_display,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.fp_ltb) AS fp_ltb,
                    SUM(query_output.mp_ltb) AS mp_ltb,
                    SUM(query_output.n) AS n
               FROM query_output
               WHERE {2} = 'SingleExp'
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.analysis_type,
                    query_output.season,
                    query_output.experiment_id,
                    query_output.untested_entry_display,
                    query_output.entry_id,
                    query_output.be_bid
            ) rvt
        """.format("'" + ap_data_sector + "'",int(analysis_year),"'" + analysis_type + "'", "'" + crop_guid + "'"),
            do_lower=True
        )

    if season is not None:
        output_df = output_df[output_df['season'] == season]

    output_df = output_df.drop(columns=['season'])

    return output_df


def get_source_ids_decision_groups(ap_data_sector, analysis_year):
    query_str = """
    SELECT DISTINCT experiment_id  as "source_id",
        decision_group AS "decision_group"
    FROM rv_ap_sector_experiment_config
    WHERE ap_data_sector_name = {0}
        AND analysis_year = {1}
        --AND experiment_id!=decision_group
    """.format("'" + ap_data_sector + "'", int(analysis_year))

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_source_id_loc_map(source_ids):
    source_id_str = performance_helper.make_search_str(source_ids)

    query_str = """
        SELECT DISTINCT experiment_id as "source_id",
            trial_id as "trial_id",
            loc_selector as "loc_selector"
        FROM rv_trial_pheno_analytic_dataset
        WHERE experiment_id in {0}
    """.format(source_id_str)

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_breakout_data_batch(search_ids, breakout_level, breakout_source, step=500):
    # get unique list of source ids
    search_ids = pd.unique(search_ids)

    # split search_ids into batches to avoid overflow issues
    df_breakout_list = []
    with SnowflakeConnection() as dc:
        for i in range(0, search_ids.shape[0], step):
            if i + step > search_ids.shape[0]:
                df_breakout_list.append(get_breakout_data(search_ids[i:], breakout_level, breakout_source,dc))
            else:
                df_breakout_list.append(get_breakout_data(search_ids[i:i + step], breakout_level, breakout_source,dc))

    return pd.concat(df_breakout_list, axis=0)


def get_breakout_data(search_ids, breakout_level, breakout_source, denodo_conn):
    if breakout_level == 'ET':
        breakout_level = 'EnvironmentType_level1'

    search_ids_str = performance_helper.make_search_str(search_ids)

    if breakout_source == "rv_feature_export":
        breakout_level = breakout_level.lower()
        query_str = """
            SELECT source_id as "source_id", value as "value"
            FROM rv_feature_export
            WHERE source_id in {0}
                AND LOWER(feature_name) = {1}

        """.format(search_ids_str, "'" + breakout_level + "'")
    elif breakout_source == "rv_ap_sector_experiment_config":
        query_str = """
            SELECT experiment_id as "source_id",
                {1} as "value"
            FROM rv_ap_sector_experiment_config
            WHERE  experiment_id in {0}
        """.format(search_ids_str, "\"" + breakout_level + "\"")
    else:
        print("unknown breakout source")
        return pd.DataFrame()

    output_df = denodo_conn.get_data(query_str)
    return output_df


def get_decision_groups(ap_data_sector,
                        analysis_year,
                        season=None):
    query_str = """
        SELECT rv_exp.experiment_id as "experiment_id",
            rv_exp.decision_group as "decision_group"
        FROM rv_ap_sector_experiment_config rv_exp
        LEFT JOIN (
            select distinct experiment_id ,season
            from rv_trial_pheno_analytic_dataset 
            where ap_data_sector = {0}
        ) rvt
            on rvt.experiment_id = rv_exp.experiment_id
        WHERE CAST(CONCAT('20',SUBSTRING(rv_exp.experiment_id,0,2)) as integer)={1}
            AND rv_exp.ap_data_sector_name={0}
            AND rv_exp.experiment_id!=rv_exp.decision_group
    """.format("'" + ap_data_sector + "'", int(analysis_year))

    if season is not None:
        query_str = query_str + """
            AND upper(rvt.season) = {}
        """.format("'" + season + "'")

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_plot_result_data(be_bid_arr, crop_name, analysis_year):
    # crop names = 'soybean', 'corn'
    step = 10000
    with SnowflakeConnection() as dc:
        trait_data_list = []
        # get all material ids
        for i in range(0,be_bid_arr.shape[0],step):
            if i+step < be_bid_arr.shape[0]:
                be_bid_str = performance_helper.make_search_str(be_bid_arr[i:i+step])
            else:
                be_bid_str = performance_helper.make_search_str(be_bid_arr[i:])

            query_str = """
                SELECT MAX(rv_pl.ALPHA_VALUE) AS "alpha_value",
                    AVG(rv_pl.NUMBER_VALUE) AS "number_value",
                    rv_tr.CODE AS "trait",
                    rv_mat.BE_BID AS "be_bid" ,
                    {1} AS "year"
                FROM RV_PLOT_RESULT_SP  rv_pl
                INNER JOIN RV_TRAIT_SP rv_tr
                    ON rv_tr.TRAIT_GUID = rv_pl.TRAIT_GUID 
                INNER JOIN RV_PLOT_SUBPLOT_TRIAL_ENTRY rv_sb
                    ON rv_sb.PLOT_SUBPLOT_GUID = rv_pl.PLOT_SUBPLOT_GUID 
                INNER JOIN RV_BB_MATERIAL_DAAS rv_mat
                    ON rv_mat.material_id = rv_sb.MATERIAL_ID 
                WHERE UPPER(rv_tr.CODE) IN ('DIC_T','E3_T','LL55_T','RR2_T','DPM_T',
                                                'MI__T','CLS_T','BP_T','BSR_T','CN3_T',
                                                'E1_T','FELS_T','FL_CT','HILCT','MET_T',
                                                'PB_CT','PD_CT','RPS_T','STMTT','STS_T','HARVT',
                                                'PLTQT','NOTET')
                    AND rv_pl.EXCLUDE_FROM_ANALYSIS = 0
                    AND CAST(CONCAT('20',SUBSTRING(rv_sb.trial_id,0,2)) as integer) <= {1}
                    AND rv_mat.be_bid in {0}
                GROUP BY rv_tr.CODE , rv_mat.BE_BID 
            """.format(be_bid_str, int(analysis_year))
            trait_data_list.append(dc.get_data(query_str))

    # join trait_data with bebids, output
    output_df = pd.concat(trait_data_list,axis=0)
    output_df = output_df.groupby(by=['trait','be_bid','year']).first().reset_index().rename(columns={'be_bid':'entry_id'})

    return output_df


# gets parental information (donor and receiver) from manged.rv_be_bid_ancestry_laas in batches
# input array of be_bids, outputs dataframe with be_bid, donor_p, receiver_p
def get_ancestors(entry_ids, is_corn=1):
    batch_size = 1000
    df_par_list = []

    with SnowflakeConnection() as dc:
        for i in range(0, entry_ids.shape[0], batch_size):
            if i + batch_size < entry_ids.shape[0]:
                entry_ids_small = entry_ids[i:i + batch_size]
            else:
                entry_ids_small = entry_ids[i:]

            id_sql_str = ''
            for entry_id in entry_ids_small:
                id_sql_str += "'" + entry_id + "',"
            id_sql_str = id_sql_str[:-1]  # remove final comma

            if is_corn == 1:
                query_str = """
                        SELECT distinct be_bid as "entry_id",
                            pool1_be_bid AS "pool1_be_bid",
                            pool2_be_bid AS "pool2_be_bid"
                        FROM RV_CORN_MATERIAL_TESTER_ADAPT  ---managed.rv_corn_material_tester_adapt
                        where be_bid in ({})
                    """.format(id_sql_str)
            else:
                query_str = """
                        SELECT distinct be_bid as "entry_id",
                            donor_p as "pool1_be_bid",
                            receiver_p as "pool2_be_bid"
                        FROM RV_BE_BID_ANCESTRY 
                        where be_bid in ({})
                    """.format(id_sql_str)
            df_par_temp = dc.get_data(query_str)
            df_par_list.append(df_par_temp)

    df_par = pd.concat(df_par_list, axis=0)
    return df_par


def merge_pvs_input(ap_data_sector, analysis_year, df_dgs=pd.DataFrame(), get_parents=0):
    if df_dgs.shape[0] > 0:
        if get_parents == 0:
            analysis_type_str = "(\'SingleExp\', \'MultiExp\')"
        else:
            if ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                analysis_type_str = "(\'SingleExp\', \'MultiExp\', \'PhenoGCA\')"
            else:
                analysis_type_str = "(\'SingleExp\', \'MultiExp\', \'GenoPred\')"
    else:
        if get_parents == 0:
            analysis_type_str = "(\'MultiExp\')"
        else:
            if ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                analysis_type_str = "(\'SingleExp\', \'MultiExp\', \'PhenoGCA\')"
            else:
                analysis_type_str = "(\'SingleExp\', \'MultiExp\', \'GenoPred\')"

    query_str_pre_where = """
    -- select the single year PVS data with the appropriate source_id and apply config tables
    WITH pvs_single_year AS (
        SELECT 
        rv_ap_all_pvs.ap_data_sector,
        rv_ap_all_pvs.analysis_type,
        rv_ap_all_pvs.source_id,
        CASE 
            WHEN rv_ap_all_pvs.ap_data_sector = 'SOY_NA_SUMMER' AND rv_ap_all_pvs.analysis_type = 'MultiYear'
                THEN 'all'
            WHEN rv_ap_all_pvs.market_seg IS NULL OR rv_ap_all_pvs.market_seg = ''
                THEN 'all'
            ELSE LOWER(rv_ap_all_pvs.market_seg)
        END AS market_seg,
        rv_ap_all_pvs.trait,
        rv_ap_all_pvs.model,
        rv_ap_all_pvs.entry_identifier,
        CASE 
            WHEN LOWER(rv_ap_all_pvs.material_type) = 'female' 
                THEN 'pool1'
            WHEN LOWER(rv_ap_all_pvs.material_type) = 'male' 
                THEN 'pool2'
            WHEN LOWER(rv_ap_all_pvs.material_type) = 'hybrid'
                THEN 'entry'
            ELSE LOWER(rv_ap_all_pvs.material_type)
        END AS material_type,
        rv_ap_all_pvs.count,
        rv_ap_all_pvs.prediction,
        CASE
            WHEN rv_ap_sector_trait_config.dme_chkfl = 'cperf'
                THEN rv_ap_all_pvs.cperf_per
            WHEN rv_ap_sector_trait_config.dme_chkfl = 'cmatf'
                THEN rv_ap_all_pvs.cmatf_per
            WHEN rv_ap_sector_trait_config.dme_chkfl = 'cagrf'
                THEN rv_ap_all_pvs.cagrf_per
            WHEN rv_ap_sector_trait_config.dme_chkfl = 'crtnf'
                THEN rv_ap_all_pvs.crtnf_per
            ELSE rv_ap_all_pvs.cperf_per
        END as prediction_perc,
        rv_ap_all_pvs.stderr,
        dg_rm_list.decision_group_rm,
        dg_rm_list.stage,
        CASE 
            WHEN rv_ap_all_pvs.ap_data_sector = 'SOY_NA_SUMMER' AND rv_ap_all_pvs.analysis_type = 'MultiYear'
                THEN LOWER(rv_ap_all_pvs.market_seg)
            WHEN dg_rm_list.technology IS NULL OR dg_rm_list.technology = ''
                THEN 'all'
            ELSE LOWER(dg_rm_list.technology)
        END AS technology,
        rv_ap_sector_trait_config.conv_operator,
        rv_ap_sector_trait_config.conv_factor,
        rv_ap_sector_trait_config.dme_metric,
        rv_ap_sector_trait_config.dme_chkfl,
        rv_ap_sector_trait_config.dme_reg_x,
        rv_ap_sector_trait_config.dme_reg_y,
        rv_ap_sector_trait_config.dme_rm_est,
        rv_ap_sector_trait_config.dme_weighted_trait
      FROM (
        SELECT
            ap_data_sector,
            trialing_year as source_year,
            analysis_type,
            source_id,
            MAX(pipeline_runid) AS pipeline_runid,
            LOWER(market_seg) AS market_seg,
            trait,
            model,
            entry_identifier,
            material_type,
            MAX(count) AS count,
            MAX(prediction) AS prediction,
            MAX(stderr) AS stderr,
            COALESCE(MAX(cperf_per), MAX(cpifl_per)) as cperf_per,
            COALESCE(MAX(cmatf_per), MAX(cpifl_per)) as cmatf_per,
            COALESCE(MAX(cagrf_per), MAX(cpifl_per)) as cagrf_per,
            COALESCE(MAX(crtnf_per), MAX(cpifl_per)) as crtnf_per
          FROM rv_ap_all_pvs 
      """

    if df_dgs.shape[0] != 0:
        dg_str = performance_helper.make_search_str(df_dgs['decision_group_name'].values)
        print(dg_str)
        query_str_where = """
        WHERE ap_data_sector = {0}
          --AND TRY_TO_NUMBER(trialing_year) = {1}
          AND analysis_type IN {3}
          AND loc = 'ALL'
          AND material_type != 'untested_entry'
          AND upper(source_id) in {2}
        """
    elif "EAME_SUMMER" in ap_data_sector:
        dg_str = ""
        query_str_where = """
            WHERE ap_data_sector = {0}
              AND TRY_TO_NUMBER(trialing_year) = {1}
              AND analysis_type in {3}
              AND loc = 'ALL'
              AND material_type != 'untested_entry'
       """
    else: # if no decision groups, don't do anything....
        print("no decision groups...")
        dg_str = ""
        query_str_where = """
            WHERE ap_data_sector = {0}
              AND TRY_TO_NUMBER(trialing_year) = {1}
              AND analysis_type = 'MultiExp'
              AND loc = 'ALL'
              AND material_type != 'untested_entry'
              AND source_id in ('')
       """

    # if not CORN_NA_SUMMER include market segment filter.
    # corn na data before 2022 is weird....
    if not ap_data_sector in [
        'CORN_NA_SUMMER',
        'CORN_CHINA_SPRING',
        'CORN_CHINA_SUMMER',
        'CORN_BRAZIL_SUMMER',
        'CORN_BRAZIL_SAFRINHA'
    ]:
        query_str_where = query_str_where + """
            AND lower(market_seg) = 'all'
        """

    query_str_post_where = """
        GROUP BY
            ap_data_sector,
            trialing_year,
            analysis_type,
            source_id,
            LOWER(market_seg),
            trait,
            model,
            entry_identifier,
            material_type
        ORDER BY pipeline_runid DESC
    ) rv_ap_all_pvs
    LEFT JOIN (
        SELECT
        ap_data_sector_name,
        analysis_year,
        UPPER(decision_group) as decision_group,
        AVG(CASE 
                WHEN IS_REAL(decision_group_rm) OR decision_group_rm IS NULL OR decision_group_rm = ''
                    THEN 0
                ELSE decision_group_rm
        END) AS decision_group_rm,
        MAX(stage) AS stage,
        MAX(technology) AS technology
      FROM rv_ap_sector_experiment_config
    WHERE ap_data_sector_name = {0}
    GROUP BY
        ap_data_sector_name,
        analysis_year,
        decision_group
    ) dg_rm_list
      ON UPPER(rv_ap_all_pvs.source_id) = dg_rm_list.decision_group
    INNER JOIN ( -- make a dummy row containing ymh_residual so that we grab that trait
        SELECT DISTINCT
            ap_data_sector_name,
            analysis_year,
            trait,
            conv_operator,
           conv_factor,
            dme_metric,
            dme_chkfl,
            dme_reg_x,
            dme_reg_y,
            dme_rm_est,
            dme_weighted_trait
        from rv_ap_sector_trait_config
        where ap_data_sector_name = {0}
            and analysis_year = {1}
        UNION 
        SELECT
            {0} as ap_data_sector_name,
            {1} as analysis_year,
            'ymh_residual'  as trait,
            '/' as conv_operator,
            1 as conv_factor,
            'performance' as dme_metric,
            'cperf' as dme_chkfl,
            False as dme_reg_x,
            False as dme_reg_y,
            0 as dme_rm_est,
            0 as dme_weighted_trait
    ) rv_ap_sector_trait_config
      ON rv_ap_sector_trait_config.ap_data_sector_name = rv_ap_all_pvs.ap_data_sector
        --AND rv_ap_sector_trait_config.analysis_year = TRY_TO_NUMBER(rv_ap_all_pvs.source_year)
        AND TRIM(rv_ap_sector_trait_config.trait) = rv_ap_all_pvs.trait      
    )
    -- find the maturity group associated with the source ID from experiment_config and the entry_id's associated with the source_id, then grab the appropriate multiyear rows
    SELECT
        pvs_single_year.ap_data_sector,
        pvs_single_year.analysis_type AS analysis_type,
        {1} AS analysis_year,
        pvs_single_year.source_id,
        pvs_single_year.stage,
        pvs_single_year.decision_group_rm,
        LOWER(pvs_single_year.market_seg) AS pvs_market_seg,
        'all' AS pvs_loc,
        'all' AS pvs_my_source_id,
        pvs_single_year.technology AS exp_technology,
        pvs_single_year.trait,
        pvs_single_year.entry_identifier,
        pvs_single_year.material_type,
        pvs_single_year.count,
        CASE 
            WHEN pvs_single_year.conv_operator = '*' AND pvs_single_year.material_type LIKE '%entry' AND pvs_single_year.conv_factor != 0 AND pvs_single_year.prediction IS NOT NULL
                THEN pvs_single_year.prediction/pvs_single_year.conv_factor
            WHEN pvs_single_year.conv_operator = '/' AND pvs_single_year.material_type LIKE '%entry' AND pvs_single_year.conv_factor != 0 AND pvs_single_year.prediction IS NOT NULL
                THEN pvs_single_year.prediction*pvs_single_year.conv_factor
            ELSE pvs_single_year.prediction
        END AS prediction,
        pvs_single_year.prediction_perc,
        CASE 
            WHEN pvs_single_year.conv_operator = '*' AND pvs_single_year.material_type LIKE '%entry' AND pvs_single_year.conv_factor != 0 AND pvs_single_year.stderr IS NOT NULL
                THEN pvs_single_year.stderr/pvs_single_year.conv_factor
            WHEN pvs_single_year.conv_operator = '/' AND pvs_single_year.material_type LIKE '%entry' AND pvs_single_year.conv_factor != 0 AND pvs_single_year.stderr IS NOT NULL
                THEN pvs_single_year.stderr*pvs_single_year.conv_factor
            ELSE pvs_single_year.stderr
        END AS stderr,
        pvs_single_year.dme_metric,
        pvs_single_year.dme_chkfl,
        pvs_single_year.dme_reg_x,
        pvs_single_year.dme_reg_y,
        pvs_single_year.dme_rm_est,
        pvs_single_year.dme_weighted_trait
      FROM pvs_single_year
    """

    query_str = query_str_pre_where + query_str_where + query_str_post_where

    query_str = query_str.format(
        "'" + ap_data_sector + "'",
        int(analysis_year),
        dg_str,
        analysis_type_str
    )

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(
            query_str,
            do_lower=True
        )

    # cast all hybrid material types to entry for consistency
    output_df['material_type'][output_df['material_type'] == 'hybrid'] = 'entry'
    # rename market seg columns
    output_df = output_df.rename(columns={
        'pvs_market_seg': 'market_seg',
        'exp_technology': 'technology'
    })

    print("pvs shape:", output_df.shape[0])
    print(query_str)

    # stage info from decision group table if not in experiment config
    if df_dgs.shape[0] > 0:
        output_df_stg = output_df[output_df['stage'].notna()]
        output_df_no_stg = output_df[output_df['stage'].isna()].drop(columns='stage')
        df_dgs_pre_merge = df_dgs.rename(columns={'decision_group_name': 'source_id'})
        df_dgs_pre_merge = df_dgs_pre_merge[['source_id', 'stage']].drop_duplicates()
        output_df_no_stg = output_df_no_stg.merge(df_dgs_pre_merge, on='source_id', how='left')
        output_df = pd.concat((output_df_stg, output_df_no_stg), axis=0)

    return output_df


def get_corn_material_tester_chunk(entry_df):
    entry_list = "('" + entry_df.str.cat(sep="', '") + "')"

    query_str = """
    SELECT *
    FROM RV_CORN_MATERIAL_TESTER_ADAPT
    WHERE be_bid in {0}
    """.format(entry_list)

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str, do_lower=True)
    return output_df


def get_material_mapper(material_ids, crop_name):
    if crop_name == 'soybean':
        crop_guid = '6C9085C2-C442-48C4-ACA7-427C4760B642'
    if crop_name == 'corn':
        crop_guid = 'B79D32C9-7850-41D0-BE44-894EC95AB285'

    query_str = """
    SELECT DISTINCT abbr_code as "abbr_code", 
        be_bid as "be_bid", 
        highname as "highname",
        line_code as "line_code"
    FROM RV_BB_MATERIAL_DAAS 
    WHERE crop_guid= {0} 
         AND be_bid is NOT NULL
         """.format("'" + crop_guid + "'")

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_trial_pheno_data_reduced_columns(ap_data_sector, analysis_year, season=None):
    query_entry_str = """
        SELECT
            ap_data_sector AS "ap_data_sector",
            CAST(year AS integer) AS "analysis_year",
            entry_id AS "entry_id",
            season as "season",
            MAX(trial_stage) AS "max_stage",
            1 AS "analysis_target"
        FROM rv_trial_pheno_analytic_dataset
        WHERE ap_data_sector = {0}
            AND "YEAR"  = {1}
            AND COALESCE(pr_exclude, False) = False
            AND COALESCE(psp_exclude, False) = False
            AND COALESCE(tr_exclude, False) = False
        GROUP BY
            ap_data_sector,
            season,
            CAST("YEAR"  AS integer),
            entry_id
    """.format("'" + ap_data_sector + "'", int(analysis_year))
    with SnowflakeConnection() as dc:
        entry_df = dc.get_data(query_entry_str)

    if season is not None:
        entry_df = entry_df[entry_df['season'] == season]

    entry_arr = entry_df['entry_id'].drop_duplicates().values

    query_str = """
        WITH feature_export_ms AS (
    -- breakout_level information
            SELECT 
                rv_feature_export.market_name AS ap_data_sector,
                rv_feature_export.source_id AS source_id,
                rv_feature_export.feature_level AS feature_level,
                REPLACE(LOWER(rv_feature_export.value), ' ', '_') AS market_seg
            FROM rv_feature_export
                WHERE rv_feature_export.source = 'SPIRIT'
                AND rv_feature_export.value != 'undefined'
                AND (rv_feature_export.feature_level = 'TRIAL' OR rv_feature_export.feature_level = 'LOCATION')
                AND LOWER(rv_feature_export.feature_name) = 'market_segment'
        )
        SELECT
            trial_pheno_subset.ap_data_sector,
            UPPER(trial_pheno_subset.season) as season,
            trial_pheno_subset.analysis_year AS analysis_year,
            trial_pheno_subset.trial_id,
            trial_pheno_subset.entry_id,
            trial_pheno_subset.year AS year,
            trial_pheno_subset.experiment_id AS source_id,
            COALESCE(feature_export_et.et_value,'undefined') AS et_value,
            COALESCE(feature_export_ms_loc.market_seg, feature_export_ms_trial.market_seg, 'all') AS market_segment,
            trial_pheno_subset.trait AS trait,
            RV_AP_SECTOR_TRAIT_CONFIG.DME_CHKFL AS "dme_chkfl",
            CASE 
                WHEN LOWER(trial_pheno_subset.irrigation) = 'irr'
                    THEN 'IRR'
                ELSE 'DRY'
            END AS irrigation,
            trial_pheno_subset.maturity_group AS maturity_group,
            trial_pheno_subset.result_numeric_value AS result_numeric_value
        FROM (
            SELECT
                rv_trial_pheno_analytic_dataset.ap_data_sector AS ap_data_sector,
                rv_trial_pheno_analytic_dataset.season as season,
                CAST(rv_trial_pheno_analytic_dataset.year AS integer) AS analysis_year,
                CAST(rv_trial_pheno_analytic_dataset.year AS integer) AS year,
                rv_trial_pheno_analytic_dataset.trial_rm AS trial_rm,
                rv_trial_pheno_analytic_dataset.experiment_id AS experiment_id,
                rv_trial_pheno_analytic_dataset.plot_barcode AS plot_barcode,
                rv_trial_pheno_analytic_dataset.entry_id AS entry_id,
                rv_trial_pheno_analytic_dataset.be_bid AS be_bid,
                rv_trial_pheno_analytic_dataset.trait_measure_code AS trait,
                rv_trial_pheno_analytic_dataset.trial_stage AS trial_stage,
                rv_trial_pheno_analytic_dataset.loc_selector AS loc_selector,
                rv_trial_pheno_analytic_dataset.trial_id AS trial_id,
                rv_trial_pheno_analytic_dataset.x_longitude AS x_longitude,
                rv_trial_pheno_analytic_dataset.y_latitude AS y_latitude,
                rv_trial_pheno_analytic_dataset.irrigation AS irrigation,
                rv_trial_pheno_analytic_dataset.maturity_group AS maturity_group,
                rv_trial_pheno_analytic_dataset.result_numeric_value AS result_numeric_value
            FROM (
                SELECT
                    rv_trial_pheno_analytic_dataset.ap_data_sector AS ap_data_sector,
                    rv_trial_pheno_analytic_dataset.season as season,
                    CAST(rv_trial_pheno_analytic_dataset.year AS integer) AS year,
                    AVG(COALESCE(rv_trial_pheno_analytic_dataset.maturity_group,0)) AS trial_rm,
                    rv_trial_pheno_analytic_dataset.experiment_id AS experiment_id,
                    rv_trial_pheno_analytic_dataset.plot_barcode AS plot_barcode,
                    rv_trial_pheno_analytic_dataset.entry_id AS entry_id,
                    rv_trial_pheno_analytic_dataset.be_bid AS be_bid,
                    rv_trial_pheno_analytic_dataset.trait_measure_code AS trait_measure_code,
                    rv_trial_pheno_analytic_dataset.trial_stage AS trial_stage,
                    rv_trial_pheno_analytic_dataset.loc_selector AS loc_selector,
                    rv_trial_pheno_analytic_dataset.trial_id AS trial_id,
                    AVG(rv_trial_pheno_analytic_dataset.x_longitude) AS x_longitude,
                    AVG(rv_trial_pheno_analytic_dataset.y_latitude) AS y_latitude,
                    rv_trial_pheno_analytic_dataset.irrigation AS irrigation,
                    AVG(rv_trial_pheno_analytic_dataset.maturity_group) AS maturity_group,
                    AVG(rv_trial_pheno_analytic_dataset.result_numeric_value) AS result_numeric_value
                  FROM rv_trial_pheno_analytic_dataset
                WHERE rv_trial_pheno_analytic_dataset.result_numeric_value IS NOT NULL
                    AND rv_trial_pheno_analytic_dataset.entry_id IS NOT NULL
                    AND rv_trial_pheno_analytic_dataset.entry_id in {2}
                    AND rv_trial_pheno_analytic_dataset.ap_data_sector = {0}
                    AND rv_trial_pheno_analytic_dataset.year = {1}
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.tr_exclude,False) AS boolean)
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.psp_exclude,False) AS boolean)
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.pr_exclude,False) AS boolean)
                GROUP BY
                    ap_data_sector,
                    season,
                    year,
                    experiment_id,
                    plot_barcode,
                    entry_id,
                    be_bid,
                    trait_measure_code,
                    trial_stage,
                    loc_selector,
                    trial_id,
                    irrigation
            ) rv_trial_pheno_analytic_dataset
        ) trial_pheno_subset
        LEFT JOIN (
            SELECT DISTINCT
              rv_feature_export.source_id AS trial_id,
              rv_feature_export.value AS et_value
            FROM rv_feature_export
            WHERE rv_feature_export.source = 'SPIRIT' AND
                rv_feature_export.value != 'undefined' AND
                (
                    (rv_feature_export.market_name = 'CORN_NA_SUMMER' AND
                         (rv_feature_export.feature_name = 'EnvironmentType_level2' AND
                          rv_feature_export.value LIKE 'ET08%')
                     OR
                        (rv_feature_export.feature_name = 'EnvironmentType_level1' AND
                         rv_feature_export.value != 'ET08')
                    )
                 OR
                    (rv_feature_export.market_name != 'CORN_NA_SUMMER' AND
                     rv_feature_export.feature_name = 'EnvironmentType_level1')
                )
        ) feature_export_et
            ON trial_pheno_subset.trial_id = feature_export_et.trial_id
        LEFT JOIN feature_export_ms feature_export_ms_loc
            ON trial_pheno_subset.loc_selector = feature_export_ms_loc.source_id
                AND trial_pheno_subset.ap_data_sector = feature_export_ms_loc.ap_data_sector
                AND feature_export_ms_loc.feature_level = 'LOCATION'
        LEFT JOIN feature_export_ms feature_export_ms_trial
            ON trial_pheno_subset.trial_id = feature_export_ms_trial.source_id
                AND trial_pheno_subset.ap_data_sector = feature_export_ms_trial.ap_data_sector
                AND feature_export_ms_trial.feature_level = 'TRIAL'
        LEFT JOIN(
            SELECT 
                ap_data_sector AS ap_data_sector,
                plot_barcode AS plot_barcode,
                trait AS trait,
                MAX(CASE WHEN outlier_flag=TRUE THEN 1 ELSE 0 END) AS outlier_flag
              FROM rv_ap_all_yhat
              WHERE ap_data_sector = {0}
                AND source_year = {1}
            GROUP BY
                ap_data_sector, 
                plot_barcode,
                trait
        ) rv_ap_all_yhat
        ON rv_ap_all_yhat.ap_data_sector = trial_pheno_subset.ap_data_sector
            AND rv_ap_all_yhat.plot_barcode = trial_pheno_subset.plot_barcode
            AND rv_ap_all_yhat.trait = trial_pheno_subset.trait
        INNER JOIN RV_AP_SECTOR_TRAIT_CONFIG 
            ON RV_AP_SECTOR_TRAIT_CONFIG.TRAIT = trial_pheno_subset.trait
                AND RV_AP_SECTOR_TRAIT_CONFIG.ANALYSIS_YEAR = trial_pheno_subset.YEAR
	            AND RV_AP_SECTOR_TRAIT_CONFIG.AP_DATA_SECTOR_NAME = trial_pheno_subset.ap_data_sector
        WHERE rv_ap_all_yhat.outlier_flag = 0 or rv_ap_all_yhat.outlier_flag IS NULL
    """

    step = 10000
    output_df_list = []
    with SnowflakeConnection() as dc:
        for i in range(0,entry_arr.shape[0],step):
            if i+step > entry_arr.shape[0]:
                entry_step = entry_arr[i:]
            else:
                entry_step = entry_arr[i:i+step]

            entry_list_as_str = performance_helper.make_search_str(entry_step)
            query_str_use = query_str.format("'" + ap_data_sector + "'", int(analysis_year), entry_list_as_str)
            output_temp = dc.get_data(query_str_use, do_lower=True)
            if output_temp.shape[0] > 0:
                output_df_list.append(output_temp)

    if len(output_df_list) > 0:
        output_df = pd.concat(output_df_list,axis=0)
    else:
        return pd.DataFrame(
            columns=[
                'ap_data_sector',
                'season',
                'analysis_year',
                'trial_id',
                'entry_id',
                'year',
                'source_id',
                'et_value',
                'market_segment',
                'trait',
                'dme_chkfl',
                'irrigation',
                'maturity_group',
                'result_numeric_value'
            ]
        )



    if season is not None:
        output_df = output_df[output_df['season'] == season]

    output_df = output_df.drop(columns=['season'])

    return output_df


def get_variety_entry_data(ap_data_sector, analysis_year, season=None):
    query_str = """
        SELECT DISTINCT
            rv_ap_sector_experiment_config.analysis_year AS "analysis_year",
            rv_ap_sector_experiment_config.ap_data_sector_name AS "ap_data_sector_name",
            rv_ap_sector_experiment_config.decision_group AS "decision_group",
            rv_ap_sector_experiment_config.decision_group_rm AS "decision_group_rm",
            rv_ap_sector_experiment_config.stage AS "stage",
            rv_ap_sector_experiment_config.combined AS "combined",
            rv_ap_sector_experiment_config.technology AS "technology",
            rv_bb_experiment_trial_entry_sdl.entry_id AS "entry_id",
            rv_material.genetic_affiliation_guid AS "genetic_affiliation_guid",
            rv_material.crop_guid AS "crop_guid"
          FROM (
            SELECT 
                crop_guid,
                genetic_affiliation_guid,
                material_id
              FROM RV_MATERIAL 
          ) rv_material
          INNER JOIN RV_AP_DATA_SECTOR_CONFIG 
            ON rv_material.crop_guid = rv_ap_data_sector_config.spirit_crop_guid
          INNER JOIN (
            SELECT DISTINCT
                ap_data_sector,
                experiment_id,
                season,
                material_id,
                entry_id
              FROM rv_trial_pheno_analytic_dataset
              WHERE ap_data_sector = {0}
          ) rv_bb_experiment_trial_entry_sdl
            ON rv_material.material_id = rv_bb_experiment_trial_entry_sdl.material_id
          INNER JOIN rv_ap_sector_experiment_config
            ON rv_bb_experiment_trial_entry_sdl.experiment_id = rv_ap_sector_experiment_config.experiment_id
          WHERE rv_ap_sector_experiment_config.ap_data_sector_name = {0}
            AND rv_ap_sector_experiment_config.analysis_year = {1}
            """.format("'" + ap_data_sector + "'", int(analysis_year))

    if season is not None:
        query_str = query_str + """
                AND upper("rv_bb_experiment_trial_entry_sdl"."season") = {}
        """.format("'" + season + "'")

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_variety_trait_data(ap_data_sector):
    query_str = """
        SELECT 
            rv_trait_sp.code AS "code",
            rv_trait_sp.NAME AS "name",
            rv_variety_hybrid.crop_guid AS "crop_guid",
            rv_variety_hybrid.genetic_affiliation_guid AS "genetic_affiliation_guid",
            AVG(rv_variety_hybrid_trait_sp.number_value) AS "number_value"
          FROM RV_VARIETY_HYBRID_TRAIT_SP
        INNER JOIN (
            SELECT 
                trait_guid,
                code,
                NAME 
              FROM RV_TRAIT_SP
              WHERE LOWER(descr) LIKE '%maturity%' 
                AND  LOWER(descr) NOT LIKE '%gene%'
                AND  LOWER(descr) NOT LIKE '%weight%'
                AND  (LOWER(descr) NOT LIKE '%days%' OR {0} LIKE 'CORN%')
            ) rv_trait_sp
          ON rv_variety_hybrid_trait_sp.trait_guid = rv_trait_sp.trait_guid
        INNER JOIN RV_VARIETY_HYBRID
          ON rv_variety_hybrid_trait_sp.variety_hybrid_guid = rv_variety_hybrid.variety_hybrid_guid
        WHERE rv_variety_hybrid.genetic_affiliation_guid IS NOT NULL
          AND rv_variety_hybrid_trait_sp.number_value > 60
          AND rv_variety_hybrid_trait_sp.number_value < 200
        GROUP BY
            rv_trait_sp.code,
            rv_trait_sp.name,
            rv_variety_hybrid.crop_guid,
            rv_variety_hybrid.genetic_affiliation_guid
    """.format("'" + ap_data_sector + "'")

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_material_trait_data(ap_data_sector, analysis_year, season=None):
    query_str = """
    SELECT DISTINCT
        rv_ap_sector_experiment_config.analysis_year AS "analysis_year",
        rv_ap_sector_experiment_config.ap_data_sector_name AS "ap_data_sector_name",
        rv_ap_sector_experiment_config.decision_group AS "decision_group",
        rv_ap_sector_experiment_config.decision_group_rm AS "decision_group_rm",
        rv_ap_sector_experiment_config.stage AS "stage",
        rv_ap_sector_experiment_config.combined AS "combined",
        rv_ap_sector_experiment_config.technology AS "technology",
        UPPER(rv_bb_experiment_trial_entry_sdl.season) AS "season",
        rv_bb_experiment_trial_entry_sdl.entry_id AS "entry_id",
         CASE 
            WHEN rv_material_trait_sp.number_value < 15
                THEN 80 + rv_material_trait_sp.number_value*5
            WHEN rv_ap_sector_experiment_config.ap_data_sector_name LIKE 'SOY%' AND rv_trait_sp.code = 'MRTYN'
                THEN rv_material_trait_sp.number_value - 30
            ELSE rv_material_trait_sp.number_value
        END AS "number_value",
        CASE 
            WHEN rv_material_trait_sp.number_value < 15
                THEN 1
            ELSE 0
        END AS "rescaled_flag",
        rv_trait_sp.code AS "code",
        rv_trait_sp.name AS "name"
      FROM (
        SELECT 
            trait_guid,
            material_guid,
            AVG(number_value) AS number_value
          FROM rv_material_trait_sp
          GROUP BY
            trait_guid,
            material_guid
      ) rv_material_trait_sp
      INNER JOIN (
        SELECT *
        FROM rv_trait_sp
        WHERE LOWER(descr) LIKE '%maturity%' 
            AND  LOWER(descr) NOT LIKE '%gene%'
            AND  LOWER(descr) NOT LIKE '%weight%'
            AND (LOWER(descr) NOT LIKE '%days%' OR {0} LIKE 'CORN%')
        ) rv_trait_sp
        ON rv_material_trait_sp.trait_guid = rv_trait_sp.trait_guid
      INNER JOIN rv_ap_data_sector_config
        ON rv_trait_sp.crop_guid = rv_ap_data_sector_config.spirit_crop_guid
      INNER JOIN rv_material
        ON rv_material_trait_sp.material_guid = rv_material.material_guid
      INNER JOIN (
        SELECT DISTINCT
            ap_data_sector,
            season,
            experiment_id,
            material_id,
            entry_id
          FROM rv_trial_pheno_analytic_dataset
          WHERE ap_data_sector = {0}
      ) rv_bb_experiment_trial_entry_sdl
        ON rv_material.material_id = rv_bb_experiment_trial_entry_sdl.material_id
      INNER JOIN rv_ap_sector_experiment_config
        ON rv_bb_experiment_trial_entry_sdl.experiment_id = rv_ap_sector_experiment_config.experiment_id
      WHERE rv_ap_sector_experiment_config.ap_data_sector_name = {0}
        AND rv_ap_sector_experiment_config.analysis_year = {1}
        AND rv_material_trait_sp.number_value < 200
        AND (rv_material_trait_sp.number_value <15 OR rv_material_trait_sp.number_value > 65)
    """.format("'" + ap_data_sector + "'", int(analysis_year))

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)

    if season is not None:
        output_df = output_df[output_df['season'] == season]
    output_df = output_df.drop(columns=['season'])

    return output_df


def get_material_by_trialstage_year_one_sector(ap_data_sector, min_year=2012, max_year=2025, merge_on_loc_guid=True):
    if merge_on_loc_guid == False:
        query_str = """
            WITH data_sectors as (
                select distinct sector1.ap_data_sector_name,
                    sector1.spirit_country_code_list,
                    sector1.spirit_crop_name, 
                    sector1.spirit_crop_guid, 
                    sector1.spirit_season_code_list
                from rv_ap_data_sector_config sector1
                inner join rv_ap_data_sector_config sector2
                on sector1.spirit_country_code_list = sector2.spirit_country_code_list
                    and sector1.spirit_crop_name = sector2.spirit_crop_name 
                where sector2.ap_data_sector_name = {0}
            )
            SELECT
                rv_bb_experiment_trial_entry_sdl."ap_data_sector_name" as "ap_data_sector_name",
                rv_bb_experiment_trial_entry_sdl."year" AS "year",
                rv_bb_experiment_trial_entry_sdl."be_bid" AS "be_bid",
                RV_BE_BID_ANCESTRY.receiver_p AS "fp_be_bid",
                RV_BE_BID_ANCESTRY.donor_p AS "mp_be_bid",
                rv_bb_experiment_trial_entry_sdl."season" AS "season",
                MAX(rv_bb_experiment_trial_entry_sdl."stage_lid") AS "stage_lid"
              FROM (
                SELECT 
                    data_sectors.ap_data_sector_name as "ap_data_sector_name",
                    CAST(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year as integer) as "year",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid AS "crop_guid",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.BE_BID AS "be_bid",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.season AS "season",
                  CASE 
                      WHEN RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.cpi > 0
                          THEN 7
                      WHEN CAST(TRY_TO_NUMERIC(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid) AS decimal) < 7 AND CAST(TRY_TO_NUMERIC(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid) AS decimal) > 0
                          THEN CAST(ROUND(CAST(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid AS decimal), 0) AS integer)
                      WHEN LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr) LIKE '% stage%'
                          THEN CAST(COALESCE(99, SUBSTRING(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stage'), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stage')+1)) AS integer)
                      WHEN LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr) LIKE '% stg%'
                          THEN CAST(COALESCE(99, SUBSTRING(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stg') + LEN('stg'), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stg')+1)) AS integer)
                      WHEN RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_trial_no LIKE '[1-6]%'
                          THEN CAST(SUBSTRING(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_trial_no,1,1) AS integer)
                      ELSE 99
                  END AS "stage_lid"
                  FROM RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS  
                INNER JOIN data_sectors
                        ON RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid = data_sectors.spirit_crop_guid
                        AND data_sectors.spirit_season_code_list LIKE CONCAT('%', RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.season, '%')
                INNER JOIN (
                    SELECT DISTINCT
                          location_guid,
                          crop_guid,
                          country_code,
                          region_lid
                    FROM RV_BB_LOCATION_DAAS 
                    WHERE upper(region_lid) IN ('NOAM','EAME','APAC','LATAM','CHINA')
                  ) rv_bb_location_sdl
                    ON (RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid = rv_bb_location_sdl.crop_guid)
                      AND data_sectors.spirit_country_code_list LIKE CONCAT('%', rv_bb_location_sdl.country_code, '%')
                WHERE RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year >= {1}
                    AND (RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid IS NOT NULL AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid != '')
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year < {2}
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.be_bid IS NOT NULL
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_id IS NOT NULL
            ) rv_bb_experiment_trial_entry_sdl
            LEFT JOIN RV_BE_BID_ANCESTRY 
                ON rv_bb_experiment_trial_entry_sdl."be_bid" = RV_BE_BID_ANCESTRY.be_bid
            WHERE rv_bb_experiment_trial_entry_sdl."stage_lid" <= 7
            GROUP BY
                rv_bb_experiment_trial_entry_sdl."ap_data_sector_name",
                rv_bb_experiment_trial_entry_sdl."year",
                rv_bb_experiment_trial_entry_sdl."crop_guid",
                rv_bb_experiment_trial_entry_sdl."be_bid",
                rv_bb_experiment_trial_entry_sdl."season",
                RV_BE_BID_ANCESTRY.receiver_p,
                RV_BE_BID_ANCESTRY.donor_p
        """.format("'" + ap_data_sector + "'", int(min_year), int(max_year))

    else:
        query_str = """
            WITH data_sectors as (
                select distinct sector1.ap_data_sector_name,
                    sector1.spirit_country_code_list,
                    sector1.spirit_crop_name, 
                    sector1.spirit_crop_guid, 
                    sector1.spirit_season_code_list
                from rv_ap_data_sector_config sector1
                inner join rv_ap_data_sector_config sector2
                on sector1.spirit_country_code_list = sector2.spirit_country_code_list
                    and sector1.spirit_crop_name = sector2.spirit_crop_name 
                where sector2.ap_data_sector_name = {0}
            )
            SELECT
                rv_bb_experiment_trial_entry_sdl."ap_data_sector_name" as "ap_data_sector_name",
                rv_bb_experiment_trial_entry_sdl."year" AS "year",
                rv_bb_experiment_trial_entry_sdl."be_bid" AS "be_bid",
                RV_BE_BID_ANCESTRY.receiver_p AS "fp_be_bid",
                RV_BE_BID_ANCESTRY.donor_p AS "mp_be_bid",
                rv_bb_experiment_trial_entry_sdl."season" AS "season",
                MAX(rv_bb_experiment_trial_entry_sdl."stage_lid") AS "stage_lid"
              FROM (
                SELECT 
                    data_sectors.ap_data_sector_name as "ap_data_sector_name",
                    CAST(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year as integer) as "year",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid AS "crop_guid",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.BE_BID AS "be_bid",
                    RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.season AS "season",
                  CASE 
                      WHEN RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.cpi > 0
                          THEN 7
                      WHEN CAST(TRY_TO_NUMERIC(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid) AS decimal) < 7 AND CAST(TRY_TO_NUMERIC(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid) AS decimal) > 0
                          THEN CAST(ROUND(CAST(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid AS decimal), 0) AS integer)
                      WHEN LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr) LIKE '% stage%'
                          THEN CAST(COALESCE(99, SUBSTRING(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stage'), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stage')+1)) AS integer)
                      WHEN LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr) LIKE '% stg%'
                          THEN CAST(COALESCE(99, SUBSTRING(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stg') + LEN('stg'), REGEXP_INSTR(LOWER(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.trialcore_descr),'stage') + LEN('stg')+1)) AS integer)
                      WHEN RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_trial_no LIKE '[1-6]%'
                          THEN CAST(SUBSTRING(RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_trial_no,1,1) AS integer)
                      ELSE 99
                  END AS "stage_lid"
                  FROM RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS  
                INNER JOIN data_sectors
                        ON RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid = data_sectors.spirit_crop_guid
                        AND data_sectors.spirit_season_code_list LIKE CONCAT('%', RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.season, '%')
                INNER JOIN (
                    SELECT DISTINCT
                          location_guid,
                          crop_guid,
                          country_code,
                          region_lid
                    FROM RV_BB_LOCATION_DAAS 
                    WHERE upper(region_lid) IN ('NOAM','EAME','APAC','LATAM','CHINA')
                  ) rv_bb_location_sdl
                    ON (RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.location_guid = rv_bb_location_sdl.location_guid)
                      AND (RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.crop_guid = rv_bb_location_sdl.crop_guid)
                      AND data_sectors.spirit_country_code_list LIKE CONCAT('%', rv_bb_location_sdl.country_code, '%')
                WHERE RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year >= {1}
                    AND (RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid IS NOT NULL AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.stage_lid != '')
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.year < {2}
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.be_bid IS NOT NULL
                    AND RV_BB_EXPERIMENT_TRIAL_ENTRY_DAAS.experiment_id IS NOT NULL
            ) rv_bb_experiment_trial_entry_sdl
            LEFT JOIN RV_BE_BID_ANCESTRY 
                ON rv_bb_experiment_trial_entry_sdl."be_bid" = RV_BE_BID_ANCESTRY.be_bid
            WHERE rv_bb_experiment_trial_entry_sdl."stage_lid" <= 7
            GROUP BY
                rv_bb_experiment_trial_entry_sdl."ap_data_sector_name",
                rv_bb_experiment_trial_entry_sdl."year",
                rv_bb_experiment_trial_entry_sdl."crop_guid",
                rv_bb_experiment_trial_entry_sdl."be_bid",
                rv_bb_experiment_trial_entry_sdl."season",
                RV_BE_BID_ANCESTRY.receiver_p,
                RV_BE_BID_ANCESTRY.donor_p
        """.format("'" + ap_data_sector + "'", int(min_year), int(max_year))

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)
    return output_df


def get_material_by_trialstage_year_one_sector_tops(ap_data_sector, min_year=2012, max_year=2025):
    query_str = """
        SELECT rv_trial.ap_data_sector as "ap_data_sector_name",
            rv_trial.year as "year",
            rv_trial.be_bid AS "be_bid",
            RV_BE_BID_ANCESTRY.receiver_p AS "fp_be_bid",
            RV_BE_BID_ANCESTRY.donor_p AS "mp_be_bid",
            rv_trial.season as "season",
            MAX(rv_trial.stage_lid) as "stage_lid"
        FROM (
            select 
                ap_data_sector,
                year,
                be_bid,
                'SUMR' as season,
                CASE 
                  WHEN cpifl = True
                      THEN 7
                  WHEN CAST(trial_stage AS decimal) < 7 AND CAST(trial_stage AS decimal) > 0
                    THEN CAST(ROUND(CAST(trial_stage AS decimal), 0) AS integer)
                  WHEN experiment_trial_no LIKE '[1-6]%'
                    THEN CAST(SUBSTRING(experiment_trial_no,1,1) AS integer)
                  ELSE 13
                END AS stage_lid
            FROM rv_trial_pheno_analytic_dataset 
            WHERE ap_data_sector = {0}
                and year >= {1}
                and year < {2}
                and experiment_id is not null
                and be_bid is not null
                and COALESCE(pr_exclude,False) = False 
                and COALESCE(psp_exclude,False) = False
        ) rv_trial
        LEFT JOIN RV_BE_BID_ANCESTRY 
            ON rv_trial.be_bid = RV_BE_BID_ANCESTRY.be_bid
        WHERE stage_lid is not null 
            and stage_lid > 0 
            and stage_lid <= 13
        GROUP BY rv_trial.ap_data_sector , 
            rv_trial.year , 
            rv_trial.be_bid ,
            rv_trial.season,
            RV_BE_BID_ANCESTRY.receiver_p,
            RV_BE_BID_ANCESTRY.donor_p
    """.format("'" + ap_data_sector + "'", int(min_year), int(max_year))

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)

    return output_df


def get_trial_checks(ap_data_sector, analysis_year, analysis_type, breakout_level=''):
    query_str = """
        WITH rv_feature_export_ms AS (
            -- breakout_level information
            SELECT 
                market_name AS ap_data_sector,
                source_id,
                rv_feature_export.feature_level,
                REPLACE(LOWER(rv_feature_export.value), ' ', '_') AS market_seg
            FROM RV_FEATURE_EXPORT  
                WHERE rv_feature_export.source = 'SPIRIT'
                AND rv_feature_export.value != 'undefined'
                AND (rv_feature_export.feature_level = 'TRIAL' OR rv_feature_export.feature_level = 'LOCATION')
                AND LOWER(rv_feature_export.feature_name) = COALESCE(CASE WHEN {3} = '' THEN NULL ELSE '' END,'market_segment')
        ), rv_trait_config_backpop as (
             SELECT {1} as analysis_year, ap_data_sector_id, ap_data_sector_name, TRIM(trait) as trait, trait_guid, description,
                ap_analysis, distribution_type, viz_column, direction, conv_operator, conv_factor, spirit_uom, viz_uom, yield_trait,
                level, aae_use, dme_metric, dme_chkfl, dme_reg_x, dme_reg_y, dme_rm_est, dme_weighted_trait, is_gse_analysis_require, gse_trait_type
            FROM  rv_ap_sector_trait_config
            WHERE analysis_year=GREATEST(2019, {1})
        ), rv_experiment_config_backpop AS (
            SELECT analysis_year, ap_data_sector_id, ap_data_sector_name, experiment_id, decision_group,
                decision_group_rm, stage, combined, technology
            FROM rv_ap_sector_experiment_config
            UNION
            -- backpopulate by grabbing all experiment id's if year is before 2019. Default values for decision group, stage, etc.
            SELECT MIN(year) as analysis_year,
                   NULL as ap_data_sector_id,
                   MIN(ap_data_sector) as ap_data_sector_name,
                   experiment_id as experiment_id,
                   experiment_id as decision_group,
                   ROUND((AVG(maturity_group)*5 + 80)*2,0)/2  as decision_group_rm, -- round to nearest 5
                   0 as stage,
                   0 as combined,
                   NULL as technology
                FROM rv_trial_pheno_analytic_dataset
            WHERE ap_data_sector = {0}
                AND year = {1}
                AND year < 2019 
            GROUP BY experiment_id
        ), query_output AS (
            SELECT 
                rv_trial_pheno_analytic_dataset.ap_data_sector AS ap_data_sector,
                rv_ap_sector_experiment_config.analysis_year AS analysis_year, 
                rv_trial_pheno_analytic_dataset.experiment_id AS experiment_id,
                rv_ap_sector_experiment_config.decision_group AS decision_group, 
                rv_trial_pheno_analytic_dataset.entry_id AS entry_id,
                rv_trial_pheno_analytic_dataset.be_bid AS be_bid,
                rv_trial_pheno_analytic_dataset.cpifl AS cpifl,
                CASE 
                    WHEN MAX(rv_trial_pheno_analytic_dataset.cperf) OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),
                                 LOWER(rv_feature_export_ms_trial.market_seg),'all')) > 0
                        THEN rv_trial_pheno_analytic_dataset.cperf
                    ELSE rv_trial_pheno_analytic_dataset.cpifl
                END AS cperf,
                CASE 
                    WHEN MAX(rv_trial_pheno_analytic_dataset.cagrf) OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),
                                 LOWER(rv_feature_export_ms_trial.market_seg),'all')) > 0
                        THEN rv_trial_pheno_analytic_dataset.cagrf
                    ELSE rv_trial_pheno_analytic_dataset.cpifl
                END AS cagrf,
                CASE 
                    WHEN MAX(rv_trial_pheno_analytic_dataset.cmatf) OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),
                                 LOWER(rv_feature_export_ms_trial.market_seg),'all')) > 0
                        THEN rv_trial_pheno_analytic_dataset.cmatf
                    ELSE rv_trial_pheno_analytic_dataset.cpifl
                END AS cmatf,
                CASE 
                    WHEN MAX(rv_trial_pheno_analytic_dataset.cregf) OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),
                                 LOWER(rv_feature_export_ms_trial.market_seg),'all')) > 0
                        THEN rv_trial_pheno_analytic_dataset.cregf
                    ELSE rv_trial_pheno_analytic_dataset.cpifl
                END AS cregf,
                CASE 
                    WHEN MAX(rv_trial_pheno_analytic_dataset.crtnf) OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),
                                 LOWER(rv_feature_export_ms_trial.market_seg),'all')) > 0
                        THEN rv_trial_pheno_analytic_dataset.crtnf
                    ELSE rv_trial_pheno_analytic_dataset.cpifl
                END AS crtnf,
                rv_trial_pheno_analytic_dataset.result_count AS count,
                rv_ap_sector_experiment_config.combined AS combined, 
                COALESCE(rv_ap_sector_experiment_config.decision_group_rm, rv_trial_pheno_analytic_dataset.decision_group_rm) AS decision_group_rm, 
                rv_ap_sector_experiment_config.stage AS stage, 
                rv_ap_sector_experiment_config.technology AS technology, 
                COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),LOWER(rv_feature_export_ms_trial.market_seg),'all') AS market_seg,
                SUM(rv_trial_pheno_analytic_dataset.cpifl) 
                    OVER (PARTITION BY 
                        rv_trial_pheno_analytic_dataset.ap_data_sector,
                        rv_trial_pheno_analytic_dataset.experiment_id,
                        COALESCE(LOWER(rv_feature_export_ms_loc.market_seg),LOWER(rv_feature_export_ms_trial.market_seg),'all'))
                    AS cpifl_sum,
                MAX(rv_ap_sector_experiment_config.stage) 
                    OVER (PARTITION BY rv_ap_sector_experiment_config.ap_data_sector_name,rv_ap_sector_experiment_config.experiment_id)
                    AS stage_max
            FROM (
                --  Pull sum of each check flag per trial-material-trait
                SELECT rv_trial_pheno_analytic_dataset.ap_data_sector AS ap_data_sector,
                    rv_trial_pheno_analytic_dataset.experiment_id AS experiment_id,
                    rv_trial_pheno_analytic_dataset.trial_id AS trial_id,
                    rv_trial_pheno_analytic_dataset.entry_id AS entry_id,
                    rv_trial_pheno_analytic_dataset.be_bid AS be_bid,
                    COUNT(CASE WHEN rv_trial_pheno_analytic_dataset.cpifl THEN 1 END) AS cpifl,
                    SUM(rv_trial_pheno_analytic_dataset.cperf) AS cperf,
                    SUM(rv_trial_pheno_analytic_dataset.cagrf) AS cagrf,
                    SUM(rv_trial_pheno_analytic_dataset.cmatf) AS cmatf,
                    SUM(rv_trial_pheno_analytic_dataset.cregf) AS cregf,
                    SUM(rv_trial_pheno_analytic_dataset.crtnf) AS crtnf,
                    AVG(rv_trial_pheno_analytic_dataset.maturity_group)*5+80 as decision_group_rm,
                    rv_trial_pheno_analytic_dataset.trait_measure_code AS trait_measure_code,
                    rv_trial_pheno_analytic_dataset.loc_selector AS loc_selector,
                    GREATEST(
                        COUNT(rv_trial_pheno_analytic_dataset.result_numeric_value), 
                        COUNT(rv_trial_pheno_analytic_dataset.result_alpha_value)
                    ) AS result_count
                FROM rv_trial_pheno_analytic_dataset
                WHERE (result_numeric_value IS NOT NULL OR result_alpha_value IS NOT NULL)
                    AND ap_data_sector =  {0}
                    AND year = {1}
            --          AND NOT CAST(COALESCE(psp_exclude,False) AS BOOLEAN) 
            --          AND NOT CAST(COALESCE(pr_exclude,False) AS BOOLEAN) 
            --          AND NOT CAST(COALESCE(tr_exclude,False) AS BOOLEAN) 
                GROUP BY
                    rv_trial_pheno_analytic_dataset.ap_data_sector,
                    rv_trial_pheno_analytic_dataset.experiment_id,
                    rv_trial_pheno_analytic_dataset.trial_id,
                    rv_trial_pheno_analytic_dataset.entry_id,
                    rv_trial_pheno_analytic_dataset.be_bid,
                    rv_trial_pheno_analytic_dataset.trait_measure_code,
                    rv_trial_pheno_analytic_dataset.loc_selector
            ) rv_trial_pheno_analytic_dataset
            INNER JOIN (
                --  Restrict to DME-relevant traits
                SELECT analysis_year,
                    ap_data_sector_name,
                    trait
                FROM rv_trait_config_backpop
                WHERE ap_data_sector_name =  {0}
                    AND CAST(analysis_year as integer) = {1}
                    AND dme_metric != 'na'
                GROUP BY ap_data_sector_name, trait, analysis_year
            ) rv_ap_sector_trait_config
            ON LOWER(rv_trial_pheno_analytic_dataset.trait_measure_code) = LOWER(rv_ap_sector_trait_config.trait)
                AND rv_trial_pheno_analytic_dataset.ap_data_sector = rv_ap_sector_trait_config.ap_data_sector_name
            INNER JOIN ( 
                --  Restrict to listed experiments, except for years before 2019, where all experiments are grabbed
                SELECT DISTINCT
                    CAST(rv_experiment_config_backpop.analysis_year AS integer) AS analysis_year,
                    rv_experiment_config_backpop.ap_data_sector_name,
                    rv_experiment_config_backpop.experiment_id,
                    rv_experiment_config_backpop.decision_group,
                    rv_experiment_config_backpop.combined,
                    rv_experiment_config_backpop.stage,
                    rv_experiment_config_backpop.decision_group_rm,
                    LOWER(
                        CASE WHEN "ap_data_sector_name" = 'SOY_NA_SUMMER' 
                            THEN COALESCE(rv_experiment_config_backpop.technology,'conv')
                        WHEN "ap_data_sector_name" = 'CORNGRAIN_APAC_1' 
                            THEN COALESCE(rv_experiment_config_backpop.technology,'all')
                        ELSE 'all' END
                    ) AS technology
                FROM rv_experiment_config_backpop
                WHERE ap_data_sector_name = {0}
                    AND analysis_year = {1}
            ) rv_ap_sector_experiment_config
            ON rv_trial_pheno_analytic_dataset.experiment_id = rv_ap_sector_experiment_config.experiment_id
                AND rv_trial_pheno_analytic_dataset.ap_data_sector = rv_ap_sector_experiment_config.ap_data_sector_name
                AND rv_ap_sector_trait_config.analysis_year = rv_ap_sector_experiment_config.analysis_year
                AND rv_ap_sector_trait_config.ap_data_sector_name = rv_ap_sector_experiment_config.ap_data_sector_name
            LEFT JOIN (
                SELECT 
                    rv_feature_export.market_name AS ap_data_sector,
                    rv_feature_export.source_id,
                    rv_feature_export.feature_level,
                    REPLACE(LOWER(rv_feature_export.value), ' ', '_') AS market_seg
                FROM rv_feature_export
                    WHERE rv_feature_export.source = 'SPIRIT'
                    AND rv_feature_export.value != 'undefined'
                    AND rv_feature_export.feature_level = 'LOCATION'
                    AND LOWER(rv_feature_export.feature_name) = COALESCE({3},'market_seg')
            ) rv_feature_export_ms_loc
            ON rv_trial_pheno_analytic_dataset.loc_selector = rv_feature_export_ms_loc.source_id
                AND rv_trial_pheno_analytic_dataset.ap_data_sector = rv_feature_export_ms_loc.ap_data_sector
                AND rv_feature_export_ms_loc.feature_level = 'LOCATION'
            LEFT JOIN (
                SELECT 
                    rv_feature_export.market_name AS ap_data_sector,
                    rv_feature_export.source_id,
                    rv_feature_export.feature_level,
                    REPLACE(LOWER(rv_feature_export.value), ' ', '_') AS market_seg
                FROM rv_feature_export
                    WHERE rv_feature_export.source = 'SPIRIT'
                    AND rv_feature_export.value != 'undefined'
                    AND rv_feature_export.feature_level = 'TRIAL'
                    AND LOWER(rv_feature_export.feature_name) = COALESCE({3},'market_seg')
            ) rv_feature_export_ms_trial
            ON rv_trial_pheno_analytic_dataset.trial_id = rv_feature_export_ms_trial.source_id
                AND rv_trial_pheno_analytic_dataset.ap_data_sector = rv_feature_export_ms_trial.ap_data_sector
                AND rv_feature_export_ms_trial.feature_level = 'TRIAL'
        ),
        query_output_et AS (
                SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.count) AS count,
                    query_output.decision_group_rm AS decision_group_rm,
                    CASE WHEN query_output.ap_data_sector = 'CORN_NA_SUMMER'
                        THEN (CAST(query_output.decision_group_rm AS decimal)-80)/5
                        ELSE CAST(query_output.decision_group_rm AS decimal)
                    END AS decision_group_rm2,
                    query_output.technology AS technology,
                    query_output.market_seg AS market_seg,
                    SUM(query_output.cpifl_sum) AS cpifl_sum
                FROM query_output
                WHERE query_output.stage > 3
                AND {1} IN ('MultiYear', 'MynoET')
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.entry_id,
                    query_output.be_bid,
                    query_output.decision_group_rm,
                    query_output.technology,
                    query_output.market_seg
            )
            SELECT
                rv_trial_pheno_analytic_dataset.ap_data_sector AS "ap_data_sector",
                rv_trial_pheno_analytic_dataset.analysis_year AS "analysis_year",
                rv_trial_pheno_analytic_dataset.analysis_type AS "analysis_type",
                rv_trial_pheno_analytic_dataset.source_id AS "source_id",
                rv_trial_pheno_analytic_dataset.entry_id AS "entry_id",
                rv_trial_pheno_analytic_dataset.be_bid AS "be_bid",
                rv_trial_pheno_analytic_dataset.cpifl AS "cpifl",
                rv_trial_pheno_analytic_dataset.cperf AS "cperf",
                rv_trial_pheno_analytic_dataset.cagrf AS "cagrf",
                rv_trial_pheno_analytic_dataset.cmatf AS "cmatf",
                rv_trial_pheno_analytic_dataset.cregf AS "cregf",
                rv_trial_pheno_analytic_dataset.crtnf AS "crtnf",
                rv_trial_pheno_analytic_dataset.count AS "count",
                rv_trial_pheno_analytic_dataset.decision_group_rm AS "decision_group_rm",
                COALESCE(rv_trial_pheno_analytic_dataset.stage, rv_trial_pheno_analytic_dataset.stage_max) AS "stage",
                rv_trial_pheno_analytic_dataset.technology AS "technology",
                rv_trial_pheno_analytic_dataset.market_seg AS "market_seg",
                'entry' AS "material_type",
                rv_trial_pheno_analytic_dataset.cpifl_sum AS "cpifl_sum",
                COALESCE(filled_material_list.female_entry_id, RV_BE_BID_ANCESTRY.receiver_p, rv_trial_pheno_analytic_dataset.entry_id) AS "female_entry_id",
                COALESCE(filled_material_list.male_entry_id, RV_BE_BID_ANCESTRY.donor_p, rv_trial_pheno_analytic_dataset.entry_id) AS "male_entry_id"
            FROM (
                 SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    CASE WHEN query_output.combined = 0 THEN 'SingleExp' ELSE 'MultiExp' END AS analysis_type,
                    query_output.decision_group AS source_id,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.count) AS count,
                    query_output.decision_group_rm AS decision_group_rm,
                    query_output.stage AS stage,
                    query_output.technology AS technology,
                    query_output.market_seg AS market_seg,
                    SUM(query_output.cpifl_sum) AS cpifl_sum,
                    MAX(query_output.stage_max) AS stage_max
                FROM query_output
                WHERE {1} IN ('MultiExp', 'SingleExp')
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.decision_group,
                    query_output.combined,
                    query_output.entry_id,
                    query_output.be_bid,
                    query_output.decision_group_rm,
                    query_output.technology,
                    query_output.market_seg,
                    query_output.stage
                UNION ALL
                SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    'GenoPred' AS analysis_type,
                    query_output.decision_group AS source_id,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.count) AS count,
                    query_output.decision_group_rm AS decision_group_rm,
                    query_output.stage AS stage,
                    query_output.technology AS technology,
                    query_output.market_seg AS market_seg,
                    SUM(query_output.cpifl_sum) AS cpifl_sum,
                    MAX(query_output.stage_max) AS stage_max
                FROM query_output
                WHERE stage < 4.1
                --        AND decision_group NOT IN (SELECT DISTINCT experiment_id FROM managed.rv_ap_sector_experiment_config)
                    AND ap_data_sector IN ('CORN_NA_SUMMER', 'CORN_EAME_SUMMER')
                    AND {1} = 'GenoPred'
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.decision_group,
                    query_output.entry_id,
                    query_output.be_bid,
                    query_output.decision_group_rm,
                    query_output.technology,
                    query_output.market_seg,
                    query_output.stage
                UNION ALL
                SELECT 
                    query_output.ap_data_sector AS ap_data_sector,
                    query_output.analysis_year AS analysis_year,
                    'SingleExp' AS analysis_type,
                    query_output.experiment_id AS source_id,
                    query_output.entry_id AS entry_id,
                    query_output.be_bid AS be_bid,
                    SUM(query_output.cpifl) AS cpifl,
                    SUM(query_output.cperf) AS cperf,
                    SUM(query_output.cagrf) AS cagrf,
                    SUM(query_output.cmatf) AS cmatf,
                    SUM(query_output.cregf) AS cregf,
                    SUM(query_output.crtnf) AS crtnf,
                    SUM(query_output.count) AS count,
                    query_output.decision_group_rm AS decision_group_rm,
                    query_output.stage AS stage,
                    query_output.technology AS technology,
                    query_output.market_seg AS market_seg,
                    SUM(query_output.cpifl_sum) AS cpifl_sum,
                    MAX(query_output.stage_max) AS stage_max
                   FROM query_output
                   WHERE stage >= 4
                    AND {1} = 'SingleExp'
                GROUP BY
                    query_output.ap_data_sector,
                    query_output.analysis_year,
                    query_output.experiment_id,
                    query_output.entry_id,
                    query_output.be_bid,
                    query_output.decision_group_rm,
                    query_output.technology,
                    query_output.market_seg,
                    query_output.stage
                UNION ALL
                SELECT 
                    query_output_et.ap_data_sector AS ap_data_sector,
                    query_output_et.analysis_year AS analysis_year,
                    'MynoET' AS analysis_type,
                    CONCAT('zone', CAST(query_output_et.decision_group_rm2 AS integer)) AS source_id,
                    query_output_et.entry_id AS entry_id,
                    query_output_et.be_bid AS be_bid,
                    query_output_et.cpifl AS cpifl,
                    query_output_et.cperf AS cperf,
                    query_output_et.cagrf AS cagrf,
                    query_output_et.cmatf AS cmatf,
                    query_output_et.cregf AS cregf,
                    query_output_et.crtnf AS crtnf,
                    query_output_et.count AS count,
                    query_output_et.decision_group_rm AS decision_group_rm,
                    6 AS stage,
                    query_output_et.technology AS technology,
                    query_output_et.market_seg AS market_seg,
                    query_output_et.cpifl_sum AS cpifl_sum,
                    6 AS stage_max
                FROM query_output_et
                WHERE {1} = 'MynoET'
                UNION ALL
                SELECT 
                    query_output_et.ap_data_sector AS ap_data_sector,
                    query_output_et.analysis_year AS analysis_year,
                    'MultiYear' AS analysis_type,
                    rv_feature_export_et.et AS source_id,
                    query_output_et.entry_id AS entry_id,
                    query_output_et.be_bid AS be_bid,
                    query_output_et.cpifl AS cpifl,
                    query_output_et.cperf AS cperf,
                    query_output_et.cagrf AS cagrf,
                    query_output_et.cmatf AS cmatf,
                    query_output_et.cregf AS cregf,
                    query_output_et.crtnf AS crtnf,
                    query_output_et.count AS count,
                    query_output_et.decision_group_rm AS decision_group_rm,
                    6 AS stage,
                    query_output_et.technology AS technology,
                    query_output_et.market_seg AS market_seg,
                    query_output_et.cpifl_sum AS cpifl_sum,
                    6 AS stage_max
                FROM query_output_et
                INNER JOIN (
                    SELECT DISTINCT
                        rv_feature_export.market_name AS ap_data_sector,
                        rv_feature_export.value AS et
                    FROM rv_feature_export
                    WHERE rv_feature_export.source = 'SPIRIT'
                        AND rv_feature_export.value != 'undefined' 
                        AND (
                            (rv_feature_export.market_name = 'CORN_NA_SUMMER' AND
                                 (rv_feature_export.feature_name = 'EnvironmentType_level2' AND
                                  rv_feature_export.value LIKE 'ET08%')
                                OR
                                (rv_feature_export.feature_name = 'EnvironmentType_level1' AND
                                 rv_feature_export.value != 'ET08')
                            )
                            OR (
                                rv_feature_export.market_name != 'CORN_NA_SUMMER' AND
                                rv_feature_export.feature_name = 'EnvironmentType_level1'
                            )
                        )
                ) rv_feature_export_et
                ON query_output_et.ap_data_sector = rv_feature_export_et.ap_data_sector
                WHERE {1} = 'MultiYear'   
            ) rv_trial_pheno_analytic_dataset
            LEFT JOIN (
                SELECT
                    rv_corn_material_tester.be_bid AS be_bid,
            --            rv_corn_material_tester.abbr_code AS entry_id,
            --            entry_id_list.experiment_id AS experiment_id,
                    rv_corn_material_tester.female AS female_entry_id,
                    rv_corn_material_tester.male AS male_entry_id
                FROM (
                    SELECT 
                        rv_corn_material_tester.be_bid AS be_bid,
            --                rv_corn_material_tester.abbr_code AS abbr_code,
                        CASE
                            WHEN (rv_corn_material_tester.fp_het_group = 'nss' AND (rv_corn_material_tester.mp_het_group IN ('', 'ss') OR rv_corn_material_tester.mp_het_group IS NULL)) OR 
                            ((rv_corn_material_tester.fp_het_group IN ('', 'nss') OR rv_corn_material_tester.fp_het_group IS NULL) AND rv_corn_material_tester.mp_het_group = 'ss')
                            THEN COALESCE(rv_corn_material_tester.mp_be_bid, rv_corn_material_tester.male, SUBSTRING(abbr_code,REGEXP_INSTR(abbr_code,'/')+1,LEN(abbr_code)))
                            ELSE COALESCE(rv_corn_material_tester.fp_be_bid, rv_corn_material_tester.female, SUBSTRING(abbr_code,0,REGEXP_INSTR(abbr_code,'/')))
                        END AS female,
                        CASE 
                            WHEN (rv_corn_material_tester.mp_het_group = 'ss' AND (rv_corn_material_tester.fp_het_group IN ('', 'nss') OR rv_corn_material_tester.fp_het_group IS NULL)) OR 
                            ((rv_corn_material_tester.mp_het_group IN ('', 'ss') OR rv_corn_material_tester.mp_het_group IS NULL) AND rv_corn_material_tester.fp_het_group = 'nss')
                            THEN COALESCE(rv_corn_material_tester.fp_be_bid, rv_corn_material_tester.female, SUBSTRING(abbr_code,0,REGEXP_INSTR(abbr_code,'/')))
                            ELSE COALESCE(rv_corn_material_tester.mp_be_bid, rv_corn_material_tester.male, SUBSTRING(abbr_code,REGEXP_INSTR(abbr_code,'/')+1,LEN(abbr_code)))
                        END AS male
                    FROM RV_CORN_MATERIAL_TESTER_ADAPT rv_corn_material_tester
                    WHERE abbr_code LIKE '%/%'
                ) rv_corn_material_tester
                INNER JOIN (
                    SELECT DISTINCT
                        rv_trial_pheno_analytic_dataset.be_bid
                    FROM (
                        SELECT DISTINCT
                            experiment_id,
                            be_bid
                        FROM rv_trial_pheno_analytic_dataset
                    ) rv_trial_pheno_analytic_dataset
                    INNER JOIN (
                        SELECT DISTINCT
                            experiment_id
                        FROM rv_ap_sector_experiment_config
                    ) rv_ap_sector_experiment_config
                    ON rv_ap_sector_experiment_config.experiment_id = rv_trial_pheno_analytic_dataset.experiment_id
                ) entry_id_list
                ON entry_id_list.be_bid = rv_corn_material_tester.be_bid
            ) filled_material_list
            ON rv_trial_pheno_analytic_dataset.be_bid = filled_material_list.be_bid
                AND rv_trial_pheno_analytic_dataset.ap_data_sector IN ('CORN_NA_SUMMER', 'CORN_EAME_SUMMER')
            LEFT JOIN RV_BE_BID_ANCESTRY 
            ON rv_trial_pheno_analytic_dataset.be_bid = RV_BE_BID_ANCESTRY.be_bid
                AND RV_BE_BID_ANCESTRY.receiver_p <> RV_BE_BID_ANCESTRY.donor_p
                AND (RV_BE_BID_ANCESTRY.receiver_p IS NOT NULL OR RV_BE_BID_ANCESTRY.donor_p IS NOT NULL)
            WHERE rv_trial_pheno_analytic_dataset.analysis_type = {1}
    """.format("'" + ap_data_sector + "'", int(analysis_year), "'" + analysis_type + "'", "'" + breakout_level + "'")

    with SnowflakeConnection() as dc:
        output_df = dc.get_data(query_str)

    return output_df


