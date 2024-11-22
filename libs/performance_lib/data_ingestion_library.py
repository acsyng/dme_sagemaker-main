import os
import sys
import pandas as pd
import numpy as np
import boto3
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.performance_lib import performance_sql_recipes
from libs.performance_lib import performance_helper
from libs.postgres.postgres_connection import PostgresConnection

s3 = boto3.client('s3')
season_mapper = {
    'SPRING':'SPR',
    'SUMMER': 'SUMR',
    'WINTER': 'WNTR',
    'AUTUMN': 'AUTM'
}


# trial checks data
def compute_trial_checks(ap_data_sector,
                         analysis_year,
                         analysis_type,
                         pipeline_runid='',
                         season=None,
                         write_outputs=0
                         ):
    # Compute recipe outputs
    print('Connecting to Denodo')
    with SnowflakeConnection() as dc:
        data_sector_config = dc.get_data(
            """
            SELECT 
                ap_data_sector_name AS "ap_data_sector_name",
                spirit_crop_guid AS "spirit_crop_guid",
                entry_id_source AS "entry_id_source"
              FROM RV_AP_DATA_SECTOR_CONFIG 
            WHERE ap_data_sector_name = {0}
            """.format("'" + ap_data_sector + "'")
        )

    print('Creating df_checks')
    df_checks = performance_sql_recipes.merge_trial_check_entries(
        ap_data_sector,
        analysis_year,
        analysis_type,
        data_sector_config["spirit_crop_guid"].iloc[0],
        season=season
    )
    print(df_checks.shape)
    return df_checks


# get data from trial pheno
def load_trial_pheno(ap_data_sector,
                     analysis_type,
                     analysis_year,
                     season=None,
                     write_outputs=0):
    print('Creating df_trial_pheno')

    df_trial_pheno = performance_sql_recipes.get_trial_pheno_data_reduced_columns(
        ap_data_sector,
        int(analysis_year),
        season=season
    )

    print(df_trial_pheno.shape)
    return df_trial_pheno


# get decision groups and analysis type for pvs data
# get all multi experiment groups, then any single experiment groups that are not within the multi experiment group
def get_pvs_dgs(ap_data_sector, analysis_year, technology=None,season=None):
    dc = PostgresConnection()
    year_offset = 0
    if performance_helper.check_for_year_offset(ap_data_sector):
        year_offset = 1

    query_str = """
        SELECT upper(dg.decision_group_name) as "decision_group_name", dg.decision_group_type , dge.experiment_id , dg.stage
        FROM advancement.decision_group dg 
        inner join advancement.decision_group_experiment dge 
            on dge.decision_group_id  = dg.decision_group_id 	
        where dg.ap_data_sector_name = {0}
        and dg.analysis_year = {1}
        and dg.decision_group_type = {2}
        and cast(dg.stage as float) < 10
   """
    query_str_multi = query_str.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, "'" + 'MULTI_EXPERIMENT' + "'")
    query_str_single = query_str.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, "'" + 'SINGLE_EXPERIMENT' + "'")
    if technology is not None:
        query_str_multi = query_str_multi + "and dg.technology = {0}".format("'"+technology+"'")
        query_str_single = query_str_single + "and dg.technology = {0}".format("'"+technology+"'")

    df_multi = dc.get_data(query_str_multi)
    df_single = dc.get_data(query_str_single)

    # Goal is to prevent data from showing up in a single experiment and multi experiment, take multi experiment first
    # concat, check for data for all decision groups first, then drop any without data, then drop duplicated experiments
    df = pd.concat(
        (df_multi, df_single), axis=0, ignore_index=True
    )

    # for each decision group, get a count of rows in RV_AP_PVS_ALL
    # if no rows, drop
    query_str = """
        SELECT AP_DATA_SECTOR , SOURCE_ID as "decision_group_name" , COUNT(*)
        FROM RV_AP_ALL_PVS 
        WHERE AP_DATA_SECTOR = {}
        AND source_id IN {}
        GROUP BY AP_DATA_SECTOR , SOURCE_ID 
    """.format("'"+ap_data_sector+"'", performance_helper.make_search_str(df['decision_group_name']))

    with SnowflakeConnection() as dc:
        df_dg_count = dc.get_data(query_str)
    # filter decision groups without data. They won't show up in the query output, so an inner join is sufficient
    df = df.merge(
        df_dg_count[['decision_group_name']],
        on='decision_group_name',
        how='inner'
    )
    # drop duplicated experiment_ids so that we don't duplicate trialing data
    df = df.groupby(by=['experiment_id']).first().reset_index()

    # get season from RV_BB_EXPERIMENT_TRIAL_DAAS for each experiment id, only keep those from the correct season
    if season is not None:
        exp_list_search = performance_helper.make_search_str(df['experiment_id'])
        season_search=season

        query_str = """
            SELECT distinct EXPERIMENT_ID as "experiment_id"
            FROM RV_BB_EXPERIMENT_TRIAL_DAAS  
            WHERE EXPERIMENT_ID in {}
                and upper(season) = {}
        """.format(exp_list_search, "'" + season_search + "'")
        with SnowflakeConnection() as dc:
            df_exp_keep = dc.get_data(query_str)

        print(query_str)

        df = df.merge(df_exp_keep, on= "experiment_id", how='inner')

    df = df[['decision_group_name','decision_group_type','stage']]

    return df


# load geno prediction data
def load_pvs_data(ap_data_sector,
                  analysis_type,
                  analysis_year,
                  backup_data_sector=None,
                  season=None,
                  write_outputs=0,
                  get_parents=0):
    print('Creating df_pvs_no_bebid')

    df_pvs_dgs = get_pvs_dgs(
        ap_data_sector,
        analysis_year,
        season=season
    )

    df_pvs_no_bebid = performance_sql_recipes.merge_pvs_input(
        ap_data_sector=ap_data_sector,
        analysis_year=analysis_year,
        df_dgs=df_pvs_dgs,
        get_parents=get_parents
    )

    # for some data sectors (like corn philippines), historic data may be stored under a different name
    df_pvs_to_find = pd.DataFrame()
    if backup_data_sector is not None:
        # get decision groups from df_pvs_dgs that aren't in df_pvs_no_bebid
        # then search for those decision groups, append to df_pvs_no_bebid
        pvs_dgs_found = pd.unique(df_pvs_no_bebid['source_id'])
        df_dgs_to_find = df_pvs_dgs[~df_pvs_dgs['decision_group_name'].isin(pvs_dgs_found)]

        if df_dgs_to_find.shape[0] > 0: # if we are missing data from decision groups
            df_pvs_to_find = performance_sql_recipes.merge_pvs_input(
                ap_data_sector=backup_data_sector,
                analysis_year=analysis_year,
                df_dgs=df_dgs_to_find,
                get_parents=get_parents
            )
            df_pvs_to_find['ap_data_sector'] = ap_data_sector # update data sector
        elif df_pvs_dgs.shape[0] == 0:
            # if we don't have decision groups, we want to avoid grabbing data from other sectors
            # for CORNGRAIN_APAC_1, look at technology...
            if backup_data_sector == 'CORNGRAIN_APAC_1':
                # find decision groups in rv_ap_pvs_all that have technology matching the original data sector
                sector_to_tech_map = {
                    'CORN_INDONESIA_ALL' : 'Indonesia',
                    'CORN_INDIA_SPRING': 'India',
                    'CORN_INDIA_DRY': 'India',
                    'CORN_INDIA_WET':'India',
                    'CORN_PAKISTAN_ALL':'Pakistan',
                    'CORN_THAILAND_ALL':'Thailand',
                    'CORN_PHILIPPINES_ALL':'Philippines',
                    'CORN_VIETNAM_ALL':'Vietnam',
                    'CORN_BANGLADESH_ALL':'Bangladesh'
                }
                df_dgs_to_find = get_pvs_dgs(
                    ap_data_sector=backup_data_sector,
                    analysis_year=analysis_year,
                    technology=sector_to_tech_map[ap_data_sector],
                    season=season
                )

                if df_dgs_to_find.shape[0] != 0:
                    df_pvs_to_find = performance_sql_recipes.merge_pvs_input(
                        ap_data_sector=backup_data_sector,
                        analysis_year=analysis_year,
                        df_dgs=df_dgs_to_find,
                        get_parents=get_parents
                    )
                    df_pvs_to_find['ap_data_sector'] = ap_data_sector  # update data sector

        # combine with df_pvs_no_bebid
        if df_pvs_to_find.shape[0] > 0 and df_pvs_no_bebid.shape[0] > 0:  # if both, concat
            df_pvs_no_bebid = pd.concat((df_pvs_no_bebid, df_pvs_to_find), axis=0)
        elif df_pvs_to_find.shape[0] > 0 and df_pvs_no_bebid.shape[0] == 0:  # if only backup, overwrite
            df_pvs_no_bebid = df_pvs_to_find

    print(df_pvs_no_bebid.shape)
    return df_pvs_no_bebid


# Get text/marker traits from the plot
def load_text_traits_from_plot_data(be_bid_arr,crop_name,analysis_year):
    print('Creating df_plot_result')

    df_plot_result = performance_sql_recipes.get_plot_result_data(be_bid_arr=be_bid_arr,
                                                                  crop_name=crop_name,
                                                                  analysis_year=analysis_year)

    return df_plot_result


# get RM data across multiple tables
def load_rm_data_across_datasets(ap_data_sector,
                                 analysis_year,
                                 analysis_type,
                                 season=None,
                                 write_outputs=0):
    print('Creating df_material_trait_data')

    # get RM data for hybrids across 2 tables (rv_variety_trait_data, rv_material_trait_data)
    df_material_trait_data = performance_sql_recipes.get_material_trait_data(
        ap_data_sector=ap_data_sector,
        analysis_year=analysis_year,
        season=season
    )
    print(df_material_trait_data.shape)

    print('Creating df_hybrid_rm1')

    df_hybrid_rm1 = performance_helper.get_hybrid_rm(df_material_trait_data,
                                                     ap_data_sector,
                                                     analysis_year
                                                     )
    return df_material_trait_data, df_hybrid_rm1


def get_bebid_advancement_decisions(ap_data_sector,
                                    input_years_str,
                                    analysis_type=None,
                                    season=None,
                                    merge_on_loc_guid=True,
                                    write_outputs=0):
    # get be_bid, and parental be_bids, plus stage tested from denodo
    print('Creating df_get_bebid')
    # get data per year, concat across year. This should reduce load compared to getting data from all years simultaneously.
    df_get_bebid_list=[]

    input_years_int = [int(yr) for yr in input_years_str]
    min_yr = np.min(input_years_int)-2 # get data before minimum year requested to have "prev_stage" column
    max_yr = np.max(input_years_int)+3 # get data after maximum year requested to have futures stage columns

    for yr in range(min_yr, max_yr):
        if int(yr) >= 2024 and ap_data_sector == 'CORN_NA_SUMMER': # use tops dataset for recent corn NA data
            df_get_bebid_list.append(performance_sql_recipes.get_material_by_trialstage_year_one_sector_tops(
                ap_data_sector=ap_data_sector,
                min_year=yr,
                max_year=yr + 1
            ))
        else:
            df_get_bebid_list.append(performance_sql_recipes.get_material_by_trialstage_year_one_sector(
                ap_data_sector=ap_data_sector,
                min_year=yr,
                max_year=yr+1,
                merge_on_loc_guid=merge_on_loc_guid
            ))

    df_get_bebid = pd.concat(df_get_bebid_list,axis=0).drop_duplicates()
    if df_get_bebid.shape[0] > 0 and season is not None:
        df_get_bebid = df_get_bebid[df_get_bebid['season'] == season]

    print(df_get_bebid.shape)
    return df_get_bebid


# STEP 7: Step to load historical adv decision skipped because of the dependencies to get_bebid processing and
# load_bebid processing. These will be taken care in preprocessing so, the next step would be to load decision
# groups and selection remark.
def get_decision_groups(ap_data_sector,
                        analysis_year,
                        analysis_type,
                        season=None,
                        write_outputs=0):
    print('Creating df_decision_groups')

    df_decision_groups = performance_sql_recipes.get_decision_groups(
        ap_data_sector,
        analysis_year,
        season=season
    )

    print(df_decision_groups.shape)
    return df_decision_groups


def load_selection_remark(ap_data_sector,
                          analysis_year,
                          analysis_type,
                          write_outputs=0):
    return pd.DataFrame()



def run_corn_brazil_safrinha_ingestion(ap_data_sector,
                                       analysis_type,
                                       input_years,
                                       write_outputs=1,
                                       out_dir=None,
                                       get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )

        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_brazil_summer_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_las_ingestion(ap_data_sector,
                           analysis_type,
                           input_years,
                           write_outputs=1,
                           out_dir=None,
                           get_parents=0):

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_na_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                    get_parents=0):

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    # get data per year. Run queries per year to reduce load on denodo.
    # This slows down historic data ingestion, but we that infrequently and splitting queries by year
    # reduces the chance of the query failing
    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        #df_trial_pheno = load_trial_pheno(ap_data_sector,
        #                                  analysis_type,
        #                                  analysis_year,
        #                                  write_outputs=0
        #                                  )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        get_parents=get_parents,
                                        write_outputs=0
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year)-2) & (df_get_bebid_all['year'] < int(analysis_year)+3)
        ]
        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        #df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        #'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, #df_trial_pheno,
                df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corngrain_eame_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                 get_parents=0):

    # The problem with corngrain eame is that historic data may be under "CORN_EAME_SUMMER," and cornsilage data may also
    # be under "CORN_EAME_SUMMER"

    # for each step, run with both CORNGRAIN_EAME and CORN_EAME_SUMMER
    # after getting trial and pvs data, find entries with ______ traits, those will be CORNGRAIN. Use those entries to filter
    # each data frame...

    for analysis_year in input_years:
        print("year:", str(analysis_year))
        # get list of materials....
        # query rv_trial_pheno and rv_ap_all_pvs for distinct entry and traits from data sector and CORN_EAME_SUMMER
        # corngrain_entries will not have SDMCP, YSDMN, DIGEP, DNSCP, DTNHN. entries with these are silage
        silage_entries = []
        for data_sector in ["CORNGRAIN_EAME_SUMMER", "CORN_EAME_SUMMER", "CORNSILAGE_EAME_SUMMER"]:
            # get entries in pvs
            query_str = """
                SELECT DISTINCT entry_identifier as "entry_id"
                FROM rv_ap_all_pvs    
                WHERE ap_data_sector = {0}
                    AND TRY_TO_NUMBER(trialing_year) = {1}
                    AND analysis_type IN ('SingleExp','MultiExp','GenoPred')
                    AND loc = 'ALL'
                    AND material_type != 'untested_entry'
                    AND trait in ('SDMCP','YSDMN','DIGEP','DNSCP','DTNHN')
                        """.format("'" + data_sector + "'", str(analysis_year))

            with SnowflakeConnection() as dc:
                pvs_silage_entries = dc.get_data(query_str)

            silage_entries.append(pvs_silage_entries)
        silage_entries = pd.concat(silage_entries,axis=0).drop_duplicates()

        # get data from sector and corn eame summer
        df_trial_pheno = []
        df_pvs_no_bebid = []
        df_checks = []
        df_material_trait_data = []
        df_hybrid_rm1 = []
        df_get_bebid = []
        df_decision_groups = []
        for data_sector in [ap_data_sector, "CORN_EAME_SUMMER"]:
            df_temp = load_trial_pheno(data_sector,
                                              analysis_type,
                                              analysis_year,
                                              write_outputs=0
                                              )
            df_temp['ap_data_sector'] = ap_data_sector
            df_trial_pheno.append(df_temp)
            # get pvs data
            df_temp = load_pvs_data(data_sector,
                                            analysis_type,
                                            analysis_year,
                                            get_parents=get_parents,
                                            write_outputs=0
                                            )
            df_temp['ap_data_sector'] = ap_data_sector
            df_pvs_no_bebid.append(df_temp)

            df_temp = compute_trial_checks(data_sector,
                                             analysis_year,
                                             analysis_type,
                                             pipeline_runid='',
                                             write_outputs=0
                                             )
            df_temp['ap_data_sector'] = ap_data_sector
            df_checks.append(df_temp)

            # get RM data across multiple tables
            df_material_trait_data_temp, df_hybrid_rm1_temp = load_rm_data_across_datasets(data_sector,
                                                                                 analysis_year,
                                                                                 analysis_type,
                                                                                 write_outputs=0)

            df_material_trait_data_temp["ap_data_sector_name"] = ap_data_sector
            df_hybrid_rm1_temp["ap_data_sector_name"] = ap_data_sector
            df_material_trait_data.append(df_material_trait_data_temp)
            df_hybrid_rm1.append(df_hybrid_rm1_temp)

            # get bebid
            df_temp = get_bebid_advancement_decisions(
                data_sector,
                input_years_str=[analysis_year],
                write_outputs=0
            )
            df_temp["ap_data_sector_name"] = ap_data_sector
            df_get_bebid.append(df_temp)

            # get decision groups
            df_decision_groups.append(get_decision_groups(data_sector,
                                                     analysis_year,
                                                     analysis_type,
                                                     write_outputs=0
                                                     ))
        # concat dataframes
        print(df_trial_pheno[0].columns)
        print(df_trial_pheno[1].columns)
        df_trial_pheno = pd.concat(df_trial_pheno,axis=0).drop_duplicates()
        df_pvs_no_bebid = pd.concat(df_pvs_no_bebid,axis=0).drop_duplicates()
        df_checks = pd.concat(df_checks,axis=0).drop_duplicates()
        df_material_trait_data = pd.concat(df_material_trait_data,axis=0).drop_duplicates()
        df_hybrid_rm1 = pd.concat(df_hybrid_rm1,axis=0).drop_duplicates()
        df_get_bebid = pd.concat(df_get_bebid,axis=0).drop_duplicates()
        df_decision_groups = pd.concat(df_decision_groups,axis=0).drop_duplicates()

        # filter out materials found in silage...
        grain_entries = list(set(df_trial_pheno['entry_id']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['entry_id'], index=np.arange(0, len(grain_entries)))
        df_trial_pheno = df_trial_pheno.merge(df_grain, on='entry_id', how='inner')

        grain_entries = list(set(df_pvs_no_bebid['entry_identifier']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['entry_identifier'], index=np.arange(0, len(grain_entries)))
        df_pvs_no_bebid = df_pvs_no_bebid.merge(df_grain, on='entry_identifier', how='inner')

        grain_entries = list(set(df_checks['entry_id']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['entry_id'], index=np.arange(0, len(grain_entries)))
        df_checks = df_checks.merge(df_grain, on='entry_id', how='inner')

        grain_entries = list(set(df_material_trait_data['entry_id']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['entry_id'], index=np.arange(0, len(grain_entries)))
        df_material_trait_data = df_material_trait_data.merge(df_grain, on='entry_id', how='inner')

        grain_entries = list(set(df_hybrid_rm1['entry_id']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['entry_id'], index=np.arange(0, len(grain_entries)))
        df_hybrid_rm1 = df_hybrid_rm1.merge(df_grain, on='entry_id', how='inner')

        grain_entries = list(set(df_get_bebid['be_bid']).difference(silage_entries['entry_id']))
        df_grain = pd.DataFrame(grain_entries, columns=['be_bid'], index=np.arange(0, len(grain_entries)))
        df_get_bebid = df_get_bebid.merge(df_grain, on='be_bid', how='inner')

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups)


def run_cornsilage_eame_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                  get_parents=0):
    # The problem with corngrain eame is that historic data may be under "CORN_EAME_SUMMER," and cornsilage data may also
    # be under "CORN_EAME_SUMMER"

    # for each step, run with both CORNGRAIN_EAME and CORN_EAME_SUMMER
    # after getting trial and pvs data, find entries with ______ traits, those will be CORNGRAIN. Use those entries to filter
    # each data frame...

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get trial and pvs data, then get list of materials...
        # corngrain_entries will not have SDMCP, YSDMN, DIGEP, DNSCP, DTNHN. entries with these are silage
        silage_entries = []
        for data_sector in ["CORNGRAIN_EAME_SUMMER", "CORN_EAME_SUMMER", "CORNSILAGE_EAME_SUMMER"]:
            # get entries in pvs
            query_str = """
                SELECT DISTINCT entry_identifier as "entry_id"
                FROM rv_ap_all_pvs    
                WHERE ap_data_sector = {0}
                    AND TRY_TO_NUMBER(trialing_year) = {1}
                    AND analysis_type IN ('SingleExp','MultiExp','GenoPred')
                    AND loc = 'ALL'
                    AND material_type != 'untested_entry'
                    AND lower(market_seg) = 'all'
                    AND trait in ('SDMCP','YSDMN','DIGEP','DNSCP','DTNHN')
            """.format("'" + data_sector + "'", str(analysis_year))

            with SnowflakeConnection() as dc:
                pvs_silage_entries = dc.get_data(query_str)

            silage_entries.append(pvs_silage_entries)

        silage_entries = pd.concat(silage_entries, axis=0).drop_duplicates()

        # get data from sector and corn eame summer
        df_trial_pheno = []
        df_pvs_no_bebid = []
        df_checks = []
        df_material_trait_data = []
        df_hybrid_rm1 = []
        df_get_bebid = []
        df_decision_groups = []
        for data_sector in [ap_data_sector, "CORN_EAME_SUMMER"]:
            df_temp = load_trial_pheno(data_sector,
                                       analysis_type,
                                       analysis_year,
                                       write_outputs=0
                                       )
            df_temp['ap_data_sector'] = ap_data_sector

            df_trial_pheno.append(df_temp)
            # get pvs data
            df_temp = load_pvs_data(data_sector,
                                    analysis_type,
                                    analysis_year,
                                    get_parents=get_parents,
                                    write_outputs=0
                                    )
            df_temp['ap_data_sector'] = ap_data_sector
            df_pvs_no_bebid.append(df_temp)

            df_temp = compute_trial_checks(data_sector,
                                           analysis_year,
                                           analysis_type,
                                           pipeline_runid='',
                                           write_outputs=0
                                           )
            df_temp['ap_data_sector'] = ap_data_sector
            df_checks.append(df_temp)

            # get RM data across multiple tables
            df_material_trait_data_temp, df_hybrid_rm1_temp = load_rm_data_across_datasets(data_sector,
                                                                                           analysis_year,
                                                                                           analysis_type,
                                                                                           write_outputs=0)

            df_material_trait_data_temp["ap_data_sector_name"] = ap_data_sector
            df_hybrid_rm1_temp["ap_data_sector_name"] = ap_data_sector
            df_material_trait_data.append(df_material_trait_data_temp)
            df_hybrid_rm1.append(df_hybrid_rm1_temp)

            # get bebid
            df_temp = get_bebid_advancement_decisions(
                data_sector,
                input_years_str=[analysis_year],
                write_outputs=0
            )
            df_temp["ap_data_sector_name"] = ap_data_sector
            df_get_bebid.append(df_temp)

            # get decision groups
            df_decision_groups.append(get_decision_groups(data_sector,
                                                          analysis_year,
                                                          analysis_type,
                                                          write_outputs=0
                                                          ))
        # concat dataframes
        df_trial_pheno = pd.concat(df_trial_pheno, axis=0).drop_duplicates()
        df_pvs_no_bebid = pd.concat(df_pvs_no_bebid, axis=0).drop_duplicates()
        df_checks = pd.concat(df_checks, axis=0).drop_duplicates()
        df_material_trait_data = pd.concat(df_material_trait_data, axis=0).drop_duplicates()
        df_hybrid_rm1 = pd.concat(df_hybrid_rm1, axis=0).drop_duplicates()
        df_get_bebid = pd.concat(df_get_bebid, axis=0).drop_duplicates()
        df_decision_groups = pd.concat(df_decision_groups, axis=0).drop_duplicates()

        # filter out materials found in silage...
        df_trial_pheno = df_trial_pheno.merge(silage_entries, on='entry_id', how='inner')
        df_pvs_no_bebid = df_pvs_no_bebid.merge(silage_entries.rename(columns={'entry_id': 'entry_identifier'}),
                                                on='entry_identifier', how='inner')
        df_checks = df_checks.merge(silage_entries, on='entry_id', how='inner')
        df_hybrid_rm1 = df_hybrid_rm1.merge(silage_entries, on='entry_id', how='inner')
        df_get_bebid = df_get_bebid.merge(silage_entries.rename(columns={'entry_id': 'be_bid'}), on='be_bid',
                                          how='inner')

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups)


def run_sunflower_eame_ingestion(ap_data_sector,
                             analysis_type,
                             input_years,
                             write_outputs=1,
                             out_dir=None,
                                 get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        get_parents=get_parents,
                                        write_outputs=0
                                        )

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_soy_brazil_ingestion(ap_data_sector,
                             analysis_type,
                             input_years,
                             write_outputs=1,
                             out_dir=None,
                             get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )


        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # store list of all bebids for text trait query
        be_bids = df_get_bebid[['be_bid']].drop_duplicates()
        # get text/marker traits from the plot
        df_plot_result = load_text_traits_from_plot_data(
            be_bid_arr=be_bids['be_bid'].values,
            analysis_year=analysis_year,
            crop_name='soybean'
        )

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_plot_result,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'plot_result.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_plot_result, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_soy_las_ingestion(ap_data_sector,
                          analysis_type,
                          input_years,
                          write_outputs=1,
                          out_dir=None,
                          get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]
        # store list of all bebids for text trait query
        be_bids = df_get_bebid[['be_bid']].drop_duplicates()
        # get text/marker traits from the plot
        df_plot_result = load_text_traits_from_plot_data(
            be_bid_arr=be_bids['be_bid'].values,
            analysis_year=analysis_year,
            crop_name='soybean'
        )

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )

        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_plot_result,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'plot_result.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

        if write_outputs == 0:
            return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_plot_result, df_material_trait_data, df_hybrid_rm1,
                    df_get_bebid, df_decision_groups, df_selection_remark)


def run_soy_na_ingestion(ap_data_sector,
                          analysis_type,
                          input_years,
                          write_outputs=1,
                          out_dir=None,
                         get_parents=0):
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]
        # store list of all bebids for text trait query
        be_bids = df_get_bebid[['be_bid']].drop_duplicates()
        # get text/marker traits from the plot
        df_plot_result = load_text_traits_from_plot_data(
            be_bid_arr=be_bids['be_bid'].values,
            analysis_year=analysis_year,
            crop_name='soybean'
        )

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )

        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_plot_result,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'plot_result.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

        if write_outputs == 0:
            return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_plot_result, df_material_trait_data, df_hybrid_rm1,
                    df_get_bebid, df_decision_groups, df_selection_remark)


def run_corn_china_spring_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn china spring

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_china_summer_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn china spring

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_philippines_all_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn china spring

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_PHILIPPINES_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        #season=season,
        merge_on_loc_guid=False,
        write_outputs=0
    )
    # separate into this season and other season, this lets us track advancements within the same data sector (season)
    # and across data sectors (seasons)
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
            ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_thailand_all_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn thailand all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """

    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_THAILAND_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        #season=season,
        merge_on_loc_guid=True,
        write_outputs=0
    )
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
            ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_indonesia_all_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn indonesia all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """

    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_INDONESIA_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        #season=season,
        merge_on_loc_guid=False,
        write_outputs=0
    )
    # separate into this season and other season, this lets us track advancements within the same data sector (season)
    # and across data sectors (seasons)
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_pakistan_all_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn indonesia all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_PAKISTAN_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        merge_on_loc_guid=False,
        #season=season,
        write_outputs=0
    )
    # separate into this season and other season, this lets us track advancements within the same data sector (season)
    # and across data sectors (seasons)
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
            ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_bangladesh_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn indonesia all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_BANGLADESH_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        #season=season,
        merge_on_loc_guid=False,
        write_outputs=0
    )
    # separate into this season and other season, this lets us track advancements within the same data sector (season)
    # and across data sectors (seasons)
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_india_ingestion(ap_data_sector,
                             analysis_type,
                             input_years,
                             write_outputs=1,
                             out_dir=None,
                             get_parents=0):
    """
    function to ingest data for corn indonesia all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        ap_data_sector,
        input_years_str=input_years,
        write_outputs=0,
        merge_on_loc_guid=False
    )

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(ap_data_sector,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0
                                         )

        # get trial level data
        df_trial_pheno = load_trial_pheno(ap_data_sector,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0
                                          )

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(ap_data_sector,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(ap_data_sector,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             write_outputs=0)

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(ap_data_sector,
                                                 analysis_year,
                                                 analysis_type,
                                                 write_outputs=0
                                                 )
        # get selection remark
        df_selection_remark = load_selection_remark(ap_data_sector,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)


def run_corn_vietnam_ingestion(ap_data_sector,
                                     analysis_type,
                                     input_years,
                                     write_outputs=1,
                                     out_dir=None,
                                     get_parents=0):
    """
    function to ingest data for corn indonesia all

    inputs:
        ap_data_sector : data sector in all caps, separated by _  e.g CORN_CHINA_SPRING
        analysis_type : MultiExp, SingleExp, GenoPred, etc.
        input_years : list of years to get data for. Each year is a string because it comes from user input
        write_outputs : whether to output values, meaningless input
        out_dir: s3 bucket to output data
        get_parents : whether to get data associated with parents in addition to hybrid data.

    outputs:
        files in out_dir related to check information, trialing data, genotypic and AAE predictions, relative maturity,
        advancement decisions
    """
    # parse data sector for season. This parameter is useful in load_pvs_data
    season_orig = ap_data_sector.split('_')[-1]
    if season_orig in season_mapper.keys():
        season = season_mapper[season_orig]
    else:
        season = season_orig

    print(season)
    data_sector_use = 'CORN_VIETNAM_ALL'

    # get be_bids per stage across many years once. Split by year when outputting to conform with previous style
    df_get_bebid_all = get_bebid_advancement_decisions(
        data_sector_use,
        input_years_str=input_years,
        #season=season,
        merge_on_loc_guid=False,
        write_outputs=0
    )
    # separate into this season and other season, this lets us track advancements within the same data sector (season)
    # and across data sectors (seasons)
    df_get_bebid_all.loc[df_get_bebid_all['season'] == season, 'ap_data_sector_name'] = ap_data_sector

    for analysis_year in input_years:
        print("year:", str(analysis_year))

        # get checks
        df_checks = compute_trial_checks(data_sector_use,
                                         analysis_year,
                                         analysis_type,
                                         pipeline_runid='',
                                         write_outputs=0,
                                         season=season
                                         )
        df_checks['ap_data_sector'] = ap_data_sector

        # get trial level data
        df_trial_pheno = load_trial_pheno(data_sector_use,
                                          analysis_type,
                                          analysis_year,
                                          write_outputs=0,
                                          season=season
                                          )
        df_trial_pheno['ap_data_sector'] = ap_data_sector

        # get pvs data
        df_pvs_no_bebid = load_pvs_data(data_sector_use,
                                        analysis_type,
                                        analysis_year,
                                        write_outputs=0,
                                        get_parents=get_parents,
                                        backup_data_sector='CORNGRAIN_APAC_1',
                                        season=season
                                        )
        df_pvs_no_bebid['ap_data_sector'] = ap_data_sector

        # get RM data across multiple tables
        df_material_trait_data, df_hybrid_rm1 = load_rm_data_across_datasets(data_sector_use,
                                                                             analysis_year,
                                                                             analysis_type,
                                                                             season=season,
                                                                             write_outputs=0)
        df_material_trait_data['ap_data_sector_name'] = ap_data_sector
        df_hybrid_rm1['ap_data_sector_name'] = ap_data_sector

        # split df_get_bebid_all into components for each specific analysis year
        df_get_bebid = df_get_bebid_all[
            (df_get_bebid_all['year'] >= int(analysis_year) - 2) & (df_get_bebid_all['year'] < int(analysis_year) + 3)
        ]

        # get decision groups
        df_decision_groups = get_decision_groups(data_sector_use,
                                                 analysis_year,
                                                 analysis_type,
                                                 season=season,
                                                 write_outputs=0
                                                 )
        # no ap data sector column, don't overwrite

        # get selection remark
        df_selection_remark = load_selection_remark(data_sector_use,
                                                    analysis_year,
                                                    analysis_type,
                                                    write_outputs=0
                                                    )
        df_selection_remark['ap_data_sector'] = ap_data_sector

        if write_outputs == 1:
            prefix = 'data_' + str(analysis_year) + '_'
            for df, fname in zip(
                    [
                        df_checks,
                        df_trial_pheno,
                        df_pvs_no_bebid,
                        df_material_trait_data,
                        df_hybrid_rm1,
                        df_get_bebid,
                        df_decision_groups,
                        df_selection_remark
                    ],
                    [
                        'checks.csv',
                        'trial_pheno.csv',
                        'pvs_no_bebid.csv',
                        'material_trait_data.csv',
                        'hybrid_rm1.csv',
                        'get_bebid.csv',
                        'decision_groups.csv',
                        'selection_remark.csv'
                    ]
            ):
                df.to_csv(os.path.join(out_dir, prefix + fname), index=False)

    if write_outputs == 0:
        return (df_checks, df_trial_pheno, df_pvs_no_bebid, df_material_trait_data, df_hybrid_rm1, df_get_bebid,
                df_decision_groups, df_selection_remark)
