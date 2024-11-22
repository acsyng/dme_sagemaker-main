import os

import pandas as pd, numpy as np

from libs.performance_lib import performance_helper
from libs.performance_lib import performance_sql_recipes
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection

def preprocess_trial_pheno(folder, yr, df_cpifl_grouped, get_parents=0):

    # meta columns to output
    common_melt_cols = ['ap_data_sector', 'analysis_year', 'source_id', 'trial_id', 'entry_identifier',
                        'market_seg', 'decision_group_rm', 'et_value',
                        'material_type', 'trait']

    # make a list with entry_id instead of entry_identifier
    common_melt_cols_entry_id = common_melt_cols.copy()
    common_melt_cols_entry_id.remove('entry_identifier')
    common_melt_cols_entry_id.append('entry_id')

    # load trial pheno
    if not os.path.exists(os.path.join(folder, 'data_' + yr + '_trial_pheno.csv')):
        return pd.DataFrame()

    df_trial_pheno = pd.read_csv(os.path.join(folder, 'data_' + yr + '_trial_pheno.csv'))

    # remove columns like et_value, plot_barcode, etc.
    # this could be done in the sql query, which would be faster and less memory intensive.
    df_trial_pheno = df_trial_pheno[[
        'ap_data_sector',
        'analysis_year',
        'trial_id',
        'entry_id',
        'source_id',
        'market_segment',
        'trait',
        'maturity_group',
        'dme_chkfl',
        'irrigation',
        'et_value',
        'result_numeric_value'
    ]]

    # merge in check flags per trait, fill missing with 0 (assume not a check if not specified)
    df_trial_pheno = df_trial_pheno.merge(
        df_cpifl_grouped,
        on=['ap_data_sector', 'analysis_year', 'source_id', 'entry_id'],
        how='left'
    ).rename(columns={'market_segment': 'market_seg'})

    df_trial_pheno = performance_helper.get_chkfl(df_trial_pheno)

    df_trial_pheno[['cpifl', 'chkfl']] = df_trial_pheno[['cpifl', 'chkfl']].fillna(value=0)
    df_trial_pheno['material_type'] = df_trial_pheno['material_type'].fillna(value='entry')

    # get trait values relative to check and trial mean
    # if check mean does not exist, use trial mean
    trial_group_cols = ['ap_data_sector', 'analysis_year', 'trial_id', 'trait']
    score_col = 'result_numeric_value'
    keep_cols = trial_group_cols.copy()
    keep_cols.append(score_col)

    # check mean for each trait
    df_trial_pheno_checks_mean = df_trial_pheno[df_trial_pheno['chkfl'] == 1][keep_cols].groupby(
        by=trial_group_cols
    ).mean().reset_index()  # chkfl is per trait
    df_trial_pheno_checks_mean = df_trial_pheno_checks_mean.rename(
        columns={'result_numeric_value': 'trial_mean_value'}
    )

    # trial mean for each trait
    df_trial_pheno_mean = df_trial_pheno[keep_cols].groupby(by=trial_group_cols).mean().reset_index()
    df_trial_pheno_mean = df_trial_pheno_mean.rename(columns={'result_numeric_value': 'trial_mean_value'})

    # choose check mean, then trial mean if check mean does not exist
    # to do this, stack two dataframes, groupby and choose the first value
    df_trial_pheno_mean = pd.concat((df_trial_pheno_checks_mean, df_trial_pheno_mean), axis=0)
    df_trial_pheno_mean = df_trial_pheno_mean.groupby(by=trial_group_cols).first().reset_index()

    # merge in check or trial mean, then subtract and drop trial mean
    df_trial_pheno = df_trial_pheno.merge(
        df_trial_pheno_mean, on=['ap_data_sector', 'analysis_year', 'trial_id', 'trait']
    )
    # find raw difference and percentage, as breeders may look at either
    df_trial_pheno['result_diff'] = df_trial_pheno['result_numeric_value'] - df_trial_pheno['trial_mean_value']
    df_trial_pheno['result_perc'] = np.divide(df_trial_pheno['result_numeric_value'],df_trial_pheno['trial_mean_value'])

    df_trial_pheno = df_trial_pheno.drop(columns=['trial_mean_value']).rename(
        columns={'maturity_group': 'decision_group_rm'}
    )

    # there are nan's in the stage, decision_group_rm, technology cols
    # fill, do group by, replace with nan's. nan's mess up groupby
    df_trial_pheno['decision_group_rm'] = df_trial_pheno['decision_group_rm'].fillna(value=-123)

    # convert hybrid to parental information if requested
    if get_parents == 1 and df_trial_pheno.shape[0] > 0:
        # query rv_corn_material_tester_adapt for parental information
        entry_ids = pd.unique(df_trial_pheno['entry_id'])
        if df_trial_pheno.shape[0] > 0 and "CORN" in df_trial_pheno['ap_data_sector'].iloc[0]:
            is_corn=1
        else:
            is_corn=0

        df_par = performance_sql_recipes.get_ancestors(entry_ids,is_corn=is_corn)

        # assign hybrid pheno observations to parents
        df_p = df_trial_pheno.merge(df_par, on='entry_id', how='inner')
        df_donor = df_p[df_p['pool1_be_bid'].notna()].drop(
            columns=['entry_id', 'pool2_be_bid']
        ).rename(
            columns={'pool1_be_bid': 'entry_id'}
        )
        df_donor['material_type']='pool1'
        df_rec = df_p[df_p['pool2_be_bid'].notna()].drop(
            columns=['entry_id', 'pool1_be_bid']
        ).rename(
            columns={'pool2_be_bid': 'entry_id'}
        )
        df_rec['material_type']='pool2'

        # append parental data to trial pheno
        df_trial_pheno = pd.concat((df_trial_pheno,df_donor, df_rec), axis=0)

    print(df_trial_pheno.shape)

    # group by with aggregations listed below
    agg_dict = {
        'result_numeric_value': 'mean',
        'result_diff': 'mean',
        'result_perc': 'mean',
        'cpifl': 'max',
        'chkfl': 'max'
    }

    df_trial_pheno_grouped = df_trial_pheno.groupby(
        by=common_melt_cols_entry_id
    ).agg(agg_dict).reset_index()

    # with multiple aggregations, fix columns. Don't append agg type for columns with a single aggregation
    new_cols = []
    for col in df_trial_pheno_grouped.columns:
        if isinstance(col, list):
            if col[0] in agg_dict and isinstance(agg_dict[col[0]], list) and len(agg_dict[col[0]]) > 1:
                new_cols.append(col[0] + '_' + col[1])
            else:
                new_cols.append(col[0])
        else:
            new_cols.append(col)
    df_trial_pheno_grouped.columns = new_cols

    # rename result_diff_mean and result_numeric_value_mean
    df_trial_pheno_grouped = df_trial_pheno_grouped.rename(
        columns={
            'result_diff': 'result_diff',
            'result_numeric_value': 'result',
            'result_perc': 'result_perc'
        }
    )

    # replace temp nan-vals with nan's
    for col, nan_val in zip(['decision_group_rm'], [-123]):
        df_trial_pheno_grouped[col][df_trial_pheno_grouped[col] == nan_val] = np.nan

    # rename entry_id to entry_identifier to keep consistent with other tables
    df_trial_pheno_grouped = df_trial_pheno_grouped.rename(columns={'entry_id': 'entry_identifier'})

    # put in dummy market segment column...may need to change this later
    df_trial_pheno_grouped['market_seg'] = 'all'

    # melt df_trial_pheno_grouped into a tall format
    df_trial_pheno_melt = pd.melt(
        df_trial_pheno_grouped,
        id_vars=common_melt_cols,
        var_name='var',
        value_name='value',
        value_vars=['result', 'result_diff', 'result_perc', 'chkfl']
    )

    # move trait into var name, then drop trait
    df_trial_pheno_melt['var'] = df_trial_pheno_melt['var'] + '_' + df_trial_pheno_melt['trait']
    df_trial_pheno_melt = df_trial_pheno_melt.drop(columns='trait')

    # get cpifl for each entry, not trait-specific
    common_melt_cols_no_trait = common_melt_cols.copy()
    common_melt_cols_no_trait.remove('trait')
    common_melt_cols_no_trait_cpifl = common_melt_cols_no_trait.copy()
    common_melt_cols_no_trait_cpifl.append('cpifl')

    df_trial_pheno_cpifl_melt = pd.melt(
        df_trial_pheno_grouped[common_melt_cols_no_trait_cpifl].groupby(
            by=common_melt_cols_no_trait
        ).max().reset_index(),
        id_vars=common_melt_cols_no_trait,
        var_name='var',
        value_name='value',
        value_vars=['cpifl']
    )

    df_trial_pheno_melt = pd.concat((df_trial_pheno_melt, df_trial_pheno_cpifl_melt), axis=0)

    return df_trial_pheno_melt


# STEP 3: load geno prediction data
def load_and_process_pvs_data(folder, yr,  df_cpifl_grouped):
    # analysis year is a str
    # define common output columns
    # get mapper between line codes (pvs data) and be bids
    # define common output columns
    common_melt_cols = ['ap_data_sector', 'analysis_year', 'entry_identifier', 'source_id',
                        'market_seg', 'stage', 'decision_group_rm', 'technology',
                        'material_type', 'trait']

    # drop market seg from cpifl table if it is here. There are discrepancies between cpifl and pvs for market seg
    if "market_seg" in df_cpifl_grouped.columns:
        df_cpifl_grouped = df_cpifl_grouped.drop(columns='market_seg')

    # load in pvs data
    df_pvs_no_bebid = pd.read_csv(os.path.join(folder, 'data_' + yr + '_pvs_no_bebid.csv'))

    # if decision_group_rm follows the ~100 scale, convert to maturity group scale
    if np.nanpercentile(df_pvs_no_bebid['decision_group_rm'], 50) > 79:
        df_pvs_no_bebid['decision_group_rm'] = (df_pvs_no_bebid['decision_group_rm'] - 80) / 5

        # for Soy, pvs data may not use be_bid...mapper maps between linecode, highname, abbr_code and be_bid
    if df_pvs_no_bebid.shape[0] > 0 and 'SOY' in df_pvs_no_bebid['ap_data_sector'].iloc[0]:
        # get mapper between line codes (pvs data) and be bids
        df_material_mapper = performance_sql_recipes.get_material_mapper(
            pd.unique(df_pvs_no_bebid['entry_identifier']),
            crop_name='soybean'
        )

        df_mapper_list = []
        for map_col in ['abbr_code', 'highname', 'line_code', 'be_bid']:
            if map_col == 'be_bid':
                df_map = df_material_mapper[['be_bid']].dropna().drop_duplicates()
                df_map['entry_identifier'] = df_map['be_bid']
            else:
                df_map = df_material_mapper[['be_bid', map_col]].dropna().drop_duplicates()
                df_map = df_map.rename(columns={map_col: 'entry_identifier'})
            df_mapper_list.append(df_map)

        df_mapper = pd.concat(df_mapper_list, axis=0).groupby(by=['entry_identifier']).first().reset_index()

        # get be bid from mapper, drop extra identifier column
        df_pvs_data = df_pvs_no_bebid.merge(df_mapper, how='inner', on=['entry_identifier'])
        df_pvs_data = df_pvs_data.drop(columns=['entry_identifier']).rename(
            columns={'be_bid': 'entry_identifier'}).drop_duplicates()

    else:
        df_pvs_data = df_pvs_no_bebid

    if df_pvs_data.shape[0] > 0:
        # check for duplicated bebids
        alias_meta_cols = ['ap_data_sector', 'analysis_year', 'entry_identifier', 'source_id',
                        'market_seg', 'stage', 'decision_group_rm', 'technology',
                        'material_type', 'trait']

        df_pvs_data = get_bebid_aliases(df_pvs_data, be_bid_col='entry_identifier',meta_cols=alias_meta_cols)

    # merge check information into pvs_data
    df_pvs_data_cpifl = df_pvs_data.merge(
        df_cpifl_grouped,
        how='left',
        left_on=['ap_data_sector', 'analysis_type', 'analysis_year', 'entry_identifier', 'source_id', 'material_type'],
        right_on=['ap_data_sector', 'analysis_type', 'analysis_year', 'entry_id', 'source_id', 'material_type']
    )

    # make chkfl column, which compresses the cperf,cagrf etc. columns into one
    df_pvs_data_cpifl = performance_helper.get_chkfl(df_pvs_data_cpifl)
    df_pvs_data_cpifl = df_pvs_data_cpifl.drop(
        columns=['dme_chkfl', 'dme_reg_x', 'dme_reg_y', 'dme_rm_est', 'entry_id'])

    # stack trial_pheno and pvs_data
    # drop some columns that are not common between pvs and trial_pheno, make columns that aren't common
    df_pvs_data_pre_melt = df_pvs_data_cpifl.drop(
        columns=['analysis_type'])
    df_pvs_data_pre_melt['irrigation'] = 'NA'

    # pivot to tall format (use melt function), then stack data.
    # do marker data after this as we can take an average over all of these numeric traits
    # can't take average over text traits
    df_pvs_data_melt = pd.melt(
        df_pvs_data_pre_melt,
        id_vars=common_melt_cols,
        var_name='var',
        value_vars=['count', 'prediction','prediction_perc', 'stderr']
    )

    if df_pvs_data_melt.shape[0] > 0:
        # move trait into var name, then drop trait
        df_pvs_data_melt['var'] = df_pvs_data_melt['var'] + '_' + df_pvs_data_melt['trait']
        df_pvs_data_melt = df_pvs_data_melt.drop(columns='trait')

        # stage comes in as strings and integers. Convert to floats because some regions have decimal point stages
        df_pvs_data_melt['stage'] = df_pvs_data_melt['stage'].astype(float)

        df_pvs_data_melt = df_pvs_data_melt.dropna(subset=['value'])
    else:
        df_pvs_data_melt = df_pvs_data_melt.drop(columns='trait')

    return df_pvs_data_melt


# Get text/marker traits from the plot
def load_text_traits_from_plot_data(folder, yr):
    if not os.path.exists(os.path.join(folder, 'data_'+yr+'_plot_result.csv')):
        return pd.DataFrame()

    df_plot_result = pd.read_csv(os.path.join(folder, 'data_'+yr+'_plot_result.csv'))

    if df_plot_result.shape[0] > 0:
        # convert material id to bebid, get experiment id for df_plot_result
        df_plot_result = df_plot_result[df_plot_result['alpha_value'].notna()]
        df_plot_result = df_plot_result[['trait', 'year', 'entry_id', 'alpha_value']]
        df_plot_result = df_plot_result.groupby(by=['trait', 'year', 'entry_id']).first().reset_index().rename(
            columns={'year': 'analysis_year','entry_id': 'entry_identifier'}
        )

        df_plot_result = df_plot_result.rename(columns={'trait': 'var', 'alpha_value': 'alpha_value'})
        df_plot_result['var'] = df_plot_result['var'].str.lower()

    return df_plot_result


# get RM data across multiple tables
def load_rm_data_across_datasets(ap_data_sector, folder, input_year):
    # get RM data for hybrids across 2 tables (rv_variety_trait_data, rv_material_trait_data)
    # For corn, get RM for parents by averaging over offspring RM
    # need rv_variety_entry_data to map material guids to be bids
    # run a number of SQL recipes to get initial datasets

    # get hybrid rm data from 3 tables, stack on top of each other
    #df_material_trait_data = pd.read_csv(os.path.join(folder, 'data_'+input_year+'_material_trait_data.csv'))

    df_hybrid_rm1 = pd.read_csv(os.path.join(folder, 'data_'+input_year+'_hybrid_rm1.csv'))

    df_hybrid_rm1 = df_hybrid_rm1[['analysis_year', 'ap_data_sector_name', 'entry_id', 'number_value']].rename(
        columns={'ap_data_sector_name': 'ap_data_sector', 'number_value': 'rm_estimate'}
    ).drop_duplicates()

    # get rm from variety entry and variety trait tables, merge together to get correct identifier
    df_variety_entry_data = performance_sql_recipes.get_variety_entry_data(
        ap_data_sector=ap_data_sector,
        analysis_year=input_year
    )

    #df_variety_trait_data = performance_sql_recipes.get_variety_trait_data(ap_data_sector=ap_data_sector)
    df_variety_trait_data = pd.DataFrame(
        columns=['code','name','crop_guid','genetic_affiliation_guid','number_value']
        )

    df_variety_entry_trait = df_variety_entry_data.merge(
        df_variety_trait_data,
        on=['genetic_affiliation_guid', 'crop_guid'],
        how='inner').drop_duplicates()

    df_hybrid_rm2 = performance_helper.get_hybrid_rm(
        df_variety_entry_trait,
        ap_data_sector=ap_data_sector,
        analysis_year=input_year
    )

    df_hybrid_rm2 = df_hybrid_rm2[['analysis_year', 'ap_data_sector_name', 'entry_id', 'number_value']].rename(
        columns={
            'ap_data_sector_name': 'ap_data_sector', 'number_value': 'rm_estimate'}
    ).drop_duplicates()

    ############ TO BE IMPLEMENTED
    # get rm from postgres table

    # concat RMs across source tables
    df_hybrid_rm = pd.concat((df_hybrid_rm1, df_hybrid_rm2), axis=0).drop_duplicates()

    if df_hybrid_rm.shape[0] > 0:
        df_hybrid_rm = df_hybrid_rm.groupby(
            by=['ap_data_sector', 'analysis_year', 'entry_id']).mean().reset_index()

    # merge RM data using a small version of df_merged because RM is not trait specific.
    if df_hybrid_rm.shape[0] > 0:
        df_rm_data = df_hybrid_rm.rename(columns={'entry_id': 'entry_identifier'})

        # put df_rm_data in a tall format so that its format matches all other files
        df_rm_melt = pd.melt(
            df_rm_data,
            id_vars=['ap_data_sector', 'analysis_year', 'entry_identifier'],
            var_name='var',
            value_vars=['rm_estimate']
        ).dropna(subset=['value'])
    else:  # empty dataframe
        df_rm_melt = pd.DataFrame(
            columns=['ap_data_sector', 'analysis_year', 'entry_identifier', 'var', 'rm_estimate'])

    return df_rm_melt


# infer historical advancement decisions by looking at the stage materials were planted in each year
def stack_bebids(df_in):
    df_list = []
    for col in ['be_bid', 'fp_be_bid', 'mp_be_bid']:
        df_temp = df_in[['ap_data_sector_name', 'year', 'season', col, 'stage_lid']].rename(
            columns={col: 'be_bid','stage_lid':'stage'})
        df_temp = df_temp[(df_temp['be_bid'].notna()) & (df_temp['be_bid'] != '')]
        if col == 'be_bid':
            df_temp['material_type'] = 'entry'
        else:
            df_temp['material_type'] = 'parent'
        df_list.append(df_temp)

    df_stack = pd.concat(df_list, axis=0)
    return df_stack


def get_bebid_aliases(df_in, be_bid_col='be_bid',meta_cols=['ap_data_sector_name', 'season', 'be_bid', 'material_type']):
    # we need to create an alias map between be_bids that share the same abbr code....
    abbr_code_list = []
    step = 1000
    bebids = df_in[be_bid_col].drop_duplicates()
    with SnowflakeConnection() as dc:
        for i in range(0, bebids.shape[0], step):
            if i + step >= bebids.shape[0]:
                bebids_step = bebids.iloc[i:]
            else:
                bebids_step = bebids.iloc[i:i + step]

            bebids_query = performance_helper.make_search_str(bebids_step)

            query_str = """
                select distinct be_bid as "be_bid", abbr_code as "abbr_code"
                from RV_BB_MATERIAL_DAAS 
                where be_bid in {0}
            """.format(bebids_query)
            abbr_code_list.append(dc.get_data(query_str))

    df_abbr = pd.concat(abbr_code_list, axis=0)

    # get bebid map
    df_bebid_map = df_abbr.merge(df_abbr, on=['abbr_code'], how='inner', suffixes=('1', '2'))
    df_bebid_map = df_bebid_map[df_bebid_map['be_bid1'] != df_bebid_map['be_bid2']]

    # convert bebids if same abbr code
    df_out = df_in.copy()
    for check_col, other_col in zip(['be_bid1', 'be_bid2'], ['be_bid2', 'be_bid1']):
        df_temp = df_out.merge(df_bebid_map[[check_col, other_col]], left_on=[be_bid_col], right_on=[check_col],
                                     how='left')
        df_temp = df_temp[df_temp[other_col].notna()]
        df_temp = df_temp.drop(columns=[be_bid_col, check_col]).rename(columns={other_col: be_bid_col})

        df_out = pd.concat((df_out, df_temp), axis=0).groupby(
            by=meta_cols).max().reset_index()
    df_out = df_out.drop_duplicates()

    return df_out


def get_bebid_advancement_decisions(folder, input_year):
    # get be_bid, and parental be_bids, plus stage tested from denodo
    df = pd.read_csv(os.path.join(folder, 'data_'+input_year+'_get_bebid.csv'))
    df['year'] = df['year'].astype(int)
    if np.sum(df['year'] == int(input_year)) == 0:
        return pd.DataFrame()

    # stack be_bid, fp_be_bid, mp_be_bid, create material_type column. This lets us do both entries and parents simultaneously
    df_stack = stack_bebids(df)
    df_stack = df_stack.drop_duplicates()

    yrs = np.arange(
        pd.unique(df_stack['year']).min().astype(int),
        pd.unique(df_stack['year']).max().astype(int)+1
    )

    # pivot dataframe by year and season
    df_stack_piv = df_stack.pivot_table(values=['stage'],
                                        index=['ap_data_sector_name', 'season', 'be_bid',
                                               'material_type'],
                                        columns=['year'],
                                        aggfunc='max').reset_index()
    # update column names
    df_stack_piv.columns = df_stack_piv.columns.map(
        lambda x: '_'.join([str(i) for i in x]).replace('value', '').strip('_'))

    # check for bebid aliases via abbr_code
    df_stack_piv = get_bebid_aliases(df_stack_piv)

    # rename columns
    stage_cols = []
    for col in df_stack_piv.columns:
        if 'stage_' in col:
            stage_cols.append(col)

    # get stage max column
    df_stack_piv['stage_max'] = df_stack_piv[stage_cols].max(axis=1)

    # make sure stage columns exist
    for yr in range(yrs[0],yrs[-1]+1):
        if 'stage_'+str(yr) not in df_stack_piv.columns:
            df_stack_piv['stage_'+str(yr)] = np.nan

    # clean year-stage info
    for yr in yrs[1:]:
        # if current year is null
        # ->if year prior is not null
        curr_stage_col = 'stage_' + str(yr)
        prev_stage_col = 'stage_' + str(yr - 1)
        future_stage_cols = [
            'stage_' + str(yr_next) for yr_next
            in range(yr + 1, np.minimum(yr + 5, yrs[-1]) + 1) if 'stage_'+str(yr_next) in df_stack_piv.columns
        ]

        # ->if all of future years are null, set as = 13
        # else = stage from prev year
        adj_mask = (df_stack_piv[curr_stage_col].isna()) & (df_stack_piv[prev_stage_col].notna())
        future_mask = np.all(df_stack_piv[future_stage_cols].isna(), axis=1)

        df_stack_piv[curr_stage_col][(adj_mask) & (future_mask)] = 13
        df_stack_piv[curr_stage_col][(adj_mask) & (future_mask == False)] = df_stack_piv[prev_stage_col][
            (adj_mask) & (future_mask == False)]

    # make stage achieved columns, fill with 0 to start
    stages = [1, 2, 3, 4, 5, 6, 7]

    for stage in stages:
        if stage < 7:
            stage_name = 'stage_' + str(stage)
        else:
            stage_name = 'stage_chk'
        df_stack_piv[stage_name] = np.any(df_stack_piv[stage_cols].values == stage, axis=1).astype(int)

    return df_stack_piv


def load_bebid_stage(df_stage_piv,ap_data_sector,input_year):
    # merge in stage information from past and future years
    # setup variables for extracting appropriate stage
    stage_str_list = ['prev_stage', 'current_stage', 'next_stage', 'third_stage']
    yr_offset_list = [-1, 0, 1, 2]

    # rename columns and drop materials from other data sectors
    df_stage_piv = df_stage_piv.rename(
        columns={'ap_data_sector_name': 'ap_data_sector',
                 'material_type': 'material_type_simple',
                 'be_bid': 'entry_identifier'}
    )

    # rename stage columns for the previous, current, next and next-next year
    stages_to_get = []
    for i_yr in range(len(stage_str_list)):
        year_to_get = int(input_year) + yr_offset_list[i_yr]
        stage_str = 'stage_' + str(year_to_get)
        if stage_str in df_stage_piv.columns:
            df_stage_piv = df_stage_piv.rename(columns={stage_str: stage_str_list[i_yr]})
            stages_to_get.append(stage_str_list[i_yr])

    # only keep meta columns and columns corresponding to years around analysis_year
    cols_to_keep = ['ap_data_sector', 'material_type_simple', 'season', 'entry_identifier']
    if len(stages_to_get) > 0:
        cols_to_keep.extend(stages_to_get)
    df_stage_piv = df_stage_piv[cols_to_keep]

    # create analysis year columns
    df_stage_piv['analysis_year'] = int(input_year)

    # make empty stage columns in df_merged if they don't exist
    # for the current year, we may not see next and third year data
    for stage_str in stage_str_list:
        if stage_str not in df_stage_piv.columns:
            df_stage_piv[stage_str] = 13

    # get advancement decisions by comparing stage information across years
    # check advancement across data sectors:
    season_order = np.array(['WNTR', 'DRY', 'SPR', 'SUMR', 'WET', 'FALL', 'AUTM'])  # for advancements within a year
    seasons_data_sector = pd.unique(df_stage_piv[df_stage_piv['ap_data_sector'] == ap_data_sector]['season'].values)

    if seasons_data_sector.shape[0] > 0:
        max_season_idx = np.max(
            np.nonzero(np.any(season_order.reshape((-1, 1)) == seasons_data_sector.reshape((1, -1)), axis=1)))
    else:
        max_season_idx = 100

    df_stage_piv['after_current_season'] = df_stage_piv['season'].apply(
        lambda x: np.max(np.nonzero(x == season_order)) > max_season_idx)

    df_stage_piv_curr = df_stage_piv[df_stage_piv['ap_data_sector'] == ap_data_sector]
    df_stage_piv_other = df_stage_piv[df_stage_piv['ap_data_sector'] != ap_data_sector]

    df_stage_piv_all = df_stage_piv_curr.merge(df_stage_piv_other[
                                                   ['entry_identifier', 'material_type_simple', 'current_stage',
                                                    'next_stage', 'after_current_season']],
                                               on=['entry_identifier', 'material_type_simple'], suffixes=('', '_other'),
                                               how='left')
    df_stage_piv_all = df_stage_piv_all.groupby(
        by=['ap_data_sector', 'material_type_simple', 'season', 'entry_identifier']).max().reset_index()

    # only keep current data sector
    df_stage_piv_all = df_stage_piv_all[df_stage_piv_all['ap_data_sector'] == ap_data_sector]

    # drop materials with a 13 in current_stage
    df_stage_piv_all = df_stage_piv_all[df_stage_piv_all['current_stage'] < 13]

    # melt data
    df_adv_dec_melt = pd.melt(
        df_stage_piv_all,
        id_vars=['ap_data_sector', 'material_type_simple', 'entry_identifier'],
        var_name='var',
        value_vars=['prev_stage', 'current_stage', 'next_stage', 'third_stage',
                    'after_current_season_other', 'current_stage_other', 'next_stage_other']
    ).dropna(subset=['value'])


    return df_adv_dec_melt


def load_ingested_data(ap_data_sector,
                    input_years,
                    pipeline_runid,
                    args,
                    get_parents=0,
                    is_infer=0,
                    write_outputs=1):

    # for each year, load in ingested data, perform some preprocessing, save locally
    # input years is a list of strings
    if not isinstance(input_years, list):
        input_years = [input_years]

    out_dir = args.output_load_data_ingestion_folder

    for yr in input_years:
        print("loading data for " + yr)
        # get checks, aggregate across traits to cpifl
        df_checks = pd.read_csv(os.path.join(
            args.s3_input_data_ingestion_folder,
            'data_' + yr + '_checks.csv'
        ))

        df_checks[['cpifl', 'cperf', 'cagrf', 'cmatf', 'cregf', 'crtnf']] = df_checks[
            ['cpifl', 'cperf', 'cagrf', 'cmatf', 'cregf', 'crtnf']].apply(
            lambda x: ((x / df_checks['result_count']) > 0.25).astype(int)
        )

        df_checks = df_checks.drop(columns=['result_count', 'untested_entry_display'])

        # aggregate check information up to the material per source id level.
        cpifl_group_cols = [
            'ap_data_sector', 'analysis_type', 'analysis_year',
            'source_id', 'entry_id', 'material_type'
        ]

        cpifl_info_cols = ['cpifl', 'cperf', 'cagrf', 'cmatf', 'cregf', 'crtnf']
        cpifl_keep_cols = cpifl_group_cols.copy() + cpifl_info_cols
        df_cpifl_grouped = df_checks[cpifl_keep_cols].groupby(by=cpifl_group_cols).max().reset_index()

        df_checks = pd.DataFrame()

        # preprocess trial pheno
        df_trial_pheno_grouped = preprocess_trial_pheno(
            args.s3_input_data_ingestion_folder,
            yr,
            df_cpifl_grouped,
            get_parents=get_parents
        )
        # save locally
        if df_trial_pheno_grouped.shape[0]>0:
            df_trial_pheno_grouped.to_csv(os.path.join(out_dir, 'data_'+yr+'_trial_pheno_tall.csv'), index=False)
        df_trial_pheno_grouped = pd.DataFrame()

        # get genetic prediction data
        df_pvs_data_melt = load_and_process_pvs_data(
            folder=args.s3_input_data_ingestion_folder,
            yr=yr,
            df_cpifl_grouped=df_cpifl_grouped
        )
        if df_pvs_data_melt.shape[0] > 0:
            df_pvs_data_melt.to_csv(os.path.join(out_dir, 'data_'+yr+'_pvs_data_tall.csv'), index=False)
        df_pvs_data_melt = pd.DataFrame()

        # get trait/marker data
        df_plot_result = load_text_traits_from_plot_data(
            folder=args.s3_input_data_ingestion_folder,
            yr=yr,
        )
        if df_plot_result.shape[0] > 0:
            df_plot_result.to_csv(os.path.join(out_dir, 'data_'+yr+'_plot_result_tall.csv'), index=False)
            marker_traits = list(pd.unique(df_plot_result['var']))
            for trait_remove in ['notet','pltqt']:
                if trait_remove in marker_traits:
                    marker_traits.remove(trait_remove)
        else:
            marker_traits = []

        df_plot_result = pd.DataFrame()
        # get RM data
        df_rm = load_rm_data_across_datasets(
            ap_data_sector=ap_data_sector,
            folder=args.s3_input_data_ingestion_folder,
            input_year=yr,
        )
        if df_rm.shape[0] > 0:
            df_rm.to_csv(os.path.join(out_dir, 'data_'+yr+'_RM_tall.csv'), index=False)
        df_rm = pd.DataFrame()

        if is_infer == 0:
            # load historical advancement decisions
            df_stage_piv = get_bebid_advancement_decisions(
                folder=args.s3_input_data_ingestion_folder,
                input_year=yr,
            )
            if df_stage_piv.shape[0] > 0: # if there is data, process it
                df_adv_dec = load_bebid_stage(
                    df_stage_piv,
                    ap_data_sector=ap_data_sector,
                    input_year=yr
                )
                df_stage_piv = pd.DataFrame()
                if df_adv_dec.shape[0] > 0:
                    df_adv_dec.to_csv(os.path.join(out_dir, 'data_'+yr+'_decisions_tall.csv'), index=False)
                df_adv_dec = pd.DataFrame()

        # Load decision groups if applicable.
        decision_groups_fname = os.path.join(
            args.s3_input_data_ingestion_folder,
            'data_'+yr+'_decision_groups.csv',
        )
        if performance_helper.check_if_file_exists(decision_groups_fname):
            df_decision_groups = pd.read_csv(decision_groups_fname)

        else:
            df_decision_groups = pd.DataFrame()
        df_decision_groups.to_csv(os.path.join(out_dir, 'data_'+yr+'_decision_groups_tall.csv'))

    return marker_traits