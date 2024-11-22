"""
SQL queries for portfolio
"""

# import packages
import os
import sys
import pandas as pd
import numpy as np

from libs.mio.mio_connection import MioConnection
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.postgres.postgres_connection import PostgresConnection
from portfolio_libs import portfolio_lib


def get_mio_sales_data(crop_name, min_year, max_year=40000):
    """
    SQL query to get sales data from SAP using mio databases.

    Currently, no way to provide region (data sector) information to query, will grab
    all data for a crop. Region filter can be done after using geo_level#

    Inputs:
        crop_name : "soybean" or "SOY", "corn" or "CRN"
        min_year : earliest year to grab data, inclusive
        max_year : latest year to grab data, exclusive
            Years are calendar year, which may not match plant/harvest season.
            If want data from a single year, do min_year = year, max_year = year+1
    Outputs:
        dataframe containing sales data cross regions for the inputted crop

    """

    if crop_name == 'soybean':
        sub_crop = 'SOY'
    elif crop_name == 'corn':
        sub_crop = 'CRN'

    query_str = """
        select
            sc.*,
            cc.total_costs,
            cc.total_fixed_costs,
            cc.total_variable_costs
        from (
            select
                m.subcrop as crop,
                s.year,
                s.month,
                m.material_id,
                m.material_number,
                m.variety_number,
                m.variety_name,
                m.brand_code,
                m.brand_description,
                mkv.classification_value as x_code,
                D.geography_hierarchy_description as geo_level1,
                D.geography_level1_description as geo_level2,
                D.geography_level2_description as geo_level3,
                D.geography_level3_description as geo_level4,
                D.geography_level4_description as geo_level5,
                D.geography_description as geo_level6, 
                currency,
                currency_type,
                sales_cogs_type,
                s.common_uom,
                s.transaction_type,
                Sum(s.consolidated_gross_sales_value) as "gross_sales_value",
                Sum(s.consolidated_net_sales_value) as "net_sales_value",
                Sum(s.consolidated_rebates_discounts_value) as "rebates_discounts_value",
                Sum(s.consolidated_sales_quantity_in_common_unit) as "net_sales_common_volume"
            from mio.public.mio042_consolidated_sales_fact s
            left join mio.public.mio02_material m
                on m.material_id = s.material_id
            inner join (
                select
                    old_destination_id,
                    market_year,
                    geography_hierarchy_description,
                    geography_level1_description,
                    geography_level2_description,
                    geography_level3_description,
                    geography_level4_description,
                    geography_description
                from
                    mio.public.mio014_sales_territory_geography_hierarchy
                where
                    business_div_level2 = 'SFC'
                    and (market_year = 'CY')
            ) D 
                on s.destination = D.old_destination_id
            left join public.mio02_material_key_value mkv
                on mkv.material_guid = m.material_guid
            where
                s.key_figure_name = 'V_ACT' --Net sales in USD. Currency_type should be 20.
                ---and (s.transaction_type = 'S01'or s.transaction_type = 'R01' or s.transaction_type = 'S04') --SO1 stands for 3rd party sales. 
                and s.year >= {1}
                and s.year < {2}
                and sales_cogs_type = '10' --means actual sales
                and productline_group = 'SEXF' --seeds materials excluding flowers
                and (m.subcrop = {0}) 
                and mkv.classification_name in ('v_x_code')
            group by m.subcrop,
                s.year,
                s.month,
                m.material_id,
                m.material_number,
                m.variety_number,
                m.variety_name,
                m.brand_code,
                m.brand_description,
                mkv.classification_value,
                D.geography_hierarchy_description,
                D.geography_level1_description,
                D.geography_level2_description,
                D.geography_level3_description,
                D.geography_level4_description,
                D.geography_description, 
                currency,
                currency_type,
                sales_cogs_type,
                s.common_uom,
                s.transaction_type
        ) sc
        left join (
            select
                m.subcrop as crop,
                mccf.year,
                mccf.month,
                m.material_id,
                m.material_number,
                m.variety_number,
                m.variety_name,
                m.brand_code,
                m.brand_description,
                mkv.classification_value as x_code,
                D.geography_hierarchy_description as geo_level1,
                D.geography_level1_description as geo_level2,
                D.geography_level2_description as geo_level3,
                D.geography_level3_description as geo_level4,
                D.geography_level4_description as geo_level5,
                D.geography_description as geo_level6, 
                mccf.currency,
                mccf.currency_type,
                mccf.sales_cogs_type,
                mccf.transaction_type,
                Sum(mccf.total_costs) as total_costs,
                Sum(mccf.total_fixed_costs) as total_fixed_costs,
                Sum(mccf.total_variable_costs) as total_variable_costs
            from mio.public.mio201_consolidated_cogs_fact mccf
            left join mio.public.mio02_material m 
                on m.material_id = mccf.material_id
            inner join (
                select
                    old_destination_id,
                    market_year,
                    geography_hierarchy_description,
                    geography_level1_description,
                    geography_level2_description,
                    geography_level3_description,
                    geography_level4_description,
                    geography_description
                from
                    mio.public.mio014_sales_territory_geography_hierarchy
                where
                    business_div_level2 = 'SFC'
                    and (market_year = 'CY')
            ) D 
                on mccf.destination = D.old_destination_id
            left join public.mio02_material_key_value mkv
                on mkv.material_guid = m.material_guid
            where
                mccf.key_figure_name = 'V_ACT'
                ---and (mccf.transaction_type = 'S01' or mccf.transaction_type = 'R01' or mccf.transaction_type = 'S04')--SO1 stands for 3rd party sales. 
                and mccf.year >= {1}
                and mccf.year < {2}
                and mccf.sales_cogs_type = '10'
                and productline_group = 'SEXF' --seeds materials excluding flowers
                and (m.subcrop = {0})
                and mkv.classification_name in ('v_x_code')
            group by m.subcrop,
                mccf.year,
                mccf.month,
                m.material_id,
                m.material_number,
                m.variety_number,
                m.variety_name,
                m.brand_code,
                m.brand_description,
                mkv.classification_value,
                D.geography_hierarchy_description,
                D.geography_level1_description,
                D.geography_level2_description,
                D.geography_level3_description,
                D.geography_level4_description,
                D.geography_description, 
                mccf.currency,
                mccf.currency_type,
                mccf.sales_cogs_type,
                mccf.transaction_type
        ) cc on
            sc.material_number = cc.material_number 
            and sc.material_id = cc.material_id
            and sc.year = cc.year
            and sc.month = cc.month
            and sc.geo_level1 = cc.geo_level1
            and sc.geo_level2 = cc.geo_level2 
            and sc.geo_level3 = cc.geo_level3
            and sc.geo_level4 = cc.geo_level4 
            and sc.geo_level5 = cc.geo_level5 
            and sc.geo_level6 = cc.geo_level6
            and sc.transaction_type = cc.transaction_type
    """.format("'" + sub_crop + "'", str(min_year), str(max_year))

    mc = MioConnection()
    df_sales = mc.get_data(query_str)

    for col in ['gross_sales_value', 'rebates_discounts_value', 'net_sales_value', 'net_sales_common_volume',
                'total_costs', 'total_fixed_costs', 'total_variable_costs']:
        df_sales[col] = df_sales[col].astype(float).fillna(0)
    # fill in missing values for strings. Groupby can't handle None/nan's
    df_sales = df_sales.fillna('')

    #df_sales = portfolio_lib.clean_decimal_points(df_sales)

    return df_sales


def get_name_synonyms(df_in):
    """
    Get synonyms for each material using variety_name and x_code.
    Use table mio02_material_key_value to find synonyms.

    Inputs: df_in contains columns variety_name and x_code

    Outputs: df_mmkv containing synonyms of each variety name and x_code

    """
    df_ids = df_in.copy()
    # append all names together
    names = np.array([])
    for col in ['x_code', 'variety_name']:
        if names.shape[0] == 0:
            names = pd.unique(df_ids[col])
        else:
            names = np.concatenate((names, df_ids[col].dropna().values.reshape(-1, )), axis=0)

    # drop duplicates
    names = np.unique(names)

    # remove leading and trailing whitespace
    names = np.array([i.strip() for i in names])

    # search for synonyms
    df_mmkv = []
    step_size = 500

    mc = MioConnection()
    for i in range(0, names.shape[0], step_size):
        if i + step_size > names.shape[0]:
            names_search_str = portfolio_lib.make_search_str(names[i:])
        else:
            names_search_str = portfolio_lib.make_search_str(names[i:i + step_size])

        query_str = """

        select distinct mm.variety_name, mmkv.classification_name, mmkv.classification_value
        from mio02_material mm
        inner join mio02_material_key_value mmkv
            on mmkv.material_guid = mm.material_guid
        where (mm.variety_name in {0} or mm.material_id in {0})
            and mmkv.classification_name in ('v_research_code','v_variety_code','v_x_code',
        'v_old_material_nr','v_synonyms','v_parent_pd_code')
        """.format(names_search_str)

        df_mmkv.append(mc.get_data(query_str))
    df_mmkv = pd.concat(df_mmkv, axis=0)

    # remove prefix. Make "USGH GH3392E3" into "GH3392E3"
    df_mmkv.loc[df_mmkv['classification_name'] == 'v_synonyms', 'classification_value'] = df_mmkv.loc[
        df_mmkv['classification_name'] == 'v_synonyms', 'classification_value'].apply(
        lambda x: portfolio_lib.remove_synonym_prefix(x))

    # pivot by variety name/x_code
    df_mmkv_piv = pd.pivot_table(df_mmkv, index=['variety_name'], columns=['classification_name'],
                                 values=['classification_value'],
                                 aggfunc=lambda x: portfolio_lib.join_synonyms_unique(x)).reset_index()
    df_mmkv_piv.columns = [''.join([s for s in col if s != 'classification_value']) for col in df_mmkv_piv.columns]

    # merge synonyms with original id table
    df_ids = df_ids.merge(df_mmkv_piv[['variety_name', 'v_synonyms']], how='left', on='variety_name')

    # break out synonyms into separete columns, check if matches x_code or variety number. IF match, remove.
    nan_mask = df_ids['v_synonyms'].isna().values
    for i in range(df_ids.shape[0]):
        if nan_mask[i] == False:  # not nan
            syns = df_ids.loc[i, "v_synonyms"].split(',')
            syn_count = 0
            for syn in syns:
                # check if equal to x_code or variety number. If match, do nothing. else put in synonym_(syn_count) col
                if syn != df_ids.loc[i, "variety_name"] and syn != df_ids.loc[i, "x_code"]:
                    if "synonym_" + str(syn_count) not in df_ids.columns:
                        df_ids["synonym_" + str(syn_count)] = None

                    df_ids.loc[i, "synonym_" + str(syn_count)] = syn
                    syn_count = syn_count + 1
    df_ids = df_ids.drop(columns=['v_synonyms'])

    #df_ids = portfolio_lib.clean_decimal_points(df_ids)

    return df_ids


def get_be_uuids(df_in):
    """
    Attach be_uuid, which is the ID for the specific instance of genetics, to variety numbers

    Inputs:
        df_in : dataframe containing variety_number column

    Ouputs:
        copy of df_in with new variety_be_uuid column.
    """

    df_ids = df_in.copy()

    # attach be_uuid's to these values....
    # apparently search for variety number aliased as material number in mio02_material
    uniq_variety_numbers = df_ids['variety_number'].drop_duplicates().values

    search_str = portfolio_lib.make_search_str(uniq_variety_numbers)

    query_str = """
        select 
                distinct be_uuid as "variety_be_uuid", material_number as "variety_number"
        from mio02_material 
        where be_uuid is not null
            and be_uuid <> ''
            and material_number in {}
    """.format(search_str)

    mc = MioConnection()
    df_be_uuid = mc.get_data(query_str)
    df_ids = df_ids.merge(df_be_uuid, on='variety_number', how='left')

    return df_ids


def get_bebids(df_in, data_sector):
    """
    gets be_bids by searching for be_uuid, x_code, variety_name, synonyms via denodo table managed.rv_be_dim_mint
    use be_uuid in rv_be_dim mint, then stable line and stable variety code
    also search for stable line code. Grab stable_variety_code in addition to be_bid.
    go through each mapping, only check for materials without be_bids

    Inputs:
        df_in : dataframe containing variety_be_uuid, x_code, variety_name, synonym_# columns

    Outputs:
        copy of df_in with added be_bid column
    """

    df_ids = df_in.copy()

    # bebid_conn id, df_ids identifier.
    if data_sector == 'CORN_NA_SUMMER':
        merge_cols = {
            'stable_variety_code': ['x_code']#, 'variety_name'] + [col for col in df_ids.columns if 'synonym_' in col],
        }
    elif data_sector == 'SOY_NA_SUMMER':
        merge_cols = {
            'variety_be_uuid':['variety_be_uuid'],
            'stable_line_code': ['x_code'],
            'stable_variety_code':['x_code','variety_name'] + [col for col in df_ids.columns if 'synonym_' in col],
        }

    # map between our column identifiers and those in rv_be_dim_min. Note that variety_be_uuid is converted to be_uuid
    rv_be_dim_mint_col_map = {
        'variety_be_uuid':'be_uuid',
        'stable_variety_code':'stable_variety_code',
        'stable_line_code':'stable_line_code'
    }

    # make new dataframe and be_bid column
    non_bebid_id_cols = list(df_ids.columns) # get all meta cols before making be_bid related columns
    for col in ['be_bid','df_ids_col','rv_be_dim_mint_col']:
        df_ids[col] = np.nan
        df_ids[col] = df_ids[col].astype(object)

    # for each connection in merge_cols, find rows without bebids, search for be_bids, merge
    with DenodoConnection() as dc: # do this once instead of per call
        for conn_col in merge_cols.keys():
            ids_col = merge_cols[conn_col]
            for id_col in ids_col: # in case there are multiple id columns for each bebid_conn col
                df_nan = df_ids[(df_ids['be_bid'].isna()) & (df_ids[id_col].notna())].drop(columns=['be_bid'])
                df_conn_list = []

                # for each row in df_nan, deal with potential of different versions for the x_code
                for i_nan in range(df_nan.shape[0]):
                    id_curr = df_nan.iloc[i_nan][id_col]
                    id_curr_no_ez = id_curr
                    for pref in ['-EZ', '-EZT', '-EV', '-EVT']:
                        id_curr_no_ez = portfolio_lib.clean_prefix_str(id_curr_no_ez,pref)

                    id_curr_clean = portfolio_lib.clean_prefix_str(
                        id_curr_no_ez, '.'
                    )

                    query_str = """
                            select distinct 
                                upper(be_uuid) as "variety_be_uuid", 
                                be_bid_preferred as "be_bid",
                                replace(stable_variety_code, ' SYN','') as "stable_variety_code", 
                                replace(stable_line_code, ' SYN','') as "stable_line_code"
                            from managed.rv_be_dim_mint 
                            where {0} like '{1}%' 
                                AND NOT {0} like '%STE%'
                        """.format(rv_be_dim_mint_col_map[conn_col], id_curr_clean)

                    df_conn = dc.get_data(query_str)

                    # if nothing, look for it in rv_bb_material_sdl. Do this only for a specific conn and id col
                    # to only do it once
                    if data_sector == 'CORN_NA_SUMMER' and \
                            df_conn.shape[0] == 0 and \
                            conn_col == 'stable_variety_code' and id_col == 'x_code':
                        # make same columns as original query.

                        query_str = """
                            select distinct 
                                '' as "variety_be_uuid", 
                                be_bid  as "be_bid",
                                '' as "stable_variety_code", 
                                '' as "stable_line_code"
                            from managed.rv_bb_material_sdl 
                            where admin_code like '%{0}%'
                        """.format(id_curr_clean)
                        df_conn2 = dc.get_data(query_str)
                        if df_conn2.shape[0] > 0:
                            df_conn2[id_col] = id_curr
                            df_conn_list.append(df_conn2)
                    # if one, grab it
                    elif df_conn.shape[0] == 1:
                        df_conn[id_col] = id_curr
                        df_conn_list.append(df_conn)
                    # if multiple, find with decimal point. Ignore '-EZ#'
                    elif df_conn.shape[0] > 1:
                        df_conn_dec = df_conn[df_conn[conn_col] == id_curr_no_ez]
                        if df_conn_dec.shape[0] >= 1:
                            df_conn_dec[id_col] = id_curr
                            df_conn_list.append(df_conn_dec)
                        # if no match with decimals, grab non-decimal version
                        else:
                            df_conn_dec = df_conn[df_conn[conn_col] == id_curr_clean]
                            if df_conn_dec.shape[0] >= 1:
                                df_conn_dec[id_col] = id_curr
                                df_conn_list.append(df_conn_dec)

                if len(df_conn_list) > 0:
                    df_bebid_conn = pd.concat(df_conn_list, axis=0)

                    # merge in df_bebid_conn
                    df_nan = df_nan.merge(
                        df_bebid_conn[[id_col,'be_bid']].drop_duplicates(), # rename column to only join on a single column
                        how='inner',
                        on=id_col
                    )
                    # store which df_ids col was used to find the be_bid and which rv_be_dim_mint col was used
                    df_nan['df_ids_col'] = id_col
                    df_nan['rv_be_dim_mint_col'] = rv_be_dim_mint_col_map[conn_col]

                    # merge back to df_ids
                    df_ids = df_ids.merge(df_nan,on=non_bebid_id_cols,how='left', suffixes=('','_new'))
                    # merge col from df_ids and col from df_nan, grab first non nan
                    for col in ['be_bid','df_ids_col','rv_be_dim_mint_col']:
                        if col + '_new' in df_ids.columns:
                            df_ids[col] = df_ids[col].combine_first(df_ids[col+'_new'])
                            df_ids = df_ids.drop(columns=[col+'_new'])
                        df_ids[col] = df_ids[col].astype(object)

    # clean up whitespace at beginning and end of strings
    df_ids = df_ids.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # deal with duplicated variety_numbers by pivoting be_bids
    df_out_list = []
    for index, df_grp in df_ids.groupby(by=['variety_number']):
        df_out = df_grp.iloc[0]
        for col in ['be_bid', 'df_ids_col', 'rv_be_dim_mint_col']:
            vals = df_grp[col].values
            for i in range(1, df_grp.shape[0]):
                df_out[col + '_' + str(i)] = vals[i]

        df_out_list.append(df_out)

    df_out = pd.concat(df_out_list, axis=1).T.reset_index().drop(columns='index')

    return df_out


def get_commercial_trialing_ids(df_in, data_sector):
    """
    grab commercial identifier to df_ids
    get all soy identifiers in mio150 trial fact
    find those that match any subset of identifiers in df_ids. Store matches in mio150_trial_fact_id column...
    only get materials from given region
    there can be multiple variety names mapped to a single variety number....
    many material_ids/numbers per variety number

    Inputs:
        df_ids : table containing identifiers from other commercial table

    Ouputs
        copy of df_ids with mio150_id_# columns added
    """

    df_ids = df_in.copy()

    # get all ids in mio150 from the data sector (country + crop)
    if data_sector == 'SOY_NA_SUMMER' or data_sector == 'CORN_NA_SUMMER':
        if data_sector == 'SOY_NA_SUMMER':
            crop_name = 'soybean'
        else:
            crop_name = 'corn'

        country_codes = "('US','CA','CAN','USA')"
        country_names = "('UNITED STATES','CANADA','UNITED STATES OF AMERICA','USA','ESTADOS UNIDOS')"
    else:
        country_codes = "()"
        country_names = "()"
        print("NEED TO DEFINE COUNTRIES FOR THIS DATA SECTOR")

    query_str = """
        select distinct mtf.variety_name as variety_name_mio150, mtf.place_material_id, mtf.brand_code, mtf.company_brand
        from mio150_trial_fact mtf
        inner join mio161_place mp 
            on mtf.place_id = mp.place_id
        where upper(mtf.crop_name) = upper({0})
        and (upper(mp.country_code) in {1} or upper(mp.country_name) in {2})
        and mtf.observation_type_cd = 'YGSMN'
    """.format("'" + crop_name + "'", country_codes, country_names)

    mc = MioConnection()
    df_mio150_ids = mc.get_data(query_str)
    # remove "BRAND" from ID
    df_mio150_ids['variety_name_mio150_join'] = df_mio150_ids['variety_name_mio150'].apply(
        lambda x: portfolio_lib.remove_brand(x))
    df_mio150_ids = df_mio150_ids.drop_duplicates()

    # Join commercial trialing ID to df_ids using the following:
    # variety_name, x_code, synonym_#
    # variety_name_mio150 contains all matches (comma-separated list)
    id_cols = ['variety_name', 'x_code'] + [col for col in df_ids.columns if 'synonym_' in col]

    for id_col in id_cols:
        df_ids = df_ids.merge(
            df_mio150_ids[['variety_name_mio150_join', 'variety_name_mio150']].drop_duplicates().rename(
                columns={'variety_name_mio150': id_col + '_mio150_temp'}),
            how='left',
            left_on=id_col,
            right_on='variety_name_mio150_join'
        ).drop(columns=['variety_name_mio150_join'])

    # join uniquely to ids_mio150.
    for i in range(df_ids.shape[0]):
        for col in id_cols:
            if pd.notnull(df_ids.loc[i, col + '_mio150_temp']):  # if we found an id
                # figure out where to put that id
                id_counter = 0
                flag_duplicate = 0
                while 'mio150_id_' + str(id_counter) in df_ids.columns and pd.notnull(
                        df_ids.loc[i, 'mio150_id_' + str(id_counter)]):
                    if df_ids.loc[i, col + '_mio150_temp'] == df_ids.loc[i, 'mio150_id_' + str(id_counter)]:
                        flag_duplicate = 1
                    id_counter = id_counter + 1

                if not flag_duplicate:
                    if 'mio150_id_' + str(id_counter) not in df_ids.columns:  # instantiate column
                        df_ids['mio150_id_' + str(id_counter)] = pd.Series(dtype=str)
                    # set value
                    df_ids.loc[i, 'mio150_id_' + str(id_counter)] = df_ids.loc[i, col + '_mio150_temp']

                    # drop all temp cols
    df_ids = df_ids.drop(columns=[col for col in df_ids.columns if '_temp' in col])

    # remove any duplicated variety_names
    df_ids = df_ids.drop_duplicates(subset='variety_name')

    return df_ids


def get_plot_result_data(be_bid_arr, crop_name):
    """
    query to get text traits from "managed"."rv_plot_result_calc_spirit"
    traits are hard-coded...

    inputs:
        array of bebids, crop_name (soybean, corn)

    outputs:
        dataframe containing be_bid, trait, and value
    """
    # crop names = 'soybean', 'corn'
    crop_name = crop_name.lower()
    step = 10000

    if crop_name == 'soybean':
        crop_guid = '6C9085C2-C442-48C4-ACA7-427C4760B642'
    if crop_name == 'corn':
        crop_guid = 'B79D32C9-7850-41D0-BE44-894EC95AB285'

    with DenodoConnection() as dc:
        material_mapper_list = []
        trait_data_list = []
        # get all material ids
        for i in range(0, be_bid_arr.shape[0], step):
            if i + step < be_bid_arr.shape[0]:
                be_bid_str = portfolio_lib.make_search_str(be_bid_arr[i:i + step])
            else:
                be_bid_str = portfolio_lib.make_search_str(be_bid_arr[i:])

            query_str = """
                SELECT DISTINCT "material_id", "be_bid"
                FROM "managed"."rv_bb_material_sdl"
                WHERE "crop_guid"= {1} -- 6C9085C2-C442-48C4-ACA7-427C4760B642 is the crop_guid for SOY
                     AND "be_bid" in {0}
            """.format(be_bid_str, "'" + crop_guid + "'")
            material_mapper_list.append(dc.get_data(query_str))

        df_material_mapper = pd.concat(material_mapper_list, axis=0)

        # get all traits for the material_ids in df_material_mapper
        material_id_arr = df_material_mapper['material_id'].drop_duplicates().values

        for i in range(0, material_id_arr.shape[0], step):
            if i + step < material_id_arr.shape[0]:
                material_id_str = portfolio_lib.make_search_str(material_id_arr[i:i + step])
            else:
                material_id_str = portfolio_lib.make_search_str(material_id_arr[i:])

            query_str = """       
                SELECT prcs."trait_code" as "trait",
                    FIRST(prcs."alpha_value") as "alpha_value",
                    prcs."material_id" as "material_id"
                FROM "managed"."rv_plot_result_calc_spirit" prcs
                WHERE prcs."material_id" in {1}
                    AND LOWER(prcs."crop_name")={0}
                    AND UPPER(prcs."trait_code") IN ('DIC_T','E3_T','LL55_T','RR2_T','DPM_T',
                                                'MI__T','CLS_T','BP_T','BSR_T','CN3_T',
                                                'E1_T','FELS_T','FL_CT','HILCT','MET_T',
                                                'PB_CT','PD_CT','RPS_T','STMTT','STS_T','HARVT',
                                                'PLTQT','NOTET')
                GROUP BY "prcs"."material_id",prcs."trait_code"

            """.format("'" + crop_name + "'", material_id_str)
            trait_data_list.append(dc.get_data(query_str))

    # join trait_data with bebids, output
    df_trait_data = pd.concat(trait_data_list, axis=0)
    output_df = df_trait_data.merge(df_material_mapper, on='material_id', how='inner').drop(columns='material_id')

    # combine traits for the same be_bid
    output_df = output_df.drop_duplicates().groupby(by=['be_bid', 'trait']).agg(
        lambda x: portfolio_lib.join_synonyms_unique(x)).reset_index()

    return output_df


def get_commercial_descriptions(names):
    """
    get descriptions about commercial products via mio01_material_key_value

    inputs:
        array of names

    outputs:
        dataframe containing various descriptions. v_relative_maturity and v_maturity_subgroup are of particular interest
    """

    step_size = 1000
    mc = MioConnection()
    df_list = []
    for i in range(0, names.shape[0], step_size):
        if i + step_size > names.shape[0]:
            names_search_str = portfolio_lib.make_search_str(names[i:])
        else:
            names_search_str = portfolio_lib.make_search_str(names[i:i + step_size])

        query_str = """
            select distinct mm.variety_name as "name", 
                mmkv.classification_name as "trait", 
                mmkv.classification_value as "alpha_value"
            from mio02_material mm
            inner join mio02_material_key_value mmkv
                on mmkv.material_guid = mm.material_guid
            where (mm.variety_name in {0})
                and mmkv.classification_name <> 'v_synonym'

        """.format(names_search_str)

        df_list.append(mc.get_data(query_str))

    return pd.concat(df_list, axis=0)


def get_materials_and_experiments_per_decision_group(ap_data_sector, analysis_year, material_type='entry'):
    """
    Get entry_ids, decision group names, stage, and check information from postgres table

    Inputs:
        Data sector, year, material type (entry or parent)

    Outputs:
        dataframe containing entry identifiers, corresponding decision groups, stages, and check information
    """
    # analysis year in postgres table corresponds to harvest year... our year is planting year.
    # for some data sectors (LATAM), harvest year is different from planting year
    dc = PostgresConnection()
    year_offset = 0
    if 'BRAZIL_SUMMER' in ap_data_sector or 'LAS' in ap_data_sector or 'LAN' in ap_data_sector:
        year_offset = 1

    if material_type == 'entry':
        postgres_table = 'decision_group_entry'
    elif material_type == 'parent':
        postgres_table = 'decision_group_hybrid_parent'

    query_str = """
                    select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", 
                        CAST(dg.stage as float) as "dg_stage", dge.is_check , dge2.experiment_id as "source_id"
                    from advancement.decision_group dg 
                    inner join advancement.{2} dge 
                        on dge.decision_group_id = dg.decision_group_id 
                    inner join advancement.decision_group_experiment dge2 
                        on dge2.decision_group_id = dg.decision_group_id 
                    where dg.ap_data_sector_name = {0}
                        and dg.analysis_year = {1}
                        and dg.decision_group_type != 'MULTI_YEAR'
                """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    df = dc.get_data(query_str)

    # get decision_group_name as source_id in case of single-exp decision groups
    query_str = """
                    select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", 
                        CAST(dg.stage as float) as "dg_stage", dge.is_check , dg.decision_group_name as "source_id"
                    from advancement.decision_group dg 
                    inner join advancement.{2} dge 
                        on dge.decision_group_id = dg.decision_group_id 
                    where dg.ap_data_sector_name = {0}
                        and dg.analysis_year = {1}
                        and dg.decision_group_type != 'MULTI_YEAR'
                """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    df_dg = dc.get_data(query_str)
    df = pd.concat((df, df_dg), axis=0).drop_duplicates()

    # all 1's and 0's for is_check
    replace_dict = {True: 1, 'True': 1, False: 0, 'False': 0}
    df = df.replace(to_replace=replace_dict)

    return df


def get_trial_pheno_data_reduced_columns(ap_data_sector, entry_arr, min_year, max_year=40000):
    """
    returns R&D trialing data using rv_trial_pheno_analytic

    Inputs:
        ap_data_sector : str
        entry_arr : numpy array of bebids
        min_year : earliest year to collect data from, inclusive
        max_year : latest year to collect data from, exclusive

    Output:
        dataframe containing trial data


    """
    entry_arr = np.unique(entry_arr)

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
            trial_pheno_subset.analysis_year,
            trial_pheno_subset.trial_id,
            trial_pheno_subset.entry_id as "be_bid",
            trial_pheno_subset.year AS year,
            trial_pheno_subset.experiment_id,
            COALESCE(feature_export_et.et_value,'undefined'),
            COALESCE(feature_export_ms_loc.market_seg, feature_export_ms_trial.market_seg, 'all'),
            trial_pheno_subset.trait,
            CASE 
                WHEN LOWER(trial_pheno_subset.irrigation) = 'irr'
                    THEN 'IRR'
                ELSE 'DRY'
            END AS irrigation,
            trial_pheno_subset.maturity_group,
            trial_pheno_subset.result_numeric_value
        FROM (
            SELECT
                rv_trial_pheno_analytic_dataset.ap_data_sector AS ap_data_sector,
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
                    AND rv_trial_pheno_analytic_dataset.entry_id in {3}
                    AND rv_trial_pheno_analytic_dataset.ap_data_sector = {0}
                    AND rv_trial_pheno_analytic_dataset.year >= {1}
                    AND rv_trial_pheno_analytic_dataset.year < {2}
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.tr_exclude,False) AS boolean)
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.psp_exclude,False) AS boolean)
                    AND NOT CAST(COALESCE(rv_trial_pheno_analytic_dataset.pr_exclude,False) AS boolean)
                GROUP BY
                    ap_data_sector,
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
                AND source_year >= {1}
                AND source_year < {2}
            GROUP BY
                ap_data_sector, 
                plot_barcode,
                trait
        ) rv_ap_all_yhat
        ON rv_ap_all_yhat.ap_data_sector = trial_pheno_subset.ap_data_sector
            AND rv_ap_all_yhat.plot_barcode = trial_pheno_subset.plot_barcode
            AND rv_ap_all_yhat.trait = trial_pheno_subset.trait
        WHERE rv_ap_all_yhat.outlier_flag = 0 or rv_ap_all_yhat.outlier_flag IS NULL
    """

    step = 2500
    output_df_list = []
    with SnowflakeConnection() as dc:
        for i in range(0, entry_arr.shape[0], step):
            if i + step > entry_arr.shape[0]:
                entry_step = entry_arr[i:]
            else:
                entry_step = entry_arr[i:i + step]

            entry_list_as_str = portfolio_lib.make_search_str(entry_step)
            query_str_use = query_str.format("'" + ap_data_sector + "'", min_year, max_year, entry_list_as_str)
            output_df_list.append(dc.get_data(query_str_use, do_lower=True))

    output_df = pd.concat(output_df_list, axis=0)

    return output_df


def get_bebid_sample_id_map(
        bebids,
        get_parents=0
):
    # map from sample id to be_bid

    step_size = 1000
    df_map1_list = []
    df_map2_list = []
    with SnowflakeConnection() as sc:
        for i_bb in range(0, bebids.shape[0], step_size):
            if i_bb + step_size > bebids.shape[0]:
                search_str = portfolio_lib.make_search_str(bebids[i_bb:])
            else:
                search_str = portfolio_lib.make_search_str(bebids[i_bb:i_bb + step_size])

            # soybean crop_id = 7
            # corn crop id = 9, sunflower crop id = 12
            if get_parents is False:
                query_str = """
                    SELECT distinct
                        rv_s.SAMPLE_CODE AS "sample_id",
                        rv_bba.be_bid AS "be_bid"
                    FROM (
                        select distinct 
                            material_guid,
                            be_bid
                        from RV_BB_MATERIAL_DAAS 
                        where be_bid in {0}
                    ) rv_bba
                    INNER JOIN RV_SAMPLE rv_s
                        ON rv_s.GERMPLASM_GUID = rv_bba.material_guid
                """.format(search_str)
                df_map1_list.append(sc.get_data(query_str, do_lower=True))
            else:
                # get parent be_bids, then merge into RV_sample

                # get pool 1 data
                query_str = """
                    SELECT distinct
                        rv_mat.be_bid,
                        CASE
                            WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_mat.female_be_bid)
                            WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_mat.male_be_bid)
                            ELSE rv_mat.female_be_bid
                        END AS par_hp1_be_bid,
                        CASE
                            WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_s_fe.sample_code
                            WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_s_ma.sample_code
                        END AS par_hp1_sample
                    FROM (
                        SELECT distinct
                            material_guid,
                            female_be_bid ,
                            male_be_bid ,
                            be_bid
                        from RV_BB_MATERIAL_DAAS 
                        WHERE be_bid IN {0}
                    ) rv_mat
                    INNER JOIN RV_CORN_MATERIAL_TESTER_ADAPT rv_cmt
                        ON rv_cmt.be_bid = rv_mat.be_bid
                    INNER JOIN RV_BB_MATERIAL_DAAS rv_mat_fe
                        ON rv_mat_fe.be_bid = rv_mat.female_be_bid
                    INNER JOIN RV_BB_MATERIAL_DAAS rv_mat_ma
                        ON rv_mat_ma.be_bid = rv_mat.male_be_bid
                    INNER JOIN RV_SAMPLE rv_s_fe
                        ON rv_s_fe.germplasm_guid = rv_mat_fe.material_guid
                    INNER JOIN RV_SAMPLE rv_s_ma
                        ON rv_s_ma.germplasm_guid = rv_mat_ma.material_guid
                """.format(search_str)
                df_map1_list.append(sc.get_data(query_str, do_lower=True))

                # get pool 2 data
                query_str = """
                    SELECT distinct
                        rv_mat.be_bid,
                        CASE
                            WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_mat.female_be_bid)
                            WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_mat.male_be_bid)
                            ELSE rv_mat.male_be_bid
                        END AS par_hp2_be_bid,
                        CASE
                            WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_s_fe.sample_code
                            WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_s_ma.sample_code
                        END AS par_hp2_sample
                    FROM (
                        SELECT distinct
                            material_guid,
                            female_be_bid ,
                            male_be_bid ,
                            be_bid
                        from RV_BB_MATERIAL_DAAS 
                        WHERE be_bid IN {0}
                    ) rv_mat
                    INNER JOIN RV_CORN_MATERIAL_TESTER_ADAPT rv_cmt
                        ON rv_cmt.be_bid = rv_mat.be_bid
                    INNER JOIN RV_BB_MATERIAL_DAAS rv_mat_fe
                        ON rv_mat_fe.be_bid = rv_mat.female_be_bid
                    INNER JOIN RV_BB_MATERIAL_DAAS rv_mat_ma
                        ON rv_mat_ma.be_bid = rv_mat.male_be_bid
                    INNER JOIN RV_SAMPLE rv_s_fe
                        ON rv_s_fe.germplasm_guid = rv_mat_fe.material_guid
                    INNER JOIN RV_SAMPLE rv_s_ma
                        ON rv_s_ma.germplasm_guid = rv_mat_ma.material_guid
                """.format(search_str)
                df_map2_list.append(sc.get_data(query_str, do_lower=True))

    if len(df_map1_list) > 0:
        df_map1 = pd.concat(df_map1_list, axis=0).drop_duplicates()
    else:
        df_map1 = pd.DataFrame()

    if len(df_map2_list) > 0:
        df_map2 = pd.concat(df_map2_list, axis=0).drop_duplicates()
    else:
        df_map2 = pd.DataFrame()

    return df_map1, df_map2


def get_pvs_data(
    data_sector,
    bebids,
    analysis_year
):
    # search for these analysis_types
    analysis_type_str = portfolio_lib.make_search_str(['SingleExp', 'MultiExp', 'PhenoGCA', 'GenoPred', 'MynoET'])

    # get decision groups to search for. Only get stages above 4
    # use decision groups to filter data from pvs_data
    pc = PostgresConnection()

    query_str = """
    select dg.decision_group_name , 
    	dg.analysis_year , 
    	cast(dg.maturity as float) as "maturity_group",  
    	cast(dg.stage as float) as "stage", 
    	dg.decision_group_type 
    from advancement.decision_group dg 
    where dg.ap_data_sector_name = {0}
    	and dg.analysis_year >= {1}
        and dg.analysis_year < {2}
        and cast(dg.stage as float) > 4
    """.format("'" + data_sector + "'", str(analysis_year - 2), str(analysis_year + 1))

    df_dgs = pc.get_data(query_str)
    dg_str = portfolio_lib.make_search_str(df_dgs['decision_group_name'].values)

    # only search for entry_identifiers in bebids
    bebid_str = portfolio_lib.make_search_str(bebids)

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
        dg_str = portfolio_lib.make_search_str(df_dgs['decision_group_name'].values)
        query_str_where = """
        WHERE ap_data_sector = {0}
          AND TRY_TO_NUMBER(trialing_year) = {1}
          AND analysis_type IN {3}
          AND loc = 'ALL'
          AND material_type != 'untested_entry'
          AND upper(source_id) in {2}
          AND entry_identifier in {4}
        """
    elif "EAME_SUMMER" in data_sector:
        dg_str = ""
        query_str_where = """
            WHERE ap_data_sector = {0}
              AND TRY_TO_NUMBER(trialing_year) = {1}
              AND analysis_type in {3}
              AND loc = 'ALL'
              AND material_type != 'untested_entry'
              AND entry_identifier in {4}
       """
    else:
        dg_str = ""
        query_str_where = """
            WHERE ap_data_sector = {0}
              AND TRY_TO_NUMBER(trialing_year) = {1}
              AND analysis_type = 'MultiExp'
              AND loc = 'ALL'
              AND material_type != 'untested_entry'
              AND entry_identifier in {4}
       """

    # if not CORN_NA_SUMMER include market segment filter.
    # corn na data before 2022 is weird....
    if not (data_sector in [
        'CORN_NA_SUMMER',
        'CORN_CHINA_SPRING',
        'CORN_CHINA_SUMMER',
        'CORN_BRAZIL_SUMMER',
        'CORN_BRAZIL_SAFRINHA'
    ]):
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
        decision_group,
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
      ON rv_ap_all_pvs.source_id = dg_rm_list.decision_group
    INNER JOIN ( -- make a dummy row containing ymh_residual so that we grab that trait
        SELECT 
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
        select 
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
        AND rv_ap_sector_trait_config.analysis_year = TRY_TO_NUMBER(rv_ap_all_pvs.source_year)
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
        pvs_single_year.entry_identifier as "be_bid",
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
        "'" + data_sector + "'",
        str(analysis_year),
        dg_str,
        analysis_type_str,
        bebid_str
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

    # stage info from decision group table if not in experiment config
    if df_dgs.shape[0] > 0:
        output_df_stg = output_df[output_df['stage'].notna()]
        output_df_no_stg = output_df[output_df['stage'].isna()].drop(columns='stage')
        df_dgs_pre_merge = df_dgs.rename(columns={'decision_group_name': 'source_id'})
        df_dgs_pre_merge = df_dgs_pre_merge[['source_id', 'stage']].drop_duplicates()
        output_df_no_stg = output_df_no_stg.merge(df_dgs_pre_merge, on='source_id', how='left')
        output_df = pd.concat((output_df_stg, output_df_no_stg), axis=0)

    return output_df
