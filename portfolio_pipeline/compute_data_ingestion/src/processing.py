# import packages
import os
import json
import argparse

from libs.event_bridge.event import error_event

import pandas as pd
from portfolio_libs import portfolio_sql
from portfolio_libs import portfolio_lib
import warnings


def ingest_data(
        crop_name,
        data_sector,
        analysis_year,
        write_outputs=1
):
    """
    this function queries alation and denodo for data related to commercial products
        this includes material identifiers and sales data
    this function also queries denodo tables for recent pvs and trial_pheno data for commercial and stage 5/6 products

    inputs:
        crop_name : name of crop ('soybean','corn',etc.)
        data_sector : name of data sector
        analysis_year : (int) year analysis is performed for. If doing a historic year, will not grab data after that year
        write_outputs : whether to save any outputs locally.

    outputs:
        df_ids : table containing material identifiers for commercial products. Variety name and be_bid, plus others
        df_sales : table containing transaction data for commercial products
        df_traits : table containing traits for commercial and stage 5/6 products. RM, marker traits, etc.
        df_trial : table containing trialing data for commercial products and stage 5/6 materials
        df_pvs : table containing AAE/GSE outputs for commercial products and stage 5/6 materials
    """

    # query mio database for sales data
    df_sales = portfolio_sql.get_mio_sales_data(crop_name, min_year=2010)

    # filter sales information to NA
    df_sales = df_sales[df_sales['geo_level2'] == 'NORTH AMERICA']

    # prepare sales table for output
    # assign expected plant_year to each transaction based on month/year sold
    # we want to aggregate based on when the seed will be planted, even if it was sold in a previous calendar year
    # these thresholds will need to be set per data sector presumably.

    # for soy, there is a clear dip in sales before July, so use July as cutoff each year

    if data_sector == 'SOY_NA_SUMMER':
        sales_month = 7
        returns_month = 8
    elif data_sector == 'CORN_NA_SUMMER':
        sales_month = 8
        returns_month = 8
    else:
        sales_month = 8
        returns_month = 8
    df_sales['plant_year'] = df_sales['year']
    df_sales.loc[(df_sales['month'] > sales_month) & (df_sales['transaction_type'].isin(['S01', 'S04'])), 'plant_year'] += 1
    df_sales.loc[(df_sales['month'] > returns_month) & (df_sales['transaction_type'].isin(['R01', 'R02'])), 'plant_year'] += 1

    # remove first year since this will only have half a year (we grabbed fiscal year X and above, but planting year is shifted by 6ish months)
    df_sales = df_sales.loc[df_sales['plant_year'] > df_sales['year'].min(), :]

    # remove all data after analysis year to enforce year consistency
    df_sales = df_sales[df_sales['plant_year'] <= analysis_year]

    # get identifiers for sales data. Build material_id table
    # variety number is used as the common identifer across tables
    # split out identifiers and create an identifier table to merge back to sales data, to merge to commercial trialing data, to merge to be_bids and all R&D tables...
    df_ids_material = df_sales[
        ['material_id', 'material_number', 'variety_number', 'variety_name', 'brand_code', 'brand_description',
         'x_code']].drop_duplicates()
    # clean up whitespace at beginning and end of strings
    df_ids_material = df_ids_material.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df_ids = df_ids_material.drop(columns=['material_id', 'material_number']).drop_duplicates()

    # drop any duplicated variety_numbers.
    df_ids = df_ids.drop_duplicates(subset='variety_number')

    # # get all synonyms for these materials. # mio02_maerial_key_value also contains Maturity, resistances, trait info.
    # x_code/variety_name -> material_guid via mio02_material
    # join to mio_02_material_classification on material_guid
    # extract any identifier column
    df_ids = portfolio_sql.get_name_synonyms(df_ids)

    # get table connecting variety_number to synonym (variety_name or synonym_# column)
    # this table is used to convert between synonyms and variety_number

    synonym_cols = [col for col in df_ids.columns if 'synonym' in col]

    df_map = pd.DataFrame()
    for col in ['variety_name', 'x_code'] + synonym_cols:
        df_map = pd.concat((df_map, df_ids[['variety_number', col]].rename(columns={col: 'name'}).dropna()),
                           axis=0)  # defaults to any

    df_map = df_map.drop_duplicates()

    # connect be_uuid via variety_number
    df_ids = portfolio_sql.get_be_uuids(df_ids)

    # get bebids
    df_ids = portfolio_sql.get_bebids(df_ids, data_sector=data_sector)

    # grab IDs in commercial trialing data table
    df_ids = portfolio_sql.get_commercial_trialing_ids(df_ids, data_sector=data_sector)

    # due to multiple be_bids,
    # make be_bid alias dataframe, which maps all different versions of the same be_bid to 1 be_bid
    # this is the "source of truth" be bid
    # bebid does map to itself so we can inner join using be_bid
    df_bebid_alias_list = []

    df_bebid_alias = df_ids[['be_bid']].dropna().copy()
    df_bebid_alias['be_bid_truth'] = df_bebid_alias['be_bid']
    df_bebid_alias_list.append(df_bebid_alias)

    bebid_cols = [col for col in df_ids.columns if 'be_bid_' in col]  # skips 'be_bid', which we did above
    for col in bebid_cols:
        df_bebid_alias = df_ids[['be_bid', col]].dropna().rename(columns={'be_bid': 'be_bid_truth', col: 'be_bid'})
        df_bebid_alias_list.append(df_bebid_alias)

    df_bebid_alias = pd.concat(df_bebid_alias_list, axis=0)
    df_bebid_alias = df_bebid_alias.drop_duplicates()

    # remove all IDs except for variety_number/variety_name in df_sales. Keep brand code and description to keep brands separate
    for col in ['material_id', 'material_number', 'x_code']:
        if col in df_sales.columns:
            df_sales = df_sales.drop(columns=col)

    # load in marker/trait info for both commercial bebids and stage 5/6 bebids
    # load in RM, marker information etc. for IDs
    df_mmkv = portfolio_sql.get_commercial_descriptions(df_map['name'].drop_duplicates())
    df_mmkv = df_mmkv.merge(df_map, on='name', how='inner').drop(columns='name')
    df_mmkv = df_mmkv.merge(df_ids[['variety_number', 'be_bid']].drop_duplicates(), on='variety_number', how='left')

    # get stage 5/6 bebids
    df_rd_bebids = portfolio_sql.get_materials_and_experiments_per_decision_group(ap_data_sector=data_sector,
                                                                                  analysis_year=analysis_year,
                                                                                  material_type='entry')
    df_rd_bebids = df_rd_bebids[df_rd_bebids['dg_stage'] >= 5][
        ['entry_identifier', 'is_check']].drop_duplicates().groupby(by=['entry_identifier']).max().reset_index()

    # search for text traits
    bebids_search = pd.concat((df_bebid_alias['be_bid'], df_rd_bebids['entry_identifier']), axis=0).drop_duplicates()
    if crop_name == 'soybean':
        df_t = portfolio_sql.get_plot_result_data(bebids_search, crop_name=crop_name)

        df_t = df_t.merge(df_ids[['variety_number', 'be_bid']].drop_duplicates(), on='be_bid', how='left')
        # merge commercial and R&D traits
        df_traits = pd.concat((df_t, df_mmkv), axis=0).drop_duplicates()
    else:
        df_traits = df_mmkv

    # R&D trialing data
    df_trial = portfolio_sql.get_trial_pheno_data_reduced_columns(
        ap_data_sector=data_sector,
        entry_arr=bebids_search,
        min_year=analysis_year-2,
        max_year=analysis_year+1  # max year is exclusive. act like future data does not exist
    )
    print(df_trial.shape, df_trial.columns)
    df_trial = portfolio_lib.alias_bebid(
        df_trial,
        df_bebid_alias,
        in_col='be_bid'
    )

    # AAE/GSE outputs
    df_pvs = portfolio_sql.get_pvs_data(
        data_sector=data_sector,
        bebids=bebids_search,
        analysis_year=analysis_year
    )
    
    df_pvs = portfolio_lib.alias_bebid(
        df_pvs,
        df_bebid_alias,
        in_col='be_bid'
    )

    # write all output tables
    if write_outputs:
        # set output directory
        out_dir = '/opt/ml/processing/data/data_ingestion/'
        # check to see if output dir exists, if no create
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        df_ids.to_parquet(os.path.join(out_dir, 'material_ids_table_{}.parquet'.format(str(analysis_year))), index=False)
        df_sales.to_parquet(os.path.join(out_dir, 'sales_table_{}.parquet'.format(str(analysis_year))), index=False)
        df_traits.to_parquet(os.path.join(out_dir, 'material_trait_table_{}.parquet'.format(str(analysis_year))), index=False)
        df_trial.to_parquet(os.path.join(out_dir, 'trial_table_{}.parquet'.format(str(analysis_year))), index=False)
        df_pvs.to_parquet(os.path.join(out_dir, 'pvs_table_{}.parquet'.format(str(analysis_year))), index=False)
        df_bebid_alias.to_parquet(os.path.join(out_dir, 'bebid_alias_table_{}.parquet'.format(str(analysis_year))), index=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    args = parser.parse_args()
    warnings.filterwarnings("ignore")

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        analysis_year = int(data['analysis_year'])
        pipeline_runid = data['target_pipeline_runid']

    try:
        crop_name = None
        if 'SOY' in ap_data_sector:
            crop_name = 'soybean'
        elif 'CORN' in ap_data_sector:
            crop_name = 'corn'
        elif 'SUNFLOWER' in ap_data_sector:
            crop_name = 'sunflower'
        else:
            error_event(ap_data_sector, analysis_year, pipeline_runid, "unknown crop name")

        if crop_name is not None:
            ingest_data(
                crop_name=crop_name,
                analysis_year=analysis_year,
                data_sector=ap_data_sector
            )

    except Exception as e:
        error_event(ap_data_sector, analysis_year, pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
