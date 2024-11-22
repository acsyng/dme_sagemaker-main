# import packages
import os
import json
import argparse

from libs.event_bridge.event import error_event

import pandas as pd
import numpy as np

from portfolio_libs import portfolio_sql
from portfolio_libs import portfolio_lib


def compute_product_maturity(row, crop_name, var_name='net_sales_common_volume'):
    """
    Function to get product maturity stages based on volume of sales data

    maturities:
        pre-launch (stage 7) = units sold < (1000 for corn, 100 for soy?)
        launch (stage 8) = first year sales >= (1000 for corn, 100 for soy?)
        growth (stage 9) = 120%+ from previous year
        mature (stage 10) = 80%+ from previous year, not in growth
        decline (stage 11) = <80% from previous year
        phase-out (stage 12) = <80% from previuos year and stopped seed production (sales < 100 for soy)
        retire (stage 13) = no sales

        also, if 5+ years of sale and not in stage before decline, put in decline
        soy has no phase out because they need to generate new seeds annually

    inputs:
        row of a dataframe
        crop_name ('soybean','corn','sunflower')
    """
    lifecycle_to_stage_dict = {
        'pre-launch': 7,
        'launch': 8,
        'growth': 9,
        'mature': 10,
        'decline': 11,
        'phase-out': 12,
        'retire': 13
    }

    min_vol_thresh = {
        'soybean': 100,
        'corn': 10
    }
    out = []
    flag_found_first = 0
    n_years = 0
    prev_vol = np.nan
    out_name = []
    for (idx, vol) in zip(row.index, row):
        if var_name in idx:  # only use var_name columns.
            out_name.append('stg_' + idx.split('_')[-1])
            if np.isnan(vol) == True:
                if flag_found_first == 1:  # we have data previously. reset state
                    flag_found_first = 0
                    prev_vol = np.nan
                    prev_prev_vol = np.nan
                out.append(np.nan)
            else:  # non-nan value
                # if first, set stage to prelaunch or launch
                if flag_found_first == 0:
                    if vol < min_vol_thresh[crop_name]:
                        out.append(lifecycle_to_stage_dict['pre-launch'])
                    else:
                        out.append(lifecycle_to_stage_dict['launch'])
                    flag_found_first = 1
                    n_years = 1
                # if retired, stay in retire
                elif out[-1] == lifecycle_to_stage_dict['retire']:
                    out.append(lifecycle_to_stage_dict['retire'])
                else:
                    # check if we go to the next stage
                    # decline = <80% prev stage or 5+ years of sales data or was in decline previously
                    # retire or pre-launch if vol < min thresh and
                    if vol < min_vol_thresh[crop_name]:
                        if out[-1] == lifecycle_to_stage_dict['pre-launch']:  # if still in pre-launch/launch
                            out.append(out[-1])
                        else:
                            out.append(lifecycle_to_stage_dict['retire'])
                    elif vol / prev_vol <= 0.8 or n_years >= 5:  # or out[-1] == lifecycle_to_stage_dict['decline']:
                        out.append(lifecycle_to_stage_dict['decline'])  # in decline
                    # growth = >120%  previous stage
                    elif vol / prev_vol > 1.2:
                        out.append(lifecycle_to_stage_dict['growth'])
                    # mature = >80% prev stage and not growth
                    elif vol / prev_vol > 0.8:
                        out.append(lifecycle_to_stage_dict['mature'])  # in mature
                    else:
                        out.append(lifecycle_to_stage_dict['retire'])
                # count how many years of sales
                n_years = n_years + 1
            # prevent division by 0
            if vol == 0:
                vol = 1e-6
            if prev_vol == 0:
                vol = 1e-6
            prev_vol = vol

    return pd.Series(out, index=out_name)


def compute_product_lifecycle(
        crop_name,
        data_sector,
        analysis_year,
        data_ingestion_folder,
        write_outputs=1
):
    """
    this function queries alation and denodo for data related to commercial products
        this includes material identifiers and sales data
    this function also queries denodo tables for recent pvs and trial_pheno data for commercial and stage 5/6 products

    inputs:
        crop_name : name of crop ('soybean','corn',etc.)
        data_ingestion_folder : local location of outputs from data ingestion : name of data sector
        analysis_year : (int) year analysis is performed for. If doing a historic year, will not grab data after that year
            will compute product lifecycle for all years prior to analysis year and analysis year
            also used to grab files from data ingestion folder
        write_outputs : whether to save any outputs locally.

    outputs:
        df_lifecycle : table containing product lifecycle information per product per brand for all years ingested
            stage and volume sold per year, RM information contained in this dataframe
    """
    # load in ingested data
    # IDs
    df_ids = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'material_ids_table_{}.parquet'.format(str(analysis_year))
    ))
    # sales data
    df_sales = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'sales_table_{}.parquet'.format(str(analysis_year))
    ))
    # clean-up sales brand codes
    df_sales = df_sales[df_sales['brand_code'] != '']
    # rm data
    df_trait = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'material_trait_table_{}.parquet'.format(str(analysis_year))
    ))
    df_rm = df_trait[df_trait['trait'] == 'v_relative_maturity']

    # group sales data
    meta_cols = ['plant_year', 'variety_number', 'brand_code']
    value_cols = ['net_sales_value', 'total_costs', 'net_sales_common_volume']

    df_sales_grp = df_sales[meta_cols + value_cols].groupby(by=meta_cols).mean().reset_index()
    # df_sales_grp = df_sales_grp[df_sales_grp['net_sales_value'] >= 0]
    df_sales_grp['sales_minus_costs'] = df_sales_grp['net_sales_value'] - df_sales_grp['total_costs']

    # set plant year as a string to make indexing easier, will set as integer before writing
    df_sales_grp['plant_year'] = df_sales_grp['plant_year'].astype(str)

    # pivot net_sales_value by year
    index_cols = meta_cols.copy()
    index_cols.remove('plant_year')
    df_sales_piv = pd.pivot_table(
        df_sales_grp,
        index=index_cols,
        columns='plant_year',
        values=['net_sales_value', 'net_sales_common_volume']
    ).reset_index()
    # update column names
    df_sales_piv.columns = ['_'.join(filter(None, col)) for col in df_sales_piv.columns]

    # get stages based on sales data for each row
    df_stg = df_sales_piv.set_index(['variety_number', 'brand_code']).apply(
        lambda x : compute_product_maturity(x, crop_name=crop_name),
        axis=1,
        result_type='expand'
    )
    df_stg = df_stg.reset_index()

    # merge RM data with volume and stage data

    # parse RM for country code, brand code and RM
    df_rm_temp = pd.DataFrame(
        list(df_rm['alpha_value'].apply(lambda x: portfolio_lib.parse_rm(x, data_sector=data_sector))),
        columns=['country_code', 'brand_code', 'rm']
    )

    for col in df_rm_temp.columns:
        df_rm[col] = df_rm_temp[col].values

    # output table with RM, sales data. Melt by year
    df_lifecycle = df_stg.merge(df_sales_piv, on=['variety_number', 'brand_code'], how='inner').merge(
        df_rm[['rm', 'variety_number']].groupby(by='variety_number').mean().reset_index(),
        how='inner',
        on='variety_number'
    )
    # merge in variety_name
    df_lifecycle = df_lifecycle.merge(df_ids[['variety_number', 'brand_code', 'variety_name']],
                                      on=['variety_number', 'brand_code'], how='left')

    # melt df_lifecycle to get data by year, remove rows where stage is NaN
    var_names = ['stg', 'net_sales_common_volume', 'net_sales_value']
    var_name_mapper = {
        'stg': 'stage',
        'net_sales_common_volume': 'net_sales_volume',
        'net_sales_value': 'net_sales_value'
    }
    id_vars = ['variety_number', 'brand_code', 'variety_name']

    # melt RM
    df_melt = pd.melt(
        df_lifecycle,
        id_vars=id_vars,
        value_vars=['rm'],
        value_name='RM'
    ).drop(columns='variable')

    # melt each var_name that isn't rm, merge with melted rm dataframe
    for var_name in var_names:
        value_vars = [col for col in df_lifecycle.columns if var_name in col]

        df_melt_temp = pd.melt(
            df_lifecycle,
            id_vars=id_vars,
            value_vars=value_vars,
            value_name=var_name_mapper[var_name]
        )

        # extract year from variable
        df_melt_temp['year'] = df_melt_temp['variable'].apply(lambda x: x.split('_')[-1])
        df_melt_temp = df_melt_temp.drop(columns='variable')

        # merge with df_melt. Use year if it's in df_melt, otherwise it is the first year and we don't merge with it
        id_vars_merge = id_vars.copy()
        if 'year' in df_melt.columns:
            id_vars_merge.append('year')

        df_melt = df_melt.merge(
            df_melt_temp,
            on=id_vars_merge,
            how='left'
        )

    df_melt = df_melt.dropna(subset='stage')
    df_melt['year'] = df_melt['year'].astype(int)

    # write all output tables
    if write_outputs:
        # set output directory
        out_dir = '/opt/ml/processing/data/product_lifecycle/'
        # check to see if output dir exists, if no create
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        df_melt.to_parquet(
            os.path.join(
                out_dir,
                'product_lifecycle_table_{}.parquet'.format(str(analysis_year))
            ),
            index=False
        )


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input data ingestion data folder', required=True)
    args = parser.parse_args()

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
            compute_product_lifecycle(
                crop_name=crop_name,
                data_sector=ap_data_sector,
                analysis_year=analysis_year,
                data_ingestion_folder=args.s3_input_data_ingestion_folder
            )

    except Exception as e:
        error_event(ap_data_sector, analysis_year, pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
