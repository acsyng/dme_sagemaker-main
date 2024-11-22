# import packages
import os
import sys
import pandas as pd
import numpy as np
import json
import argparse

from libs.event_bridge.event import error_event

import pandas as pd
from portfolio_libs import portfolio_lib
from portfolio_libs import portfolio_sql
from portfolio_libs import ranking_algorithms

from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.postgres.postgres_connection import PostgresConnection

import multiprocessing as mp
import pareto

import pickle
import lightgbm

import time
from libs.config.config_vars import CONFIG, ENVIRONMENT


def get_nondominated_fronts(df_in, meta_cols=['be_bid_other'], objectives=None, eps=None, n_fronts=None,
                            max_feats=['YGSMN']):
    """
    function to get nondominated fronts for rows in a dataframe.

    Inputs:
        df_in: input dataframe. Must be a pandas dataframe.
        meta_cols: columns defining each row. Only information from meta cols and objectives will be kept in output
        objectives: index of columns in the objective function
        eps: To beat another material in a feature, must be better by at least this value
            if value, same eps is used across features.
            if list, eps is for each feature. Must have same length as objectives
            if None, eps defaults to 1e-9
        n_fronts: number of fronts to obtain. None means get all fronts (maximum 10,000 fronts)
        max_feats: features to maximize. All other features will be minimized.
            Need to match column names exactly.
    """

    df_out = df_in.copy()
    df_out['front_idx'] = np.nan

    if meta_cols is None or len(meta_cols) == 0:
        # make an index column
        df_out['idx'] = np.arange(0, df_out.shape[0])
        meta_cols = ['idx']

    if n_fronts is None:
        n_fronts = 10000  # maximum
        do_all_rows = True
    else:
        do_all_rows = False

    # flip sign for some traits
    for idx in objectives:
        if df_out.columns[idx] in max_feats:
            df_out.iloc[:, idx] = -1 * df_out.iloc[:, idx]

    i_front = 0
    all_rows_done = False
    while i_front < n_fronts or (do_all_rows == True and all_rows_done == False):
        df_front = df_out[df_out['front_idx'].isna()]  # remove materials already placed in a front

        if df_front.shape[0] == 0:
            all_rows_done = True
        else:
            # assumes minimization.
            # output contains the feature values in the nondominated set and the index in df_front
            # of nondominated materials (because attribution is true)

            if isinstance(eps, list):
                eps_use = eps
            elif eps is not None:
                eps_use = [eps for i in range(len(objectives))]
            nondominated = pareto.eps_sort(
                tables=df_front.values,
                objectives=objectives,  # columns idx list
                epsilons=eps_use,  # None defaults to 1e-9
                attribution=True  # add row idx to output
            )

            # merge back with df_top, set front idx to i_front for corresponding materials
            nondom_idx = np.array(nondominated)[:, -1].astype(int)
            df_front.iloc[nondom_idx, df_front.columns.get_loc('front_idx')] = i_front

            df_out = df_out.merge(df_front[meta_cols + ['front_idx']], how='left', on=meta_cols)
            df_out['front_idx'] = df_out['front_idx_x']
            df_out.loc[df_out['front_idx'].isna(), 'front_idx'] = df_out.loc[df_out['front_idx'].isna(), 'front_idx_y']
            df_out = df_out.drop(columns=['front_idx_x', 'front_idx_y'])

        # counter for while loop
        i_front = i_front + 1

    # undo flip sign for some traits
    for idx in objectives:
        if df_out.columns[idx] in max_feats:
            df_out.iloc[:, idx] = -1 * df_out.iloc[:, idx]

    return df_out


def get_qualifiers(
    data_sector,
    analysis_year,
    df_to_replace,
    df_trait,
    df_trial,
    df_pvs,
):
    df_rm = df_trait[df_trait['trait'].isin(['v_maturity_subgroup'])].rename(
        columns={'alpha_value': 'rm_estimate'})  # only for commercial products
    df_rm['rm_estimate'] = df_rm['rm_estimate'].astype(float)

    # check for 80,120 scale. If so, convert to 0-8 scale
    if np.sum(df_rm['rm_estimate'] > 50) > np.sum(df_rm['rm_estimate'].notna())*0.5:
        df_rm['rm_estimate'] = (df_rm['rm_estimate'] - 80)/5

    # get RM from dme erm regression
    bebid_search_str = portfolio_lib.make_search_str(df_trial['be_bid'])
    pc = PostgresConnection()
    query_str = """
        select ap_data_sector, entry_id as "be_bid", CAST(rm_estimate as float)  as "erm"
        from dme.rm_estimates re 
        where ap_data_sector = {0}
            and analysis_year = {1}
            and entry_id in {2}
    """.format("'" + data_sector + "'", str(analysis_year), bebid_search_str)

    df_erm = pc.get_data(query_str)
    df_erm = df_erm.groupby(by=['ap_data_sector', 'be_bid']).mean().reset_index()

    # check for 80,120 scale. If so, convert to 0-8 scale
    if np.sum(df_erm['erm'] > 50) > np.sum(df_erm['erm'].notna()) * 0.5:
        df_erm['erm'] = (df_erm['erm'] - 80) / 5

    # also get MRTYN from df_trial
    df_mrtyn = df_pvs[df_pvs['trait'] == 'MRTYN'][['ap_data_sector', 'be_bid', 'prediction']].groupby(
        by=['ap_data_sector', 'be_bid']
    ).mean().reset_index()
    df_mrtyn = df_mrtyn.rename(columns={'prediction': 'MRTYN'})

    # current qualifiers: technology from pvs table, RM distance from many tables
    df_tech = df_pvs[['be_bid', 'technology']].drop_duplicates().groupby(by='be_bid').first().reset_index()

    df_rm = df_rm[['be_bid', 'rm_estimate']].merge(
        df_erm[['be_bid', 'erm']],
        on='be_bid',
        how='outer'
    ).merge(
        df_mrtyn[['be_bid', 'MRTYN']],
        on='be_bid',
        how='outer'
    )
    df_rm = df_rm.groupby(by=['be_bid']).first().reset_index()

    df_rm_comp = df_rm.merge(
        df_rm[df_rm['be_bid'].isin(pd.unique(df_to_replace['be_bid']))],
        how='cross',
        suffixes=('', '_product')
    )

    # make qualifier - check if rm is within some threshold
    df_rm_qual = df_rm_comp.copy()

    for var, thresh in zip(['rm_estimate', 'erm', 'MRTYN'], [0.5, 0.5, 10]):
        df_rm_qual[var + '_qual'] = np.abs(df_rm_qual[var] - df_rm_qual[var + '_product']) < thresh
        df_rm_qual.loc[(df_rm_qual[var].isna()) | (df_rm_qual[var + '_product'].isna()), var + '_qual'] = np.nan
        df_rm_qual = df_rm_qual.drop(columns=[var, var + '_product'])

    # get technology qualifier
    df_tech_comp = df_tech.merge(
        df_tech[df_tech['be_bid'].isin(pd.unique(df_to_replace['be_bid']))],
        how='cross',
        suffixes=('', '_product')
    )

    df_tech_qual = df_tech_comp.copy()
    df_tech_qual['technology_same'] = df_tech_qual['technology'] == df_tech_qual['technology_product']
    df_tech_qual = df_tech_qual.drop(columns=['technology', 'technology_product'])

    # merge tech and rm qualifiers
    df_qual = df_tech_qual.merge(
        df_rm_qual,
        on=['be_bid', 'be_bid_product'],
        how='outer'
    ).drop_duplicates()

    df_qual['qualifier'] = ((df_qual['technology_same'].isna()) | (df_qual['technology_same'] == True)) & (
            (df_qual['rm_estimate_qual'] == True) |
            (df_qual['erm_qual'] == True) |
            (df_qual['MRTYN_qual'] == True)
    )

    return df_qual, df_rm


def epsilon_nondominated_sorting(
        data_sector,
        analysis_year,
        data_ingestion_folder,
        min_placement_year=2023,
        write_outputs=1
):
    """
    TO DO:
        Correct placement model file paths:
            cropfact_mg?
            placement model
            GRM

        Make step in portfolio_pipeline entry, test.

        Clean-up redundant RM code
        package code into functions (placement for example...)
        General clean-up

    inputs:


    outputs:

    """
    if '3.8.' in sys.version or '3.9.' in sys.version:
        n_cpus=1
    else:
        n_cpus=mp.cpu_count()

    start_time=time.time()
    # load in all ingested data.
    # Need : commercial ids, commercial RM, pvs data (AAE/GSE outputs)
    #       lifecycle data, placement data (loaded in placement section)

    df_trial = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'trial_table_{}.parquet'.format(analysis_year)
    ))
    df_ids = pd.read_parquet(os.path.join(
      data_ingestion_folder,
        'material_ids_table_{}.parquet'.format(analysis_year)
    ))
    df_trait = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'material_trait_table_{}.parquet'.format(analysis_year)
    ))
    df_lifecycle = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'product_lifecycle_table_{}.parquet'.format(analysis_year)
    ))

    df_pvs = pd.read_parquet(os.path.join(
        data_ingestion_folder,
        'pvs_table_{}.parquet'.format(analysis_year)
    ))
    df_pvs = df_pvs.rename(
        columns={'entry_identifier': 'be_bid'}
    )

    # load in previously trained placement model
    fpath_timestamp = portfolio_lib.get_latest_timestamp(
        bucket=CONFIG['bucket'],
        fpath='{}/dme/placement/compute_train_data_lgbm/data/train_data_lgbm/{}/{}/'.format(
            ENVIRONMENT, data_sector, analysis_year)
    )
    placement_model = portfolio_lib.load_placement_model_s3(
        bucket=CONFIG['bucket'],
        fpath='{}/dme/placement/compute_train_data_lgbm/data/train_data_lgbm/{}/{}/{}/'.format(
            ENVIRONMENT, data_sector, analysis_year, fpath_timestamp)
    )

    # load in placement model and GRM
    df_grm = pd.read_parquet(
        "s3://{}/{}/dme/placement/compute_pca_output_hetpool1/data/pca_output_hetpool1/{}/{}/hetpool1_pca.parquet".format(
            CONFIG['bucket'], ENVIRONMENT, data_sector, analysis_year)
    )

    get_parents = False
    if np.any(['par2' in feat for feat in placement_model.feature_name_]): # use both hetpool 1 and 2
        get_parents = True
        df_grm2 = pd.read_parquet(
            "s3://{}/{}/dme/placement/compute_pca_output_hetpool2/data/pca_output_hetpool2/{}/{}/hetpool2_pca.parquet".format(
                CONFIG['bucket'], ENVIRONMENT, data_sector, analysis_year)
        )

    # get materials to be replaced -- materials that are in decline or retired but still has sales data.
    # decline = stage 11, phase-out = stage 12, retired = stage 13
    # get products that are not to be replaced (but could replace other materials)

    # merge in bebid to lifecycle
    df_lifecycle = df_lifecycle.merge(
        df_ids[['variety_number', 'be_bid']],
        on='variety_number',
        how='left'
    )

    # products to replace -- generate outputs for many products, even if they're from an earlier stage
    df_to_replace = df_lifecycle[
        (df_lifecycle['year'] == analysis_year) &
        (df_lifecycle['stage'] >= 9)
    ]

    # products that could be used to replace another product
    df_not_replace = df_lifecycle[
        (df_lifecycle['year'] == analysis_year) &
        (df_lifecycle['stage'] < 11)
    ]


    #####################################
    # STEP 1: Qualifiers
    ####################################
    # use RM data and trait groups
    # RM: v_maturity_subgroup, v_relative_maturity, v_maturity_group

    df_qual, df_rm = get_qualifiers(
        data_sector=data_sector,
        analysis_year=analysis_year,
        df_to_replace=df_to_replace,
        df_trait=df_trait,
        df_trial=df_trial,
        df_pvs=df_pvs,
    )

    ####################################
    # Step 2: Process PVS data (pivot, get product <-> material comparisons
    ######################################

    # pivot pvs data to get wide format
    meta_cols = ['ap_data_sector', 'analysis_year', 'analysis_type', 'source_id', 'technology', 'decision_group_rm']
    value_cols = ['prediction']

    # drop GenoPred
    df_pvs = df_pvs[df_pvs['analysis_type'] != 'GenoPred']

    # pivot PVS table
    df_pvs_piv = pd.pivot_table(
        df_pvs,
        values=value_cols,
        columns='trait',
        index=meta_cols + ['be_bid']
    ).reset_index()

    df_pvs_piv.columns = ['_'.join(filter(None, col)) for col in df_pvs_piv.columns]

    ### prepare data for one-sided distance calculation and yield comparison.
    # get agronomic traits
    # which agronomic traits to care about -- get from rv_ap_sector_trait_config
    # get non-text traits
    # get YGSMN here as well so that it goes through the same comparison process as agronomic traits
    query_str = """
        SELECT distinct trait as "trait"
        from RV_AP_SECTOR_TRAIT_CONFIG 
        where ap_data_sector_name = {0}
        and analysis_year = {1}
        and yield_trait = 0
        and distribution_type <> 'text'
        and trait not in ('MRTYN','RMT_N')
    """.format("'" + data_sector + "'", str(analysis_year))

    with SnowflakeConnection() as sc:
        agro_traits = sc.get_data(query_str).values.reshape((-1,))

    # get agronomic trait comparisons
    cols_keep = meta_cols.copy()
    cols_keep.append('be_bid')
    agro_traits_use = ['prediction_' + trait for trait in agro_traits if 'prediction_' + trait in df_pvs_piv.columns]
    yield_trait = 'prediction_YGSMN'

    cols_keep.extend(agro_traits_use)
    cols_keep.append(yield_trait)

    df_pvs_agro = df_pvs_piv[cols_keep]

    # Clip outliers, normalize to put data on same scale, fill nan's with median
    numeric_cols = [col for col in df_pvs_agro if
                    'prediction_' in col and np.issubdtype(df_pvs_agro[col].dtype, np.number)]
    df_pvs_agro_norm = df_pvs_agro.copy()

    df_pvs_agro_norm[numeric_cols] = df_pvs_agro[numeric_cols].clip(
        lower=df_pvs_agro[numeric_cols].quantile(0.05).values,
        upper=df_pvs_agro[numeric_cols].quantile(0.95).values,
        axis=1
    )
    # normalize each column to put on same scale
    # rating traits need to be normalized by 1-9 ranking
    rating_traits = [trait for trait in agro_traits_use if trait[-1] == 'R']
    numeric_traits = [trait for trait in agro_traits_use if trait[-1] != 'R']

    df_pvs_agro_norm[rating_traits] = df_pvs_agro_norm[rating_traits] - np.mean([1, 2, 3, 4, 5, 6, 7, 8, 9]) / np.std(
        [1, 2, 3, 4, 5, 6, 7, 8, 9])
    df_pvs_agro_norm[numeric_traits] = (df_pvs_agro_norm[numeric_traits] - df_pvs_agro_norm[numeric_traits].mean()) / \
                                       df_pvs_agro_norm[numeric_traits].std()

    # do not normalize yield. Will eventually do a percent product normalization

    # missing values...
    # compute median across grouping cols, fill in later
    group_cols = ['ap_data_sector', 'analysis_year', 'analysis_type', 'technology']
    df_fill_vals = df_pvs_agro_norm[group_cols + list(numeric_cols)].groupby(by=group_cols).median().reset_index()

    print("time loading initial data: ", time.time()-start_time)

    # get ratings per trait, do this per decisiong group and technology
    # score value
    # do this for each trait
    traits = [col.replace('prediction_', '') for col in df_pvs_agro_norm.columns if 'prediction_' in col]

    inputs = [(
        df_pvs_agro_norm,
        'prediction_' + trait,
        ['analysis_year', 'analysis_type', 'technology', 'decision_group_rm'])
        for trait in traits
    ]

    start_time = time.time()
    ctx = mp.get_context("spawn")
    with ctx.Pool(n_cpus) as pool:  # process inputs (different traits) iterable with pool
        df_out_parallel = pool.starmap(ranking_algorithms.run_ranking_algorithms, inputs)

    df_ratings = pd.DataFrame()
    # combine outputs
    for i, trait in enumerate(traits):
        df_out = df_out_parallel[i]
        df_out = df_out.rename(columns={'rating': 'rating_' + trait, 'ranking': 'ranking_' + trait})
        df_out = df_out.drop(columns=['ranking_' + trait])
        # we only used a single trait, so aggregate and that trait are the same.
        df_out = df_out[df_out['trait'] == 'aggregate']

        # merge across traits
        if df_ratings.shape[0] == 0:
            df_ratings = df_out[['be_bid', 'analysis_type', 'technology', 'decision_group_rm', 'rating_' + trait]]
        else:
            df_ratings = df_ratings.merge(
                df_out[['be_bid', 'analysis_type', 'technology', 'decision_group_rm', 'rating_' + trait]],
                on=['be_bid', 'analysis_type', 'technology', 'decision_group_rm'],
                how='outer'
            )

    print("General ratings:", time.time()-start_time)

    # also do aggregation for each product and materials that qualify for that product
    # only use data from source ids that the product is in. Use all data from said source_ids
    # for only materials that qualify.
    i_count = 0
    df_ratings_product_list = []
    for index, df_grp in df_qual.groupby(by='be_bid_product'):
        print(i_count)
        be_bids_qual = df_grp['be_bid'][df_grp['qualifier'] == True]
        # source_ids_product = df_pvs_agro_norm['source_id'][df_pvs_agro_norm['be_bid'] == index]
        df_pvs_agro_norm_grp = df_pvs_agro_norm[(df_pvs_agro_norm['be_bid'].isin(
            be_bids_qual))]  # & (df_pvs_agro_norm['source_id'].isin(source_ids_product))]

        if df_pvs_agro_norm_grp.shape[0] > 0:
            input_traits = [col.replace('prediction_', '') for col in df_pvs_agro_norm_grp.columns if
                            'prediction_' in col]
            inputs = [(
                df_pvs_agro_norm_grp,
                col,
                ['analysis_year', 'analysis_type'])
                for col in df_pvs_agro_norm_grp.columns if 'prediction_' in col
            ]

            with ctx.Pool(n_cpus) as pool:  # process inputs (different traits) iterable with pool
                df_out_parallel = pool.starmap(ranking_algorithms.run_ranking_algorithms, inputs)

            df_ratings_temp = pd.DataFrame()
            # combine outputs
            for i, trait in enumerate(input_traits):
                df_out = df_out_parallel[i]
                if df_out is not None and df_out.shape[0] > 0:
                    df_out = df_out.rename(columns={'rating': 'rating_' + trait, 'ranking': 'ranking_' + trait})
                    df_out = df_out.drop(columns=['ranking_' + trait])
                    # we only used a single trait, so aggregate and that trait are the same.
                    df_out = df_out[df_out['trait'] == 'aggregate']

                    # merge across traits
                    if df_ratings_temp.shape[0] == 0:
                        df_ratings_temp = df_out[['be_bid', 'analysis_type', 'rating_' + trait]]
                    else:
                        df_ratings_temp = df_ratings_temp.merge(
                            df_out[['be_bid', 'analysis_type', 'rating_' + trait]],
                            on=['be_bid', 'analysis_type'],
                            how='outer'
                        )

            # combine across products
            df_ratings_temp['product_group'] = index
            df_ratings_product_list.append(df_ratings_temp)
        i_count = i_count + 1

    print("Per Product ratings:", time.time() - start_time)

    df_ratings_product = pd.concat(df_ratings_product_list, axis=0)

    df_ratings_product['technology'] = 'all'
    df_ratings_product['decision_group_rm'] = -100

    df_ratings = pd.concat((df_ratings, df_ratings_product), axis=0)

    # append ratings to df_pvs_agro_norm
    df_ratings['source_id'] = 'rating'
    df_ratings['ap_data_sector'] = data_sector
    df_ratings['analysis_year'] = analysis_year

    df_ratings = df_ratings.rename(columns={
        'rating_' + trait: 'prediction_' + trait for trait in traits
    })

    df_agro_norm = pd.concat((df_pvs_agro_norm, df_ratings), axis=0)

    # make comparison matrix
    comp_cols = meta_cols.copy()
    comp_cols.append('product_group')
    df_agro_comp = df_agro_norm.merge(
        df_agro_norm[df_agro_norm['be_bid'].isin(pd.unique(df_to_replace['be_bid']))],
        on=comp_cols,
        how='inner',
        suffixes=('', '_product')
    )
    # remove products that don't match product_group, if product_group is not nan
    df_agro_comp = df_agro_comp[
        (df_agro_comp['product_group'].isna()) | (df_agro_comp['be_bid_product'] == df_agro_comp['product_group'])]
    df_agro_comp = df_agro_comp.drop(columns=['product_group'])
    # remove cases where comparison is between same be_bids
    df_agro_comp = df_agro_comp[(df_agro_comp['be_bid'] != df_agro_comp['be_bid_product'])]

    agg_cols = comp_cols.copy()
    agg_cols.remove('product_group')
    agg_cols.extend(['be_bid', 'be_bid_product'])

    df_agro_comp = df_agro_comp.groupby(by=agg_cols).mean().reset_index()

    print("PVS comparison:", time.time() - start_time)
    ###################################
    # STEP 3: get placement predictions
    ###################################

    # get placement model predictions...
    # would like to use spatial region where a product is sold...
    # failing that, use maturity bands. Grab adjacent maturity groups too
    # weigh by viable acres.

    # treat this as a forward model problem, despite having some predictions from the placement model
    # we may want predictions for materials in different maturity bands and for a subset of years, so just predict all of it
    # only can predict for materials that we have genetics for

    
            #items needed : 
            #    placement model 
            #    crop fact inputs for grid
            #        Maturity group for each grid cell
            #        acres for each grid cell
            #    geno PCA inputs

    # get be bids if we don't have them already
    if 'sample_id' in df_grm.columns and 'be_bid' not in df_grm.columns:
        df_map1, df_map2 = portfolio_sql.get_bebid_sample_id_map(
            bebids=pd.unique(df_pvs_piv['be_bid']),
            get_parents=get_parents
        )

        # merge with df grm, rename columns to match model
        if get_parents:
            df_grm = df_grm.merge(
                df_map1[['be_bid', 'par_hp1_sample']].rename(columns={'par_hp1_sample': 'sample_id'}),
                on='sample_id',
                how='inner'
            ).rename(columns={
                col: col + '_par1' for col in df_grm.columns if 'comp_' in col
            })
            df_grm2 = df_grm2.merge(
                df_map2[['be_bid', 'par_hp2_sample']].rename(columns={'par_hp2_sample': 'sample_id'}),
                on='sample_id',
                how='inner'
            ).rename(columns={
                col: col + '_par2' for col in df_grm2.columns if 'comp_' in col
            })

            # merge grm2 into grm using be_bid
            df_grm = df_grm.merge(
                df_grm2,
                on=['be_bid'],
                how='outer'
            ).drop(columns=['sample_id_x', 'sample_id_y'])
        else:
            df_grm = df_grm.merge(
                df_map1,
                on='sample_id',
                how='inner'
            ).rename(columns={
                col: col + '_par1' for col in df_grm.columns if 'comp_' in col
            })

    # get maturities for each product,
    # make predictions for product and qualifying materials in the corresponding maturity groups.
    df_rm_prod = df_rm[df_rm['be_bid'].isin(df_to_replace['be_bid'])]

    if data_sector == 'SOY_NA_SUMMER':
        rms = [float(rm) for rm in [0, 1, 2, 3, 4, 5, 6, 7]]
        rm_col = 'rm_estimate'
        group_cols = ['year', 'place_id', 'irrigation', 'be_bid', 'maturity',
                      'sub_c_code', 'lat', 'lon','maturity_acres','ecw', 'irrigated_pct']
    elif data_sector == 'CORN_NA_SUMMER':
        rms = ['MG' + str(i) for i in range(9)]
        rm_col = 'erm'
        group_cols = ['year', 'place_id', 'irrigation',
                      'be_bid', 'maturity', 'sub_c_code', 'lat',
                      'lon', 'maturity_acres', 'tpp', 'wce', 'irrigated_pct']

    rm_mask = np.zeros((df_rm_prod.shape[0], len(rms)))

    for i_prod in range(df_rm_prod.shape[0]):
        for i_rm in range(len(rms)):
            rm_use = rms[i_rm]
            if isinstance(rms[i_rm], str):
                rm_use = float(''.join(i for i in rm_use if i.isalpha() == False))

            if np.abs(df_rm_prod.iloc[i_prod][rm_col] - rm_use) <= 1:
                rm_mask[i_prod, i_rm] = 1

    for i_rm, rm in enumerate(rms):
        df_rm_prod.loc[:, rm] = rm_mask[:, i_rm]
    # setting up input to model, then make all predictions at once.
    # then aggregate and compare.

    # load in a subset of cropfact/soil data from previously computed table
    fpath_timestamp = portfolio_lib.get_latest_timestamp(
        bucket=CONFIG['bucket'],
        fpath='{}/dme/placement/compute_cropFact_grid_for_model/data/cropFact_grid_for_model/{}/'.format(
            ENVIRONMENT, data_sector
        )
    )
    cropfact_fname_pref = \
        's3://{}/{}/dme/placement/compute_cropFact_grid_for_model/data/cropFact_grid_for_model/{}/{}/'.format(
            CONFIG['bucket'], ENVIRONMENT, data_sector, fpath_timestamp
    )

    # for each product, get comparisons to materials. Get placement predictions, then do comparisons
    # break up by each product to reduce space requirements
    # compute win %, mean y_pred, etc. within sub_c_code and across all data
    # do this per year, then aggregate to whole year

    df_yield_comp = []
    for prod_bid in pd.unique(df_rm_prod['be_bid']):
        df_input_list = []
        # get crop fact data for eligible materials and the product
        eligible_mats = pd.unique(
            df_qual[(df_qual['qualifier'] == True) & (df_qual['be_bid_product'] == prod_bid)][
                'be_bid'])
        eligible_mats = np.append(eligible_mats, prod_bid)
        eligible_mats = pd.unique(eligible_mats)

        for rm in rms:
            # cast integer as float to match s3 file name
            cropfact_fname_load = cropfact_fname_pref + '{}/cropFact_grid_for_model.parquet'.format(rm)
            cropfact_fname_check = '{}/dme/placement/compute_cropFact_grid_for_model/data/cropFact_grid_for_model/{}/{}/{}/cropFact_grid_for_model.parquet'.format(
                ENVIRONMENT, data_sector, fpath_timestamp, rm
            )
            if df_rm_prod.loc[df_rm_prod['be_bid'] == prod_bid, rm].values == 1 and \
                    portfolio_lib.check_if_file_exists(fname=cropfact_fname_check, read_from_s3=1,
                                                       bucket=CONFIG['bucket']):
                df_cropfact = pd.read_parquet(cropfact_fname_load, filters=[('year', '>=', min_placement_year)])

                # configure GRM and cropfact/soil data as inputs for model
                df_cropfact = df_cropfact.merge(df_grm[df_grm['be_bid'].isin(eligible_mats)], how='cross')

                df_input_list.append(df_cropfact)

        if len(df_input_list) > 0:
            df_placement = pd.concat(df_input_list, axis=0)
            df_input_list = []  # remove df_input_list from memory?

            # generate predictions
            df_placement['y_pred'] = placement_model.predict(df_placement[placement_model.feature_name_])

            # if latitude and longitude aren't in dataframe, parse place_id for them
            if 'lat' not in df_placement.columns or 'lon' not in df_placement.columns:
                # split by underscore. [0] = "gridpoint", [1] = latitude, [2] = longitude
                df_placement['lat'] = df_placement['place_id'].apply(lambda x: float(x.split('_')[1]))
                df_placement['lon'] = df_placement['place_id'].apply(lambda x: float(x.split('_')[2]))

            # aggregate placement predictions to some market segment level.
            # start by aggregating for the same 10x10km grid point for the same be_bid (this is due to multiple sample_ids associating with the same be_bid)
            pred_col = 'y_pred'
            df_placement = df_placement[group_cols + [pred_col]].groupby(by=group_cols).mean().reset_index()

            # make comparisons between materials and products.
            # aggregate up to sub_c_code across irrigation

            # first aggregate data within a grid point across irrigation
            # weight by irrigated_pct.
            df_placement['irr_weight'] = df_placement['irrigated_pct']
            df_placement.loc[df_placement['irrigation'] != 'IRRIGATED', 'irr_weight'] = 1 - df_placement.loc[
                df_placement['irrigation'] != 'IRRIGATED', 'irr_weight']
            df_placement['y_pred_weighted'] = df_placement['y_pred'] * df_placement['irr_weight']

            df_placement = df_placement.groupby(
                by=['year', 'place_id', 'be_bid', 'sub_c_code', 'lat', 'lon']
            ).agg(
                {
                    'y_pred': 'mean',
                    'y_pred_weighted': 'sum',
                    'maturity_acres': 'sum',
                    'irr_weight': 'sum'
                 }
            ).reset_index()
            df_placement['y_pred_agg'] = df_placement['y_pred_weighted'] / df_placement['irr_weight']

            df_placement = df_placement.drop(columns=['irr_weight', 'y_pred_weighted'])

            if df_placement.shape[0] > 0:
                df_placement_comp = df_placement.merge(
                    df_placement[df_placement['be_bid'] == prod_bid],
                    on=['year', 'place_id', 'sub_c_code', 'lat', 'lon', 'maturity_acres'],
                    how='inner',
                    suffixes=('', '_product')
                )

                df_placement_comp = df_placement_comp[
                    df_placement_comp['be_bid'] != df_placement_comp['be_bid_product']]

                df_placement_comp['mat_wins'] = (df_placement_comp['y_pred_agg'] > df_placement_comp[
                    'y_pred_agg_product']) * df_placement_comp['maturity_acres']
                df_placement_comp['y_pred_agg_weighed'] = df_placement_comp['y_pred_agg'] * df_placement_comp[
                    'maturity_acres']
                df_placement_comp['y_pred_agg_product_weighed'] = df_placement_comp['y_pred_agg_product'] * \
                                                                  df_placement_comp['maturity_acres']

                # aggregate up to sub_c_codes
                df_placement_sub_c_code = df_placement_comp.groupby(by=['be_bid', 'be_bid_product', 'sub_c_code']).agg(
                    {var: 'sum' for var in
                     ['mat_wins', 'y_pred_agg_weighed', 'y_pred_agg_product_weighed', 'maturity_acres']}).reset_index()
                for var in ['mat_wins', 'y_pred_agg_weighed', 'y_pred_agg_product_weighed']:
                    df_placement_sub_c_code[var] = df_placement_sub_c_code[var] / df_placement_sub_c_code[
                        'maturity_acres']

                # aggregate all data
                df_placement_all = df_placement_comp.groupby(by=['be_bid', 'be_bid_product']).agg(
                    {var: 'sum' for var in
                     ['mat_wins', 'y_pred_agg_weighed', 'y_pred_agg_product_weighed', 'maturity_acres']}
                ).reset_index()
                for var in ['mat_wins', 'y_pred_agg_weighed', 'y_pred_agg_product_weighed']:
                    df_placement_all[var] = df_placement_all[var] / df_placement_all['maturity_acres']

                # combine and store outputs
                df_placement_all['sub_c_code'] = 'all'

                df_yield_comp.append(pd.concat((df_placement_sub_c_code, df_placement_all), axis=0))

    df_yield_comp = pd.concat(df_yield_comp, axis=0)

    print("Placement predictions and comparisons:", time.time() - start_time)
    # get percent product
    df_yield_comp['y_pred_perc'] = 100 * df_yield_comp['y_pred_agg_weighed'] / df_yield_comp[
        'y_pred_agg_product_weighed']

    # RM comparisons
    # merge df_rm with commercial products and df_rm with all be bids
    cols_keep = ['be_bid', 'rm_estimate', 'erm', 'MRTYN']
    df_comm_rm = df_rm[df_rm['be_bid'].isin(df_to_replace['be_bid'])]
    df_rm_comp = df_comm_rm[cols_keep].merge(
        df_rm[cols_keep],
        how='cross',
        suffixes=('_product', '')
    ).groupby(by=['be_bid','be_bid_product']).mean().reset_index()

    # get RM range based on decision groups for trialing data, will have some commercial products.
    # use range to determine if one material is a suitable replacement for another.
    # if trialed in same decision group, suitable. If same RM range, then suitable
    # also get a distance between RM
    for col in ['rm_estimate','erm','MRTYN']:
        df_rm_comp[col+'_dist'] = df_rm_comp[col] - df_rm_comp[col+'_product']

    # get pareto-non dominated fronts for each comparison.
    # Need to merge in yield comparison data...
    # yield can come from 2 sources : pvs data and placement model.
    # pvs data comes from same pipeline as agronomic data with same comparisons

    # for pvs data, compute one-sided agronomic distance, RM distance, yield distance.

    # for placement data, include yield spatial similarity component

    # for each product, only consider traits that are well-populated.
    # leave unconsidered traits as nan, fill in considered but missing with median value
    print("RM and cleanup:", time.time() - start_time)
    df_out = pd.DataFrame()
    print("start epsilon non dominated sorting")
    for analysis_type in ['MultiExp','SingleExp','MynoET']:
        for yield_method in ['blups', 'placement']:
            # get yield data based on method
            if yield_method == 'placement':
                df_yield_use = df_yield_comp[df_yield_comp['sub_c_code'] == 'all'].copy()
                yield_cols = ['mat_wins', 'y_pred_perc']
            elif yield_method == 'blups':
                df_yield_use = df_agro_comp[
                    (df_agro_comp['analysis_type'] == analysis_type) & (df_agro_comp['source_id'] == 'rating') &
                    (df_agro_comp['decision_group_rm'] == -100)
                    ].copy()

                df_yield_use['YGSMNd'] = df_yield_use['prediction_YGSMN'] - df_yield_use['prediction_YGSMN_product']
                yield_cols = ['YGSMNd']

            df_yield_use = df_yield_use.merge(
                df_qual[df_qual['qualifier'] == True][['be_bid', 'be_bid_product']],
                on=['be_bid', 'be_bid_product'],
                how='inner'
            )

            # get agronomic data
            df_agro_use = df_agro_comp[
                (df_agro_comp['analysis_type'] == analysis_type) * (df_agro_comp['source_id'] == 'rating') & (
                            df_agro_comp['decision_group_rm'] == -100)]

            df_agro_use = df_agro_use.merge(
                df_qual[df_qual['qualifier'] == True][['be_bid', 'be_bid_product']],
                on=['be_bid', 'be_bid_product'],
                how='inner'
            )

            ### for each product and group, get non-dominated pareto fronts
            group_cols = ['ap_data_sector', 'analysis_year', 'analysis_type', 'source_id', 'technology',
                          'decision_group_rm', 'be_bid_product']

            df_out_method = []
            for idx, df_grp in df_agro_use.groupby(group_cols):
                df_temp = df_grp.copy()

                # remove columns where product is missing
                traits_use = []
                traits_remove = []
                df_count = df_temp.count()
                for col in df_count.index:
                    if 'prediction_' in col:
                        if ('_product' in col and df_count[col] <= 0) or (df_count[col] < df_temp.shape[0] / 3):
                            traits_remove.append(col.replace('_product', '').replace('prediction_', ''))

                traits_remove = list(set(traits_remove))
                cols_remove = ['prediction_' + trait for trait in traits_remove]
                cols_remove.extend(['prediction_' + trait + '_product' for trait in traits_remove])

                for col in df_count.index:
                    if 'prediction_' in col and col.replace('_product', '').replace('prediction_', '') not in traits_remove:
                        traits_use.append(col.replace('_product', '').replace('prediction_', ''))

                traits_use = list(set(traits_use))

                if 'YGSMN' in traits_use:
                    traits_use.remove('YGSMN')

                df_temp = df_temp.drop(columns=cols_remove)

                # compute agronomic distance as one-sided euclidean distance
                # if material is better than product, set distance for that trait as 0
                # otherwise use squared distance
                # for plant height and MRTYN, use squared distance regardless of direction
                for trait in traits_use:
                    df_temp['prediction_' + trait + '_dist'] = df_temp['prediction_' + trait] - df_temp[
                        'prediction_' + trait + '_product']
                    if trait == 'PLHTN' or trait == 'MRTYN': # squared dist regardless of direction
                        df_temp['prediction_' + trait + '_dist'] = df_temp['prediction_' + trait + '_dist'] ** 2
                    else: # squared dist only if material is worse than product
                        mask = df_temp['prediction_' + trait + '_dist'] > 0
                        df_temp.loc[mask, 'prediction_' + trait + '_dist'] = \
                            df_temp.loc[mask, 'prediction_' + trait + '_dist'] ** 2
                        df_temp.loc[mask == False, 'prediction_' + trait + '_dist'] = 0

                df_temp['agro_dist'] = np.sqrt(
                    df_temp[['prediction_' + trait + '_dist' for trait in traits_use]].sum(axis=1).values.astype(float)
                )

                # merge in placement predictions and RM
                df_dist = df_temp.merge(
                    df_yield_use[['be_bid', 'be_bid_product'] + yield_cols],
                    on=['be_bid', 'be_bid_product'],
                    how='inner'
                ).merge(
                    df_rm_comp[['be_bid', 'be_bid_product', rm_col+'_dist']],
                    how='left',
                    on=['be_bid', 'be_bid_product']
                )[['be_bid', 'be_bid_product', 'agro_dist', rm_col+'_dist'] + yield_cols]

                if df_dist.shape[0] > 0:
                    df_dist['rm_abs_dist'] = np.abs(df_dist[rm_col+'_dist'])
                    # set traits of interest
                    # Filter materials by yield -- get top perc_top materials
                    # Normalize each trait
                    # get first n_fronts via nondominanted epsilon sorting

                    n_fronts = None  # get front for all materials
                    eps = 0.1

                    # need to flip sign for some traits
                    max_feats = yield_cols

                    # pull in adv rec score data
                    df_top = df_dist.copy()
                    df_top = df_top.drop_duplicates()

                    # remove materials based on thresholds, set yield variable
                    pareto_traits = ['agro_dist','rm_abs_dist']
                    if yield_method == 'placement':
                        pareto_traits.append('y_pred_perc')
                        df_top = df_top.loc[
                            (df_top['y_pred_perc'] >= 100) & (df_top['agro_dist'] <= 5) & (df_top['rm_abs_dist'] <= 1.5)
                        ]
                    elif yield_method == 'blups':
                        pareto_traits.append('YGSMNd')
                        df_top = df_top.loc[
                            (df_top['YGSMNd'] >= 0) & (df_top['agro_dist'] <= 5) & (df_top['rm_abs_dist'] <= 1.5)
                        ]

                    # get first n_fronts.
                    df_top = get_nondominated_fronts(
                        df_top,
                        meta_cols=['be_bid', 'be_bid_product'],
                        objectives=[i for i, col in enumerate(df_top.columns) if col in pareto_traits],
                        eps=eps,
                        n_fronts=n_fronts,
                        max_feats=max_feats
                    )

                    # merge back with full list
                    df_top = df_top[['be_bid', 'front_idx']].merge(df_dist, on='be_bid', how='right')

                    # append to output
                    df_out_method.append(df_top)

            df_out_method = pd.concat(df_out_method, axis=0)
            df_out_method.loc[df_out_method['front_idx'].isna(), 'front_idx'] = np.max(df_out_method['front_idx']) + 10

            df_out_method = df_out_method.rename(columns={'front_idx': yield_method + '_pareto'})

            df_out_method['analysis_type'] = analysis_type

            # merge with df_out
            if df_out.shape[0] == 0:
                df_out = df_out_method.copy()
            else:
                df_out = df_out.merge(
                    df_out_method[['be_bid', 'be_bid_product', yield_method + '_pareto', ] + yield_cols],
                    on=['be_bid', 'be_bid_product'],
                    how='outer'
                )

    # merge in variety number
    df_out = df_out.merge(
        df_ids[['be_bid','variety_number']].drop_duplicates().rename(
            columns={col : col+'_product' for col in ['be_bid','variety_number']}
        ),
        on=['be_bid_product'],
        how='left'
    )
    print("Done with pareto:", time.time() - start_time)
    # write all output tables
    if write_outputs:
        # set output directory
        out_dir = '/opt/ml/processing/data/nondominated_fronts/'
        # check to see if output dir exists, if no create
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        df_out.to_parquet(os.path.join(
            out_dir, 'nondominated_sorting_{}.parquet'.format(analysis_year)
        ), index=False)
        df_agro_comp.to_parquet(os.path.join(
            out_dir, 'PVS_comparisons_{}.parquet'.format(analysis_year)
        ), index=False)
        df_yield_comp.to_parquet(os.path.join(
            out_dir, 'placement_comparisons_{}.parquet'.format(analysis_year)
        ), index=False)
        df_rm_comp.to_parquet(os.path.join(
            out_dir, 'RM_comparisons_{}.parquet'.format(analysis_year)
        ), index=False)
        df_agro_norm.to_parquet(os.path.join(
            out_dir, 'pvs_output_{}.parquet'.format(analysis_year)
        ), index=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input data ingestion data folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        data_sector = data['ap_data_sector']
        analysis_year = int(data['analysis_year'])
        pipeline_runid = data['target_pipeline_runid']

    try:

        epsilon_nondominated_sorting(
            analysis_year=analysis_year,
            data_sector=data_sector,
            data_ingestion_folder=args.s3_input_data_ingestion_folder,
            min_placement_year=2021
        )

    except Exception as e:
        error_event(data_sector, analysis_year, pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
