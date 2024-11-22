# from dataiku.core.managed_folder import Folder
from functools import partial
from multiprocessing import Pool

import fasttreeshap
import numpy as np
import pandas as pd

"""
this python library aims to summarize important variables of a model based on shap values by the following steps:
1. get binned values for all predictors;
2. calculate shap value using the trained model;
3. summarize most important variables based on shap values that are aggregated  at different user-defined levels; 
4. reshap the wide summary table from step 3 to a tall table used by UI, and 
5. post processing for UI.
"""

"""
shap_ui_flow: main function to wrap several funtions/steps together for deriving shap data for UI.
input:
    df_outA: panda dataframe,  the aggregated data that the shap analysis will be based on;
    estimator: model object, the model for calculating shap;
    id_columns: list of string, column names for all identifiers;
    analysis_year: integer;
    ap_data_sector: string;
    n_top: integer, number of top positive/negative influencers; 
    n_job: integer, number of process to use to calculate shap ;
    aggregateion_levels: list of list objects, each list includes the column names that aggregate will be based on.
output:
    ui_format: the tall table for UI.
"""


def shap_ui_flow(df_outA, estimator, id_columns, analysis_year, ap_data_sector, n_top=10, n_job=8,
                 aggregation_levels=[['Global'], ['maturity'], ['maturity', 'region_name', 'region_type'], ['be_bid']],
                 column_orders=['region_type', 'region_name', 'be_bid', 'analysis_year',
                                'ap_data_sector', 'maturity',
                                'irrigation',
                                'feature_name', 'feature_rank', 'feature_direction',
                                'feature_value_category', 'feature_shap_value', 'feature_shap_value_5',
                                'feature_shap_value_25', 'feature_shap_value_50',
                                'feature_shap_value_75', 'feature_shap_value_95', 'aggregate_level']):
    # calculate shap values and get mean and quantiles based on id_columns
    dt_shapA = calculate_shap(df_outA, estimator, id_columns=id_columns + ['year'], n_job=n_job)
    # Get binned variable
    bin_results_bid = get_binned_variables(df_outA, estimator.feature_name_, id_columns + ['year'], n_bins=6,
                                           n_process=6)

    print('dt_shapA.columns.values: ', dt_shapA.columns.values)
    print()
    print('id_columns: ', id_columns)
    print()
    print('dt_shapA index', dt_shapA.index)

    # get aggregated shap value based on id_columns;
    dt_shap = dt_shapA.groupby(id_columns).mean().reset_index()
    # get aggregated shap value dataframe that only include predictors
    dt_shap_out = dt_shap[estimator.feature_name_]

    # Get shap quantile based on regional level, change id_columns2 to change definition of regional level
    id_columns2 = list(set(id_columns) - set(['be_bid']))
    dt_shap_5q_R = dt_shapA.groupby(id_columns2).quantile(0.05, numeric_only=True).reset_index()
    dt_shap_25q_R = dt_shapA.groupby(id_columns2).quantile(0.25, numeric_only=True).reset_index()
    dt_shap_50q_R = dt_shapA.groupby(id_columns2).quantile(0.5, numeric_only=True).reset_index()
    dt_shap_75q_R = dt_shapA.groupby(id_columns2).quantile(0.75, numeric_only=True).reset_index()
    dt_shap_95q_R = dt_shapA.groupby(id_columns2).quantile(0.95, numeric_only=True).reset_index()
    dt_shap_5q = pd.merge(dt_shap[id_columns], dt_shap_5q_R, on=id_columns2, how='left')
    dt_shap_25q = pd.merge(dt_shap[id_columns], dt_shap_25q_R, on=id_columns2, how='left')
    dt_shap_50q = pd.merge(dt_shap[id_columns], dt_shap_50q_R, on=id_columns2, how='left')
    dt_shap_75q = pd.merge(dt_shap[id_columns], dt_shap_75q_R, on=id_columns2, how='left')
    dt_shap_95q = pd.merge(dt_shap[id_columns], dt_shap_95q_R, on=id_columns2, how='left')

    # Aggregate original input dataset based on id_columns
    train_data_df = df_outA.groupby(id_columns).mean().reset_index()
    # Binned data based on id_columns, and round output to ensure output data values are all integer
    bin_results_bid = bin_results_bid.groupby(id_columns).mean().round(0).reset_index()

    # Sort data to ensure all data are have the same order based on id_columns
    train_data_df = train_data_df.sort_values(id_columns)
    bin_results_bid = bin_results_bid.sort_values(id_columns)
    dt_shap = dt_shap.sort_values(id_columns)
    dt_shap_5q = dt_shap_5q.sort_values(id_columns)
    dt_shap_25q = dt_shap_25q.sort_values(id_columns)
    dt_shap_50q = dt_shap_50q.sort_values(id_columns)
    dt_shap_75q = dt_shap_75q.sort_values(id_columns)
    dt_shap_95q = dt_shap_95q.sort_values(id_columns)
    train_data_df.index = bin_results_bid.index = dt_shap.index = dt_shap_5q.index = dt_shap_95q.index = dt_shap_25q.index = dt_shap_50q.index = dt_shap_75q.index = range(
        train_data_df.shape[0])

    # get top influencers
    predictors = list(dt_shap_out.columns)
    shap_summary = get_top_influencers(dt_shap_out, dt_shap_5q, dt_shap_25q, dt_shap_50q, dt_shap_75q, dt_shap_95q,
                                       bin_results_bid, predictors, aggregate_name='aggregate_level',
                                       id_columns=id_columns, aggregation_levels=aggregation_levels, n_top=n_top,
                                       positive_prefix='TPI_', negative_prefix='TNI_')

    # reformat data for UI input
    ui_format = ui_reformat_tall_table(shap_summary, aggregate_name='aggregate_level',
                                       id_vars=id_columns + ['aggregate_level'])

    # post-processing to ensure the output comply to the UI data schema
    ui_format = ui_post_processing(ui_format, analysis_year=analysis_year, ap_data_sector=ap_data_sector,
                                   column_order=column_orders)  # ['region_type', 'region_name', 'be_bid', 'analysis_year',
    # 'ap_data_sector',  # 'maturity',
    # 'irrigation',
    # 'feature_name', 'feature_rank', 'feature_direction',
    # 'feature_value_category', 'feature_shap_value', 'feature_shap_value_5',
    # 'feature_shap_value_25', 'feature_shap_value_50',
    # 'feature_shap_value_75', 'feature_shap_value_95', 'aggregate_level'])

    return ui_format


##########******** Function block 1:  derive categorical variables for continous variables with quantile binning ********##########

"""
init_worker_bin function: used inside get_binned_variables function for multiprocessing with multiprocessing pacakge to pass the dataset (i.e. data) to all workers.
data: a panda dataframe with predictors for binning   
"""


def init_worker_bin(data):
    global dta
    dta = data


"""
getQuantileBins: get binned variable based on quantile distribution, used inside get_binned_variables function;
                  however, if number of unique value for the variable is =< n_bins, the binning is not conducted. 
input:
    nm: string, the name of the variable that will be processed to a binned categorical variable;
    n_bins: integer, number of bin/level/category for binning
output: 
    x_bin: a single column panda dataframe (named as nm) for binned values of the predictor (i.e. nm);
         
"""


def getQuantileBins(nm, n_bins=10):
    x = dta[nm]
    x2 = dta[[nm]].copy()
    if x2[nm].nunique() <= n_bins:
        x_bin = dta[[nm]]
    else:
        try:
            if len(x.shape) > 1:
                x = x.reshape((-1,))
            bin_edges = np.nanpercentile(x, 100 * np.arange(0, n_bins + 1, 1) / n_bins)
            # print(nm,  bin_edges)
            x_bin = np.digitize(x, bin_edges)  # 1 is the first bin
            x_bin = x_bin - 1
            # check bounds
            x_bin[x_bin > n_bins - 1] = n_bins - 1
            x_bin[x_bin < 0] = 0
            x_bin = pd.DataFrame(x_bin, columns=[nm])
            x_bin[nm] = x_bin[nm] + 1
            x_bin.index = range(x_bin.shape[0])
        except:
            x_bin = dta[[nm]]
    return x_bin


"""
get_binned_variables: get binned variables for all predictors with multiprocessing.
input:
    dt: a panda dataframe with predictors for binning;
    predictors: a list of string, names of all variables that wish to get binned;
    agg_ids_for_binning: a list, column names based on which data can be aggregated before binning, need to leave enough variation in data for meanning binning;
    n_bins: integer, number of bin/level/category for binning;
    n_process: integer, number of process for parallel;
    agg_ID: string, the name for the unique id column generated from add_aggregate_id function;
    be_bid: string, the name for the be_bid or entry_id in dataset.
output:
    bin_results_bid: panda dataframe, the binned variables at agg_ID + be_bid level;
    bin_results_agg: panda dataframe, the binned variables at agg_ID level.
"""


def get_binned_variables(dt, predictors, id_columns, n_bins=6, n_process=6):
    pool = Pool(initializer=init_worker_bin, initargs=(dt,), processes=n_process)
    bin_parallel = partial(getQuantileBins, n_bins=n_bins)
    bin_results = pool.map(bin_parallel, predictors)
    bin_results_bid = pd.concat(bin_results, axis=1) if len(bin_results) > 0 else pd.DataFrame()
    bin_results_bid.index = dt.index
    bin_results_bid = pd.concat([dt[id_columns], bin_results_bid], axis=1)
    return bin_results_bid


##########******** Function block 2:  calculate shap values ********##########
"""
calculate_shap:  calculate shap values for the input dataset.
input:
    dt: panda dataframe with all predictors used in model;
    estimator: a model object for the trained model;
    id_columns: list of string, names for all columns that are used as identifier;
    n_job: integer, number of jobs to parallel shap calculation.
output:
    dt_shap: panda dataframe, shap values for all predictors of the input data (i.e. dt) and also include id_columns.
"""


def calculate_shap(dt, estimator, id_columns=['analysis_year', 'region_type', 'region_name', 'maturity', 'be_bid'],
                   n_job=6):
    print('estimator feature name')
    print(dt[estimator.feature_name_])
    X = dt[estimator.feature_name_]
    dt_shap = fasttreeshap.TreeExplainer(estimator, algorithm="auto", n_jobs=n_job)
    dt_shap = pd.DataFrame(dt_shap(X).values, columns=X.columns)
    # dt_shap = shap.TreeExplainer(estimator).shap_values(X)
    # dt_shap = pd.DataFrame(dt_shap, columns= X.columns)

    dt_shap = dt_shap.round(4)
    dt_shap.index = dt.index
    dt_shap = pd.concat([dt[id_columns], dt_shap], axis=1)

    return dt_shap


##########******** Function block 3:  get  top positive and negative influencers ********##########


"""
top_shap_by_id_apply: used by get_top_influencers function to get top postive and negative influencers for each row.
input:
    row: 1 row of a panda dataframe for shap values;
    n_top: integer, number of top postive or negative variables to get.
output:
    tops: list for names of top postive influencers;
    btms: list for names of top negative influencers.
"""


def top_shap_by_id_apply(row, n_top):
    x_var = pd.DataFrame(row)
    x_var.columns = ['shap']
    x_var = x_var.sort_values(['shap'])
    btms = list(x_var.iloc[:n_top, ].index)
    x_var = x_var.sort_values(['shap'], ascending=False)
    tops = list(x_var.iloc[:n_top, ].index)
    return tops, btms


"""
init_worker_ui_data:  used inside get_top_influencers function to pass several variables as global variables for multiprocessing.
input:
    shap_dt_agg: panda dataframe, aggreated shap values for all predictors based on aggregate_level;
    shap_dt: un-aggregated shap values (i.e. shap value at be_bid + id_columns level) for all predictors;
    shap_dt_5q: panda dataframe,  5% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    shap_dt_25q: panda dataframe,  25% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    shap_dt_50q: panda dataframe,  50% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    shap_dt_75q: panda dataframe,  75% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    shap_dt_95q: panda dataframe,  95% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    value_dt: panda dataframe, raw or categorical value for the predictors, need to have same nubmer of rows as shap_dt and rows need to be ranked as same way as shap_dt; 
    positive_prefix_value: string, used to name positive influencers;
    negative_prefix_value:  string, used to name negative influencers;
    aggregate_column_name; 
    top_n: integer, number of top postive or negative variables to get;
    ids: list, items to parallel on, e.g. list of unique values for maturity_region_name. 
    
"""


def init_worker_ui_data(shap_dt_agg, shap_dt, shap_dt_5q, shap_dt_25q, shap_dt_50q, shap_dt_75q, shap_dt_95q,
                        value_dt, positive_prefix_value, negative_prefix_value, aggregate_column_name, top_n, ids):
    global dt_shap
    global dt_value
    global positive_prefix
    global negative_prefix
    global n_top
    global dt_shap_agg
    global id_columns
    global aggregate_name
    global dt_shap_5q
    global dt_shap_25q
    global dt_shap_50q
    global dt_shap_75q
    global dt_shap_95q

    dt_shap = shap_dt
    dt_shap_5q = shap_dt_5q
    dt_shap_25q = shap_dt_25q
    dt_shap_50q = shap_dt_50q
    dt_shap_75q = shap_dt_75q
    dt_shap_95q = shap_dt_95q
    dt_value = value_dt
    positive_prefix = positive_prefix_value
    negative_prefix = negative_prefix_value
    n_top = top_n
    dt_shap_agg = shap_dt_agg
    aggregate_name = aggregate_column_name
    id_columns = ids


"""
prepare_ui_data: used inside get_top_influencers function to get variable summary based on shap values within parallel processing.
input:
    uid: a string, a unique value from combinations of different identifier columns.
output:
    dt_summary: a panda dataframe,  a data summary based on shap values for a subset of entire data, a wide table includes id_columns, aggregate_name, shap values for top positive and negative influenvers, and values for top influencers.

"""


def prepare_ui_data(uid):
    dt_shap_sub = dt_shap[dt_shap[aggregate_name] == uid]
    dt_value_sub = dt_value[dt_value[aggregate_name] == uid]
    dt_shap_5q_sub = dt_shap_5q[dt_shap_5q[aggregate_name] == uid]
    dt_shap_25q_sub = dt_shap_25q[dt_shap_25q[aggregate_name] == uid]
    dt_shap_50q_sub = dt_shap_50q[dt_shap_50q[aggregate_name] == uid]
    dt_shap_75q_sub = dt_shap_75q[dt_shap_75q[aggregate_name] == uid]
    dt_shap_95q_sub = dt_shap_95q[dt_shap_95q[aggregate_name] == uid]

    global_tpi = [*dt_shap_agg['TPIs'].loc[dt_shap_agg[aggregate_name] == uid].values[0]]
    global_tni = [*dt_shap_agg['TNIs'].loc[dt_shap_agg[aggregate_name] == uid].values[0]]

    TPIs_shap_df, TNIs_shap_df = dt_shap_sub[global_tpi], dt_shap_sub[global_tni]
    TPIs_shap_df_5q, TNIs_shap_df_5q = dt_shap_5q_sub[global_tpi], dt_shap_5q_sub[global_tni]
    TPIs_shap_df_25q, TNIs_shap_df_25q = dt_shap_25q_sub[global_tpi], dt_shap_25q_sub[global_tni]
    TPIs_shap_df_50q, TNIs_shap_df_50q = dt_shap_50q_sub[global_tpi], dt_shap_50q_sub[global_tni]
    TPIs_shap_df_75q, TNIs_shap_df_75q = dt_shap_75q_sub[global_tpi], dt_shap_75q_sub[global_tni]
    TPIs_shap_df_95q, TNIs_shap_df_95q = dt_shap_95q_sub[global_tpi], dt_shap_95q_sub[global_tni]
    TPIs_shap_df.columns = ['shap_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df.columns = ['shap_' + negative_prefix + str(i) for i in range(n_top)]
    TPIs_shap_df_5q.columns = ['shap_5q_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df_5q.columns = ['shap_5q_' + negative_prefix + str(i) for i in range(n_top)]
    TPIs_shap_df_25q.columns = ['shap_25q_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df_25q.columns = ['shap_25q_' + negative_prefix + str(i) for i in range(n_top)]
    TPIs_shap_df_50q.columns = ['shap_50q_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df_50q.columns = ['shap_50q_' + negative_prefix + str(i) for i in range(n_top)]
    TPIs_shap_df_75q.columns = ['shap_75q_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df_75q.columns = ['shap_75q_' + negative_prefix + str(i) for i in range(n_top)]
    TPIs_shap_df_95q.columns = ['shap_95q_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_shap_df_95q.columns = ['shap_95q_' + negative_prefix + str(i) for i in range(n_top)]

    TPIs_value_df, TNIs_value_df = dt_value_sub[global_tpi], dt_value_sub[global_tni]
    TPIs_value_df.columns = ['value_' + positive_prefix + str(i) for i in range(n_top)]
    TNIs_value_df.columns = ['value_' + negative_prefix + str(i) for i in range(n_top)]

    dt_summary = pd.concat([dt_value_sub[id_columns], dt_value_sub[[aggregate_name]], TPIs_shap_df, TNIs_shap_df,
                            TPIs_shap_df_5q, TNIs_shap_df_5q, TPIs_shap_df_25q, TNIs_shap_df_25q,
                            TPIs_shap_df_50q, TNIs_shap_df_50q, TPIs_shap_df_75q, TNIs_shap_df_75q,
                            TPIs_shap_df_95q, TNIs_shap_df_95q, TPIs_value_df, TNIs_value_df], axis=1)
    for j in range(n_top):
        dt_summary['name_' + positive_prefix + str(j)] = global_tpi[j]
        dt_summary['name_' + negative_prefix + str(j)] = global_tni[j]
    return dt_summary


"""
get_top_influencers: directly called to derive top influencers at agg_ID level, and common and uniqeu influencers at agg_ID_be_bid level.
input:
    dt_shap: panda dataframe,  shap values for all predictors at agg_ID_be_bid level;
    dt_shap_5q: panda dataframe,  5% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    dt_shap_25q: panda dataframe,  25% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    dt_shap_50q: panda dataframe,  50% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    dt_shap_75q: panda dataframe,  75% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    dt_shap_95q: panda dataframe,  95% quantile of shap value for each predictor at regional_level; you can manually change id_columns2 in shap_ui_flow to change definition of regional;
    dt_value: panda dataframe, raw or categorical value for the predictors, need to have same nubmer of rows as dt_shap and rows need to be ranked as same way as dt_shap; 
    predictors: list of string, predictors used by the model;
    aggregate_name: string, name for the column indicating aggregate level;
    id_columns: list of string, names for all columns that are used as identifier;
    aggregation_levels: list of string list, each list includes column names that the aggregation will be based on (e.g. ['maturity','be_bid']), use ['Global'] to get global level aggregation;
    n_top: integer, number of top positive/negative influencers to get;
    positive_prefix: string, used to name positive influencers;
    negative_prefix:  string, used to name negative influencers.

output:
    dt_summaryA: panda dataframe, a data summary based on shap values for the entire data, a wide table includes id_columns, aggregate_name, shap values for top positive and negative influenvers, and values for top influencers.

"""


def get_top_influencers(dt_shap, dt_shap_5q, dt_shap_25q, dt_shap_50q, dt_shap_75q, dt_shap_95q, dt_value, predictors,
                        aggregate_name='aggregate_level',
                        id_columns=['maturity', 'region_type', 'region_name', 'be_bid'],
                        aggregation_levels=[['maturity']], n_top=10, positive_prefix='TPI_', negative_prefix='TNI_'):
    dt_summaryA = pd.DataFrame()

    for aggregation_level in aggregation_levels:

        if aggregation_level == ['Global']:
            dt_shap_agg = pd.DataFrame(dt_shap[predictors].mean(axis=0)).T

            dt_shap_agg2 = dt_shap_agg[predictors].T
            dt_shap_agg2.columns = ['shap']
            dt_shap_agg2 = dt_shap_agg2.sort_values(['shap'])
            global_tni = list(dt_shap_agg2.iloc[:n_top].index)
            dt_shap_agg2 = dt_shap_agg2.sort_values(['shap'], ascending=False)
            global_tpi = list(dt_shap_agg2.iloc[:n_top].index)

            TPIs_shap_df, TNIs_shap_df = dt_shap[global_tpi], dt_shap[global_tni]
            TPIs_shap_df_5q, TNIs_shap_df_5q = dt_shap_5q[global_tpi], dt_shap_5q[global_tni]
            TPIs_shap_df_25q, TNIs_shap_df_25q = dt_shap_25q[global_tpi], dt_shap_25q[global_tni]
            TPIs_shap_df_50q, TNIs_shap_df_50q = dt_shap_50q[global_tpi], dt_shap_50q[global_tni]
            TPIs_shap_df_75q, TNIs_shap_df_75q = dt_shap_75q[global_tpi], dt_shap_75q[global_tni]
            TPIs_shap_df_95q, TNIs_shap_df_95q = dt_shap_95q[global_tpi], dt_shap_95q[global_tni]

            TPIs_shap_df.columns = ['shap_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df.columns = ['shap_' + negative_prefix + str(i) for i in range(n_top)]
            TPIs_shap_df_5q.columns = ['shap_5q_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df_5q.columns = ['shap_5q_' + negative_prefix + str(i) for i in range(n_top)]
            TPIs_shap_df_25q.columns = ['shap_25q_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df_25q.columns = ['shap_25q_' + negative_prefix + str(i) for i in range(n_top)]
            TPIs_shap_df_50q.columns = ['shap_50q_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df_50q.columns = ['shap_50q_' + negative_prefix + str(i) for i in range(n_top)]
            TPIs_shap_df_75q.columns = ['shap_75q_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df_75q.columns = ['shap_75q_' + negative_prefix + str(i) for i in range(n_top)]
            TPIs_shap_df_95q.columns = ['shap_95q_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_shap_df_95q.columns = ['shap_95q_' + negative_prefix + str(i) for i in range(n_top)]

            TPIs_value_df, TNIs_value_df = dt_value[global_tpi], dt_value[global_tni]
            TPIs_value_df.columns = ['value_' + positive_prefix + str(i) for i in range(n_top)]
            TNIs_value_df.columns = ['value_' + negative_prefix + str(i) for i in range(n_top)]

            dt_value[aggregate_name] = 'Global'
            dt_summary = pd.concat([dt_value[id_columns], dt_value[[aggregate_name]], TPIs_shap_df, TNIs_shap_df,
                                    TPIs_shap_df_5q, TNIs_shap_df_5q, TPIs_shap_df_25q, TNIs_shap_df_25q,
                                    TPIs_shap_df_50q, TNIs_shap_df_50q, TPIs_shap_df_75q, TNIs_shap_df_75q,
                                    TPIs_shap_df_95q, TNIs_shap_df_95q, TPIs_value_df, TNIs_value_df], axis=1)

            for j in range(n_top):
                dt_summary['name_' + positive_prefix + str(j)] = global_tpi[j]
                dt_summary['name_' + negative_prefix + str(j)] = global_tni[j]

        else:
            dt_value[aggregate_name] = dt_value[aggregation_level].apply(lambda row: '_'.join(row.values.astype(str)),
                                                                         axis=1)
            dt_shap[aggregate_name] = dt_shap_5q[aggregate_name] = dt_shap_25q[aggregate_name] = dt_shap_50q[
                aggregate_name] = dt_shap_75q[aggregate_name] = dt_shap_95q[aggregate_name] = dt_value[aggregate_name]

            dt_shap_agg = dt_shap.groupby([aggregate_name]).mean().reset_index()
            dt_shap_agg['TPIs'], dt_shap_agg['TNIs'] = zip(
                *dt_shap_agg[predictors].apply(lambda row: top_shap_by_id_apply(row, n_top), axis=1))

            uids = list(dt_shap[aggregate_name].unique())

            n_count = min(12, len(uids))
            pool = Pool(initializer=init_worker_ui_data, initargs=(
                dt_shap_agg, dt_shap, dt_shap_5q, dt_shap_25q, dt_shap_50q, dt_shap_75q, dt_shap_95q, dt_value,
                positive_prefix, negative_prefix, aggregate_name, n_top, id_columns,), processes=n_count)
            results = pool.map(prepare_ui_data, uids)
            dt_summary = pd.concat(results, axis=0) if len(results) > 0 else pd.DataFrame()
            dt_summary[aggregate_name] = '_'.join(aggregation_level)

        dt_summaryA = pd.concat([dt_summaryA, dt_summary], axis=0)
    # dt_summaryA.index = range(dt_summaryA.shape[0])
    return dt_summaryA


##########******** Function block 4:  reshap wide table to tall table for UI ********##########


"""
get_rank_direction: called by the ui_reforamt_tall_table to get the rank of influencers and to decide whether the influencers has postive or negative impact.
input: 
    string: string.
output:
    int(rank): integer, the rank of the influencer;
    direction: string, Positive or Negative.
"""


def get_rank_direction(string):
    splits = string.split('_')
    if len(splits) == 4:
        rank = string.split('_')[3]
        direction = string.split('_')[2]
    elif len(splits) == 3:
        rank = string.split('_')[2]
        direction = string.split('_')[1]
    if direction == 'TPI':
        direction = 'positive'
    elif direction == 'TNI':
        direction = 'negative'
    return int(rank), direction


"""
ui_reformat_tall_table: used directly to reshape the wide table to tall table required by UI.
input:
    shap_summary: panda dataframe, output from get_top_influencers function,
    aggregate_name: string, name for the column indicating aggregate level;
    id_vars: list of string, similar with id_columns for get_top_influencers function, but need to add the column for aggregate level;
output:
    ui_reshape: panda dataframe, a tall table requried by UI. 
"""


def ui_reformat_tall_table(shap_summary, aggregate_name='aggregate_level',
                           id_vars=['maturity', 'region_type', 'region_name', 'be_bid', 'aggregate_level']):
    ui_reshape = pd.DataFrame()
    levels = list(shap_summary[aggregate_name].unique())
    for level in levels:
        ui_sub = shap_summary[shap_summary[aggregate_name] == level]

        ui_sub_melt = ui_sub.melt(id_vars)

        ui_sub_melt['feature_rank'] = np.int
        ui_sub_melt['feature_direction'] = str
        ui_sub_melt['feature_rank'], ui_sub_melt['feature_direction'] = zip(
            *ui_sub_melt['variable'].map(get_rank_direction))
        ui_sub_melt['feature_rank'] = ui_sub_melt['feature_rank'] + 1

        ui_sub_melt['data_category'] = str
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('value_')] = 'feature_value_category'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_')] = 'feature_shap_value'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_5q')] = 'feature_shap_value_5'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_25q')] = 'feature_shap_value_25'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_50q')] = 'feature_shap_value_50'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_75q')] = 'feature_shap_value_75'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('shap_95q')] = 'feature_shap_value_95'
        ui_sub_melt['data_category'].loc[ui_sub_melt['variable'].str.startswith('name_')] = 'feature_name'

        ui_sub_melt_pivot = ui_sub_melt.pivot(columns=['data_category'],
                                              index=id_vars + ['feature_rank', 'feature_direction'],
                                              values=['value']).droplevel(0, axis=1).reset_index()

        ui_reshape = pd.concat([ui_reshape, ui_sub_melt_pivot], axis=0)
    ui_reshape.index = range(ui_reshape.shape[0])
    return ui_reshape


##########******** Function block 5:  post processing for UI ********##########
"""
ui_post_processing:  function to reformat data to match the UI data schema.
input: 
    analysis_year: integer;
    ap_data_sector: string;
    dummy_variables: list of string, used to map 0-1 into 1-2;
    column_order: list of string, used to reorder the output columns to match with UI dataset.
output:
    ui_format: panda dataframe, this data match the ui data schema and is the final output that needs to be ingested.
    
"""


def ui_post_processing(ui_format, analysis_year=2021, ap_data_sector='NA_CORN_SUMMER',
                       dummy_variables=['irrflag', 'tileflag', 'previous_crop_corn'],
                       column_order=['region_type', 'region_name', 'be_bid', 'analysis_year', 'ap_data_sector',
                                     'maturity',
                                     'feature_name', 'feature_rank', 'feature_direction', 'feature_value_category',
                                     'feature_shap_value', 'aggregate_level']):
    if ap_data_sector == 'SOY_NA_SUMMER':
        dummy_variables1 = ['irrflag', 'tileflag', 'previous_crop_corn']
    else:
        dummy_variables1 = dummy_variables
    ui_format['analysis_year'] = analysis_year
    ui_format['ap_data_sector'] = ap_data_sector
    ui_format['feature_value_category'] = pd.to_numeric(ui_format['feature_value_category'], errors='coerce')
    ui_format['feature_value_category'] = ui_format['feature_value_category'].astype(int)
    ui_format['feature_value_category'].loc[ui_format['feature_name'].isin(dummy_variables1)] = \
        ui_format['feature_value_category'].loc[ui_format['feature_name'].isin(dummy_variables1)] + 1
    ui_format = ui_format[column_order]
    return ui_format


"""
merge_parent_geno_all_rms: similar with function merge_parent_geno from ui_grid_utils.py. the difference is that this function merge all data without subsetting data based on decision_group_rm.
"""


def merge_parent_geno_all_rms(bebid_df, hetpool1_df, hetpool2_df, drop_stage=True):
    if drop_stage:
        bebid_df = bebid_df.drop(columns=['stage', 'cpifl']).drop_duplicates()
    else:
        bebid_df = bebid_df.drop(columns=['cpifl']).drop_duplicates()

    out_df = bebid_df.dropna(subset=["par_hp1_sample", "par_hp2_sample"], how='all') \
        .merge(hetpool1_df.add_suffix("_par1"),
               left_on="par_hp1_sample",
               right_on="sample_id_par1",
               how="left",
               suffixes=("", "_par1")) \
        .merge(hetpool2_df.add_suffix("_par2"),
               left_on="par_hp2_sample",
               right_on="sample_id_par2",
               how="left",
               suffixes=("", "_par2")) \
        .drop(columns=["par_hp1_sample", "sample_id_par1", "par_hp2_sample", "sample_id_par2"])
    out_df = out_df.drop_duplicates()

    return out_df


def merge_parent_geno_all_rms_soy(bebid_df, hetpool1_df, drop_stage=True):
    if drop_stage:
        bebid_df = bebid_df.drop(columns=['stage', 'cpifl']).drop_duplicates()
    else:
        bebid_df = bebid_df.drop(columns=['cpifl']).drop_duplicates()

    out_df = bebid_df.dropna(subset=["par_hp1_sample"], how='all') \
        .merge(hetpool1_df.add_suffix("_par1"),
               left_on="par_hp1_sample",
               right_on="sample_id_par1",
               how="left",
               suffixes=("", "_par1")).drop(columns=["par_hp1_sample", "sample_id_par1"])
    out_df = out_df.drop_duplicates()

    return out_df
