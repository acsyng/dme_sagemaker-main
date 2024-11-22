"""
This function computes model performance statistics (Pearson correlation, Spearman rank correlation, root mean squared error) and
the number of observations for a dataset (dataframe) partitioned by analysis_year and aggregates them based on all combinations of
maturity_group, trial_stage, par_geno as well as analysis_year overall.
Inputs columns include analysis_year, maturity_group, trial_stage, YGSMN, YGSMNp, and par_geno.
par_geno column is labeled using par1_exist and par2_exist.

Output dataset (dataframe) is partitioned by analysis_year.
Output columns include:
    analysis_year
    maturity_group (0-1-2-3-4-5-6-7-8-9-10-all)
    trial_stage (2-3-4-5-6-8-all)
    par_geno = number of parents that are genotyped (None-Par1-Par2-Both-all)
    yield_corr = Pearson correlation between YGSMN and YGSMNp
    rank_corr = Spearman rank correlation between YGSMN and YGSMNp
    yield_rmse = root mean squared error between YGSMN and YGSMNp
    n_obs = number of observations in that group
"""

import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import mean_squared_error


def aggregate_metrics(df, ap_data_sector):
    if ap_data_sector == "CORN_BRAZIL_SUMMER" or ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
        df["maturity_group"] = df["tpp_region"]
        df = df.drop('tpp_region', axis=1)
        val1 = 'YGSMN'
        val2 = 'YGSMNp'
    elif ap_data_sector == "CORNGRAIN_EAME_SUMMER":
        df["maturity_group"] = df["et"]
        df = df.drop('et', axis=1)
        val1 = 'YGSMN'
        val2 = 'YGSMNp'
    elif ap_data_sector == "CORNSILAGE_EAME_SUMMER":
        df["maturity_group"] = df["et"]
        df = df.drop('et', axis=1)
        val1 = 'YSDMN'
        val2 = 'YSDMNp'
    else:
        val1 = 'YGSMN'
        val2 = 'YGSMNp'

    print(df['maturity_group'].value_counts())

    # Create the new column "par_geno"
    df['par_geno'] = np.select([(df['comp_0_par1'].isna()) & (df['comp_0_par2'].isna()),
                                (df['comp_0_par1'].notna()) & (df['comp_0_par2'].notna()),
                                (df['comp_0_par1'].notna()) & (df['comp_0_par2'].isna()),
                                (df['comp_0_par1'].isna()) & (df['comp_0_par2'].notna())],
                               ["none", "both", "Par1", "Par2"])

    # Create average per trial-bebid, maintaining important columns
    df = df.groupby(["analysis_year", "maturity_group", "trial_stage", "trial_id", "be_bid", "par_geno"])[
        [val1, val2]].mean().reset_index()

    # Define a function to calculate the metrics
    def calculate_metrics(df_cm, val1_cm, val2_cm):
        if df_cm.shape[0] > 2:
            yield_corr = pearsonr(df_cm[val1_cm], df_cm[val2_cm])[0]
            rank_corr = spearmanr(df_cm[val1_cm], df_cm[val2_cm])[0]
        else:
            yield_corr = None
            rank_corr = None

        yield_rmse = mean_squared_error(df_cm[val1_cm], df_cm[val2_cm], squared=False)
        n_obs = df_cm.shape[0]

        return pd.Series({"yield_corr": yield_corr,
                          "rank_corr": rank_corr,
                          "yield_rmse": yield_rmse,
                          "n_obs": n_obs})

    def aggregate_metrics_calc(df_am):
        yield_corr = df_am["yield_corr"].mean()
        rank_corr = df_am["rank_corr"].mean()
        yield_rmse = df_am["yield_rmse"].mean()
        n_obs = df_am["n_obs"].sum()
        return yield_corr, rank_corr, yield_rmse, n_obs

    # Create trial-level metrics
    df1 = df.copy()
    df1['par_geno'] = "all"
    df = pd.concat([df, df1])
    df = df.groupby(["analysis_year", "maturity_group", "trial_stage", "trial_id", "par_geno"]).apply(
        calculate_metrics, val1, val2).reset_index()

    # Create a list to store the aggregated results
    aggregated_results = []

    # Calculate metrics for each combination of maturity_group, trial_stage, and par_geno
    for group in df['maturity_group'].unique().tolist() + ['all']:
        for stage in df['trial_stage'].unique().tolist() + ['all']:
            for gene in df['par_geno'].unique().tolist():
                if group == 'all' and stage == 'all' and gene == 'all':
                    group_df = df.copy()
                else:
                    group_df = df
                    if group != 'all':
                        group_df = group_df[group_df['maturity_group'] == group]
                    if stage != 'all':
                        group_df = group_df[group_df['trial_stage'] == stage]
                    if gene != 'all':
                        group_df = group_df[group_df['par_geno'] == gene]

                group_df = group_df.dropna(axis=0, subset=['yield_corr'])

                if group_df.shape[0] > 0:
                    metrics = aggregate_metrics_calc(group_df)
                    aggregated_results.append((group, stage, gene, *metrics))

    # Create the aggregated dataframe
    columns = ['maturity_group', 'trial_stage', 'par_geno', 'yield_corr', 'rank_corr', 'yield_rmse', 'n_obs']
    aggregated_df = pd.DataFrame(aggregated_results, columns=columns)

    # Add Year as the first column in aggregated_df
    aggregated_df.insert(0, 'analysis_year', df['analysis_year'].unique()[0])

    if ap_data_sector == "CORN_BRAZIL_SUMMER" or ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
        aggregated_df['tpp_region'] = aggregated_df['maturity_group']
        aggregated_df = aggregated_df.drop('maturity_group', axis=1)
    if ap_data_sector == "CORNGRAIN_EAME_SUMMER" or ap_data_sector == "CORNSILAGE_EAME_SUMMER":
        aggregated_df['et'] = aggregated_df['maturity_group']
        aggregated_df = aggregated_df.drop('maturity_group', axis=1)

    return aggregated_df


def aggregate_metrics_soy(df, ap_data_sector):
    if ap_data_sector == "SOY_NA_SUMMER":
        df["tpp_region"] = df["maturity_zone"]
        df["tpp_region"] = df["tpp_region"].replace('000-00', '0')
        df["tpp_region"] = df["tpp_region"].astype(float).astype(int)
        df = df.drop('maturity_zone', axis=1)

    # Create the new column "par_geno"
    df['par_geno'] = np.select([(df['comp_0_par1'].isna()),
                                (df['comp_0_par1'].notna())],
                               ["none", "both"])

    # Create average per trial-bebid, maintaining important columns
    df = df.groupby(["analysis_year", "tpp_region", "trial_stage", "trial_id", "be_bid", "par_geno"])[
        ["YGSMN", "YGSMNp"]].mean().reset_index()

    # Define a function to calculate the metrics
    def calculate_metrics(df_cm):
        if df_cm.shape[0] > 2:
            yield_corr = pearsonr(df_cm['YGSMN'], df_cm['YGSMNp'])[0]
            rank_corr = spearmanr(df_cm['YGSMN'], df_cm['YGSMNp'])[0]
        else:
            yield_corr = None
            rank_corr = None

        yield_rmse = mean_squared_error(df_cm['YGSMN'], df_cm['YGSMNp'], squared=False)
        n_obs = df_cm.shape[0]

        return pd.Series({"yield_corr": yield_corr,
                          "rank_corr": rank_corr,
                          "yield_rmse": yield_rmse,
                          "n_obs": n_obs})

    def aggregate_metrics(df_am):
        yield_corr = df_am["yield_corr"].mean()
        rank_corr = df_am["rank_corr"].mean()
        yield_rmse = df_am["yield_rmse"].mean()
        n_obs = df_am["n_obs"].sum()
        return yield_corr, rank_corr, yield_rmse, n_obs

    # Create trial-level metrics
    df1 = df.copy()
    df1['par_geno'] = "all"
    df = pd.concat([df, df1])
    df = df.groupby(["analysis_year", "tpp_region", "trial_stage", "trial_id", "par_geno"]).apply(
        calculate_metrics).reset_index()

    print(df.head())

    # Create a list to store the aggregated results
    aggregated_results = []

    # Calculate metrics for each combination of maturity_group, trial_stage, and par_geno
    for group in df['tpp_region'].unique().tolist() + ['all']:
        for stage in df['trial_stage'].unique().tolist() + ['all']:
            for gene in df['par_geno'].unique().tolist():
                if group == 'all' and stage == 'all' and gene == 'all':
                    group_df = df.copy()
                else:
                    group_df = df
                    if group != 'all':
                        group_df = group_df[group_df['tpp_region'] == group]
                    if stage != 'all':
                        group_df = group_df[group_df['trial_stage'] == stage]
                    if gene != 'all':
                        group_df = group_df[group_df['par_geno'] == gene]

                group_df = group_df.dropna(axis=0,
                                           subset=['yield_corr'])

                if group_df.shape[0] > 0:
                    metrics = aggregate_metrics(group_df)
                    aggregated_results.append((group, stage, gene, *metrics))

    # Create the aggregated dataframe
    columns = ['tpp_region', 'trial_stage', 'par_geno', 'yield_corr', 'rank_corr', 'yield_rmse', 'n_obs']
    aggregated_df = pd.DataFrame(aggregated_results, columns=columns)

    # Add Year as the first column in aggregated_df
    aggregated_df.insert(0, 'analysis_year', df['analysis_year'].unique()[0])

    if ap_data_sector == "SOY_NA_SUMMER":
        aggregated_df['maturity_zone'] = aggregated_df['tpp_region']
        aggregated_df = aggregated_df.drop('tpp_region', axis=1)

    return aggregated_df
