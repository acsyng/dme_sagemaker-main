from math import radians, sin, cos, sqrt, atan2

import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import mahalanobis
from scipy.stats import chi2
from tqdm import tqdm

from libs.dme_statistical_tests import norm_ratio_t_test, var_ratio_f_test


##########################################################################################
# merge_parent_geno
#
# joins be_bids specific to an analysis year with the pca-by-hetpool results for that year.
#
# Input:
#    bebid_df: df of ~ [analysis_year, ap_data_sector, decision_group_rm, stage, be_bid, par_hp1_be_bid, par_hp1_sample, par_hp2_be_bid, par_hp2_sample, cpifl, cperf]
#    hetpool1/2_df's: df's of geno-PCA data for each heterotic pool
#    target_market: numeric decision_group_rm of interest (80-120)
#    drop_stage: boolean on if stage should be dropped from this dataset.
#
# Output: out_df: df of ~ [analysis_year, ap_data_sector, decision_group_rm, (stage), be_bid, par_hp1_be_bid, par_hp2_be_bid, <geno-PCA features>]
#
# Author: Keri Rehm, 2023-Aug-04
##########################################################################################


def merge_parent_geno(bebid_df, hetpool1_df, hetpool2_df, target_market, drop_stage=True):
    if drop_stage:
        bebid_df = bebid_df.loc[bebid_df["decision_group_rm"] == target_market,].drop(
            columns=['stage', 'cpifl']).drop_duplicates()
    else:
        bebid_df = bebid_df.loc[bebid_df["decision_group_rm"] == target_market,].drop(
            columns=['cpifl']).drop_duplicates()

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

    return out_df


def merge_parent_geno_soy(bebid_df, hetpool1_df, target_market, drop_stage=True):
    if drop_stage:
        bebid_df = bebid_df.loc[bebid_df["decision_group_rm"] == target_market,].drop(
            columns=['stage', 'cpifl']).drop_duplicates()
    else:
        bebid_df = bebid_df.loc[bebid_df["decision_group_rm"] == target_market,].drop(
            columns=['cpifl']).drop_duplicates()

    out_df = bebid_df.dropna(subset=["par_hp1_sample"], how='all') \
        .merge(hetpool1_df.add_suffix("_par1"),
               left_on="par_hp1_sample",
               right_on="sample_id_par1",
               how="left",
               suffixes=("", "_par1")) \
        .drop(columns=["par_hp1_sample", "sample_id_par1"])

    return out_df


###########################################################################################
# merge_cfgrid_entry
#
# For a given year and irrigation scenario, merge together relevant cf grid with be_bid genetic info
# and then predict yield using the model named 'estimator'
#
# Author: Keri Rehm, 2023-Aug-04
###########################################################################################
def merge_cfgrid_entry(cropFact_grid_df, dg_be_bids_df, X_template, estimator, year, irr_scenario,
                       lower_estimator=None, middle_estimator=None, upper_estimator=None):
    cF_year_df = cropFact_grid_df.loc[(cropFact_grid_df.year == year) &
                                      (cropFact_grid_df.irrigation == irr_scenario), :]
    print('cF_year_df shape: ', cF_year_df.shape)

    df_out = cF_year_df.merge(dg_be_bids_df, left_on="Market_Days", right_on="decision_group_rm").drop(
        columns=["Market_Days", "decision_group_rm"])

    print('df_out shape:  ', df_out.shape)
    X = df_out.reindex(columns=X_template.columns)
    print('X shape: ', X.shape)
    print('estimator: ', estimator)
    df_out["predict_ygsmn"] = estimator.predict(X)
    if lower_estimator is not None:
        df_out['YGSMNp_10'] = lower_estimator.predict(X)
    if middle_estimator is not None:
        df_out['YGSMNp_50'] = middle_estimator.predict(X)
    if upper_estimator is not None:
        df_out['YGSMNp_90'] = upper_estimator.predict(X)

    return df_out


###########################################################################################
# run_aggregate_metrics
#
#
###########################################################################################
def run_aggregate_metrics(df, groupvar="place_id"):
    # Setup aggregation level list & aggregate to desired level
    if groupvar == "place_id":
        agg_levels = [groupvar, "maturity", "stage", "density", "irrigation", "previous_crop_corn"]

        agg_base_df = df[agg_levels + ["entry_id", "cperf", "predict_ygsmn"]] \
            .groupby(agg_levels + ["entry_id", "cperf"], as_index=False) \
            .agg(count_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="count"),
                 predict_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="mean"),
                 std_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="std"))
    else:
        # print(df.columns)
        df = df.rename(columns={groupvar: "region_name"})
        df["region_type"] = groupvar
        agg_levels = ["maturity", "stage", "region_name", "region_type", "density", "irrigation", "previous_crop_corn"]
        # print(df.columns)
        print(df[agg_levels + ["entry_id", "cperf", "predict_ygsmn"]].head())
        print('dtypes of df agg levels')
        print(df[agg_levels + ["entry_id", "cperf", "predict_ygsmn"]].dtypes)

        list_str_obj_cols = df.columns[df.dtypes == "object"].tolist()
        for str_obj_col in list_str_obj_cols:
            df[str_obj_col] = df[str_obj_col].astype("category")

        print('after convert dtypes of df agg levels')
        print(df[agg_levels + ["entry_id", "cperf", "predict_ygsmn"]].dtypes)

        agg_base_df = df[agg_levels + ["entry_id", "cperf", "predict_ygsmn", "maturity_acres"]] \
            .groupby(agg_levels + ["entry_id", "cperf"], as_index=False, observed=True) \
            .apply(lambda x: pd.Series({'count_ygsmn': x['predict_ygsmn'].shape[0],
                                        'predict_ygsmn': np.average(x['predict_ygsmn'], weights=x['maturity_acres']),
                                        'std_ygsmn': np.sqrt(
                                            np.cov(x['predict_ygsmn'], aweights=x['maturity_acres']))}))

        print('agg_base_df shape: ', agg_base_df.shape)
    # Create the h2h format
    agg_df = agg_base_df.merge(agg_base_df.loc[agg_base_df.cperf == 1, :], how='left', on=agg_levels,
                               suffixes=('', '_check'))

    # Calculate h2h yield difference
    agg_df["yield_diff_vs_chk"] = agg_df["predict_ygsmn"] - agg_df["predict_ygsmn_check"]

    # Calculate h2h metrics
    agg_df["yield_pct_chk"], _, agg_df["yield_metric"], _ = norm_ratio_t_test(
        ent_pred=agg_df["predict_ygsmn"].to_numpy(),
        chk_pred=agg_df["predict_ygsmn_check"].to_numpy(),
        ent_stddev=agg_df["std_ygsmn"].to_numpy(),
        chk_stddev=agg_df["std_ygsmn_check"].to_numpy(),
        ent_count=agg_df["count_ygsmn"].to_numpy(),
        chk_count=agg_df["count_ygsmn_check"].to_numpy(),
        threshold_factor=1,
        spread_factor=1,
        direction='left')
    _, agg_df["stability_metric"] = var_ratio_f_test(ent_pred=agg_df["predict_ygsmn"].to_numpy(),
                                                     chk_pred=agg_df["predict_ygsmn_check"].to_numpy(),
                                                     ent_stddev=agg_df["std_ygsmn"].to_numpy(),
                                                     chk_stddev=agg_df["std_ygsmn_check"].to_numpy(),
                                                     ent_count=agg_df["count_ygsmn"].to_numpy(),
                                                     chk_count=agg_df["count_ygsmn_check"].to_numpy(),
                                                     spread_factor=1)

    agg_df["yield_metric"] = agg_df["yield_metric"].astype(float)

    # Aggregate h2h metrics across checks
    agg_df = agg_df[
        agg_levels + ["entry_id", "cperf", "predict_ygsmn", "yield_pct_chk", "yield_diff_vs_chk", "yield_metric",
                      "stability_metric"]] \
        .groupby(agg_levels + ["entry_id", "cperf"]) \
        .agg(predict_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="mean"),
             yield_pct_chk=pd.NamedAgg(column="yield_pct_chk", aggfunc="mean"),
             yield_diff_vs_chk=pd.NamedAgg(column="yield_diff_vs_chk", aggfunc="mean"),
             yield_metric=pd.NamedAgg(column="yield_metric", aggfunc=(lambda x: stats.gmean(x))),
             stability_metric=pd.NamedAgg(column="stability_metric", aggfunc=(lambda x: stats.gmean(x)))).reset_index() \
        .reset_index(drop=False).rename(columns={"index": "id"})
    agg_df["stage"] = agg_df["stage"].astype('str')

    return agg_df


###########################################################################################
# mal_distance_calc
#
# Calculate the mahalanobis distance between a point (vector) and distribution of points (matrix). Compare the
# squared Mahalanobis distance to a chi-squared distribution to determine if point is an outlier.
# Threshold used is 0.01
#
# Outputs: distance, p_value, outlier_ind (True/False)
#
# Author: Jesse Darlington, 2024-May
###########################################################################################

def mal_distance_calc(matrix_df, vector_df, dist_cols_list):
    # Define your matrix and vector
    matrix = matrix_df[dist_cols_list]
    # fill in missing values with column mean
    matrix = matrix.fillna(matrix.mean())
    # Calculate the mean of the matrix
    mean_vector = np.mean(matrix, axis=0)
    # Calculate the covariance matrix of the dataset (bias corrected with ddof=1)
    cov_matrix = np.cov(matrix, rowvar=False, ddof=1)
    # Calculate the inverse of the covariance matrix
    inv_cov_matrix = np.linalg.inv(cov_matrix)

    output_df = pd.DataFrame(columns=['distance', 'p_value', 'outlier_ind'])

    for i in tqdm(range(vector_df.shape[0])):
        vector = vector_df.loc[vector_df.index[i], dist_cols_list]
        distance = mahalanobis(vector, mean_vector, inv_cov_matrix)
        # print("Mahalanobis Distance:", distance)

        # If you want to determine if the vector is an outlier, you can compare the squared
        # Mahalanobis distance to a chi-squared distribution:
        p_value = 1 - chi2.cdf(distance ** 2, df=len(vector))
        # print("p-value:", p_value)
        outlier_ind = p_value < 0.01

        output_vals = [distance, p_value, outlier_ind]
        # print(output_vals)
        output_df.loc[len(output_df)] = output_vals

    print(output_df.shape)

    print(output_df.outlier_ind.value_counts())
    return output_df


###########################################################################################
# haversine_distance
#
# Calculate the haversine distance between two points given lat1, lon1, lat2, lon2
#
# Output: haversine distance between two points
#
# Author: Jesse Darlington, 2024-May
###########################################################################################

def haversine_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # Differences in latitude and longitude
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Calculate the distance
    distance = R * c
    return distance

###########################################################################################
# find_nearest_points
#
# Find the nearest n points to a desired point. Inputs include desired point lat, desired point lon, collection of points, and n number of points.
#
# Output: n points nearest to desired point.
#
# Author: Jesse Darlington, 2024-May
###########################################################################################


def find_nearest_points(current_lat, current_lon, points, n):
    distances = {}
    for point, (lat, lon) in points.items():
        distance = haversine_distance(current_lat, current_lon, lat, lon)
        distances[point] = distance

    nearest_points = sorted(distances, key=distances.get)[:n]
    return nearest_points

###########################################################################################
# mal_distance_calc_knn
#
# Calculate the mahalanobis distance between a point (vector) and distribution of points (matrix).
# Distribution of points is generated using find_nearest_points function
# Compare the squared Mahalanobis distance to a chi-squared distribution to determine if point is an outlier.
# Threshold used is 0.01
#
# Outputs: distance, p_value, outlier_ind (True/False)
#
# Author: Jesse Darlington, 2024-May
###########################################################################################


def mal_distance_calc_knn(matrix_df, vector_df, dist_cols_list, n):
    output_df = pd.DataFrame(columns=['distance', 'p_value', 'outlier_ind'])

    df_dict = matrix_df[['cropfact_id', 'lat', 'lon']].to_dict(orient='records')
    # Create a new dictionary with one key and two columns as values
    points_dict = {record['cropfact_id']: (record['lat'], record['lon']) for record in df_dict}
    df = vector_df[['lat', 'lon']]  # .drop_duplicates()
    print('vector df shape:', df.shape)
    for i in tqdm(range(vector_df.shape[0])):
        # Define your matrix and vector
        # print(df.iloc[i])
        nearest_points = find_nearest_points(df.iloc[i, 0], df.iloc[i, 1], points_dict, n)
        # print('# points: ', len(nearest_points), 'unique points: ', len(list(set(nearest_points))), 'points: ',nearest_points)

        matrix_df1 = matrix_df[matrix_df.cropfact_id.isin(nearest_points)]
        # print('i: ', i, 'shape: ',matrix_df1.shape)
        matrix = matrix_df1[dist_cols_list]
        # print('matrix shape: ', matrix.shape, '# of missing vals: ', matrix.isna().sum().sum())
        # fill in missing values with column mean
        matrix = matrix.fillna(matrix.mean())
        # print(matrix.head())
        # Calculate the mean of the matrix
        mean_vector = np.mean(matrix, axis=0)
        # print(mean_vector)
        # Calculate the covariance matrix of the dataset (bias corrected with ddof=1)
        cov_matrix = np.cov(matrix, rowvar=False, ddof=1)
        # print(cov_matrix)
        # Calculate the inverse of the covariance matrix
        inv_cov_matrix = np.linalg.pinv(cov_matrix)
        vector = vector_df.loc[vector_df.index[i], dist_cols_list]
        distance = mahalanobis(vector, mean_vector, inv_cov_matrix)
        # print("Mahalanobis Distance:", distance)

        # If you want to determine if the vector is an outlier, you can compare the squared
        # Mahalanobis distance to a chi-squared distribution:
        p_value = 1 - chi2.cdf(distance ** 2, df=len(vector))
        # print("p-value:", p_value)
        outlier_ind = p_value < 0.01

        output_vals = [distance, p_value, outlier_ind]
        # print(output_vals)
        output_df.loc[len(output_df)] = output_vals
        # print()
        # print()

    print(output_df.shape)

    print(output_df.outlier_ind.value_counts())
    return output_df
