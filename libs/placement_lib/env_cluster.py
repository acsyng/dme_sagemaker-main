"""
This function creates environment clusters by clustering weather and soil features of a given DataFrame.

Parameters:
df (pd.DataFrame): The DataFrame to cluster. Should contain the following weather and soil features:
                    - precsum30dbp-p, precsump-ve, precsumr2-r4, precsumr4-r6, precsumr6-h, precsumv6-vt, precsumve-v6, precsumvt-r2, radsum30dbp-p, radsump-ve, radsumr2-r4, radsumr4-r6, radsumr6-h, radsumv6-vt, radsumve-v6, radsumvt-r2,
                    - rhmax30dbp-p, rhmaxp-ve, rhmaxr2-r4, rhmaxr4-r6, rhmaxr6-h, rhmaxv6-vt, rhmaxve-v6, rhmaxvt-r2, rhmin30dbp-p, rhminp-ve, rhminr2-r4, rhminr4-r6, rhminr6-h, rhminv6-vt, rhminve-v6, rhminvt-r2,
                    - tempavgr2-r4, tempavgr4-r6, tempavgv6-vt, tempavgve-v6, tempavgvt-r2, tempmax30dbp-p, tempmaxp-ve, tempmaxr2-r4, tempmaxr4-r6, tempmaxr6-h, tempmaxv6-vt, tempmaxve-v6, tempmaxvt-r2, tempmin30dbp-p, tempminp-ve, tempminr2-r4, tempminr4-r6, tempminr6-h, tempminv6-vt, tempminve-v6, tempminvt-r2,
                    - sandcontent, siltcontent, claycontent, phwater, cationexchangecapacity, coarsefragmentcontent, calciumcarbonatecontent, organicmattercontent, bulkdensity, availablewateratfieldcapacity, hydraulicconductivityatsaturation.

Returns:
pd.DataFrame: The input DataFrame with the following four additional columns:
                  - weather_cluster
                  - soil_cluster
                  - WT_cluster_Label
                  - ST_cluster_Label
                  - Env_Cluster

"""

import pandas as pd
from sklearn.cluster import KMeans

"""
remove_genomic_variables:  remove genomic variable before conducting environmental clustering.
input:
    dt: panda dataframe.
output:
    dt: panda dataframe with genomic variable removed
"""


def remove_genomic_variables(dt):
    nms = list(dt.columns)
    nms2 = [nm for nm in nms if nm.startswith('comp_')]
    try:
        dt = dt.drop(nms2, axis=1)
    except:
        pass
    return dt


def cluster(df):
    """
    Creates environment clusters by clustering weather and soil features of a given DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to cluster. Should contain the following weather and soil features:
                        - precsum30dbp-p, precsump-ve, precsumr2-r4, precsumr4-r6, precsumr6-h, precsumv6-vt, precsumve-v6, precsumvt-r2, radsum30dbp-p, radsump-ve, radsumr2-r4, radsumr4-r6, radsumr6-h, radsumv6-vt, radsumve-v6, radsumvt-r2,
                        - rhmax30dbp-p, rhmaxp-ve, rhmaxr2-r4, rhmaxr4-r6, rhmaxr6-h, rhmaxv6-vt, rhmaxve-v6, rhmaxvt-r2, rhmin30dbp-p, rhminp-ve, rhminr2-r4, rhminr4-r6, rhminr6-h, rhminv6-vt, rhminve-v6, rhminvt-r2,
                        - tempavgr2-r4, tempavgr4-r6, tempavgv6-vt, tempavgve-v6, tempavgvt-r2, tempmax30dbp-p, tempmaxp-ve, tempmaxr2-r4, tempmaxr4-r6, tempmaxr6-h, tempmaxv6-vt, tempmaxve-v6, tempmaxvt-r2, tempmin30dbp-p, tempminp-ve, tempminr2-r4, tempminr4-r6, tempminr6-h, tempminv6-vt, tempminve-v6, tempminvt-r2,
                        - sandcontent, siltcontent, claycontent, phwater, cationexchangecapacity, coarsefragmentcontent, calciumcarbonatecontent, organicmattercontent, bulkdensity, availablewateratfieldcapacity, hydraulicconductivityatsaturation.

    Returns:
    pd.DataFrame: The input DataFrame with the following four additional columns:
                  - weather_cluster
                  - soil_cluster
                  - WT_cluster_Label
                  - ST_cluster_Label
                  - Env_Cluster
    """

    # Remove rows with missing values
    print('cluster init df: ', df.shape)
    df[['env_input_distance', 'env_input_p_value']] = 0
    df = df.dropna()
    print('df after drop na: ', df.shape)
    # df_sub = df[(df.be_bid =='ESY1800061652') & (df.maturity == '6C') & (df.region_type =='tpp')]
    # print(df_sub.region_name.value_counts())

    # Selecting weather features for clustering
    weather_features = [
        'precsum30dbp-p',
        'precsump-ve',
        'precsumr2-r4',
        'precsumr4-r6',
        'precsumr6-h',
        'precsumv6-vt',
        'precsumve-v6',
        'precsumvt-r2',
        'radsum30dbp-p',
        'radsump-ve',
        'radsumr2-r4',
        'radsumr4-r6',
        'radsumr6-h',
        'radsumv6-vt',
        'radsumve-v6',
        'radsumvt-r2',
        'rhmax30dbp-p',
        'rhmaxp-ve',
        'rhmaxr2-r4',
        'rhmaxr4-r6',
        'rhmaxr6-h',
        'rhmaxv6-vt',
        'rhmaxve-v6',
        'rhmaxvt-r2',
        'rhmin30dbp-p',
        'rhminp-ve',
        'rhminr2-r4',
        'rhminr4-r6',
        'rhminr6-h',
        'rhminv6-vt',
        'rhminve-v6',
        'rhminvt-r2',
        'tempavgr2-r4',
        'tempavgr4-r6',
        'tempavgv6-vt',
        'tempavgve-v6',
        'tempavgvt-r2',
        'tempmax30dbp-p',
        'tempmaxp-ve',
        'tempmaxr2-r4',
        'tempmaxr4-r6',
        'tempmaxr6-h',
        'tempmaxv6-vt',
        'tempmaxve-v6',
        'tempmaxvt-r2',
        'tempmin30dbp-p',
        'tempminp-ve',
        'tempminr2-r4',
        'tempminr4-r6',
        'tempminr6-h',
        'tempminv6-vt',
        'tempminve-v6',
        'tempminvt-r2']

    # weather_data = df[weather_features]
    weather_data = df.loc[:, df.columns.str.startswith(("precsum", "radsum", "rhmax", "rhmin", "tempavg", "tempmax", "tempmin"))]
    print('weather_data shape: ', weather_data.shape)
    # Creating weather clusters using k-means
    kmeans_weather = KMeans(n_clusters=3, random_state=42)
    weather_clusters = kmeans_weather.fit_predict(weather_data)

    # Selecting soil features for clustering
    soil_features = [
        'sandcontent',
        'siltcontent',
        'claycontent',
        'phwater',
        'cationexchangecapacity',
        'coarsefragmentcontent',
        'calciumcarbonatecontent',
        'organicmattercontent',
        'bulkdensity',
        'availablewateratfieldcapacity',
        'hydraulicconductivityatsaturation']

    soil_data = df[soil_features]

    # Creating soil clusters using k-means
    kmeans_soil = KMeans(n_clusters=3, random_state=42)
    soil_clusters = kmeans_soil.fit_predict(soil_data)

    # Adding weather_cluster and soil_cluster as columns to df
    df['weather_cluster'] = weather_clusters
    df['soil_cluster'] = soil_clusters

    # Calculate cluster means for weather clusters
    weather_cluster_means = weather_data.groupby(weather_clusters).mean()

    # Calculate cluster means for soil clusters
    soil_cluster_means = soil_data.groupby(soil_clusters).mean()

    # Rename rows in "weather_cluster_means_2" based on conditions
    weather_cluster_means_2 = pd.DataFrame()
    weather_cluster_means_2['precsum'] = weather_cluster_means.filter(like='precsum').sum(axis=1)
    weather_cluster_means_2['radsum'] = weather_cluster_means.filter(like='radsum').sum(axis=1)
    weather_cluster_means_2['rhmin'] = weather_cluster_means.filter(like='rhmin').sum(axis=1)
    weather_cluster_means_2['rhmax'] = weather_cluster_means.filter(like='rhmax').sum(axis=1)
    weather_cluster_means_2['tempmin'] = weather_cluster_means.filter(like='tempmin').sum(axis=1)
    weather_cluster_means_2['tempmax'] = weather_cluster_means.filter(like='tempmax').sum(axis=1)

    # Function to rename the weather clusters based on conditions
    def rename_cluster(row):
        if row['precsum'] == weather_cluster_means_2['precsum'].max():
            return 'Wetter'
        elif row['precsum'] == weather_cluster_means_2['precsum'].min():
            return 'Drier'
        else:
            return 'Humid'

    def rename_cluster_temp(row):
        if row['tempmax'] == weather_cluster_means_2['tempmax'].max() and row['radsum'] == weather_cluster_means_2['radsum'].max():
            return 'Warmer'
        elif row['tempmax'] == weather_cluster_means_2['tempmax'].min() and row['radsum'] == weather_cluster_means_2['radsum'].min():
            return 'Cooler'
        else:
            return 'Mild'

    # Applying the renaming functions to create new cluster labels
    weather_cluster_means_2['precsum'] = weather_cluster_means_2.apply(rename_cluster, axis=1)
    weather_cluster_means_2['temp'] = weather_cluster_means_2.apply(rename_cluster_temp, axis=1)
    weather_cluster_means_2 = weather_cluster_means_2.drop(['tempmax', 'tempmin'], axis=1)

    # Function to rename the soil clusters based on conditions
    def rename_cluster_sand(row):
        if row['sandcontent'] == soil_cluster_means['sandcontent'].max():
            return 'Lower_Water_Retention'
        elif row['sandcontent'] == soil_cluster_means['sandcontent'].min():
            return 'Higher_Water_Retention'
        else:
            return 'Balanced_Composition'

    def rename_cluster_om(row):
        if row['organicmattercontent'] == soil_cluster_means['organicmattercontent'].max():
            return 'Higher_Fertility'
        elif row['organicmattercontent'] == soil_cluster_means['organicmattercontent'].min():
            return 'Lower_Fertility'
        else:
            return 'Mid_Fertility'

    # Applying the renaming functions to create new cluster labels
    soil_cluster_means['sandcontent'] = soil_cluster_means.apply(rename_cluster_sand, axis=1)
    soil_cluster_means['organicmattercontent'] = soil_cluster_means.apply(rename_cluster_om, axis=1)

    # Merge 'weather_cluster_means_2' with the original 'df' DataFrame based on the 'weather_cluster' column
    df_weather_cluster_labels = pd.merge(df[['weather_cluster']], weather_cluster_means_2, left_on='weather_cluster',
                                         right_index=True)

    # Merge 'soil_cluster_means' with the original 'df' DataFrame based on the 'soil_cluster' column
    df_soil_cluster_labels = pd.merge(df[['soil_cluster']], soil_cluster_means, left_on='soil_cluster',
                                      right_index=True)

    # Extract the final cluster labels based on the 'Precsum' and 'Temp' columns
    df['WT_cluster_Label'] = df_weather_cluster_labels.apply(lambda row: f"{row['precsum']}_{row['temp']}", axis=1)

    # Extract the final cluster labels based on the 'Sand' and 'OM' columns
    df['ST_cluster_Label'] = df_soil_cluster_labels.apply(
        lambda row: f"{row['sandcontent']}_{row['organicmattercontent']}", axis=1)

    # Concatenating the columns to create 'Env_Cluster' column
    df['Env_Cluster'] = df['WT_cluster_Label'].map(str) + '_' + df['ST_cluster_Label'].map(str)

    return df
