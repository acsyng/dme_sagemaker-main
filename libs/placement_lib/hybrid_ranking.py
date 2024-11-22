"""
This function xxxxxxxxxxxxxxxxxx
"""

import pandas as pd
import numpy as np

import scipy.stats as stats
from sklearn.cluster import KMeans


def rank_hybrids(df, Region_of_Interest):
    # Merge all aggregated dataframes
    aggregated_dfs = []

    for item in Region_of_Interest:
        # Add environment capacity column and aggregate data
        """
        The script calculates the minimum average crop yield for each unique combination of AOI and 'watermgmt'.
        The 'Env_Capacity' is then calculated for each row by dividing the 'predict_ygsmn' by the corresponding minimum average.
        This simple formula converts the Env_Cluster into Env_Capacity, where low-yielding Env_Cluster earn more points
        for being a tougher competition.
        """
        min_avgs = df.groupby([item, 'stage', 'maturity', 'watermgmt', 'Env_Cluster'])['predict_ygsmn'].transform(
            'mean')
        min_avgs_min = df.groupby([item, 'stage', 'maturity', 'watermgmt', 'Env_Cluster'])['predict_ygsmn'].transform(
            'mean').groupby([df[item], df['watermgmt']]).transform('min')
        df['min_avgs'] = min_avgs
        df['Env_Capacity'] = min_avgs_min / min_avgs

        # Create the aggregated data frame
        aggregated_df = df.groupby([item, 'stage', 'maturity', 'watermgmt']).agg(
            n_obs=('predict_ygsmn', 'size'),
            YGSMNp_Min=('predict_ygsmn', 'min'),
            YGSMNp_25th=('predict_ygsmn', lambda x: np.percentile(x, 25)),
            YGSMNp_Median=('predict_ygsmn', 'median'),
            YGSMNp_Mean=('predict_ygsmn', 'mean'),
            YGSMNp_75th=('predict_ygsmn', lambda x: np.percentile(x, 75)),
            YGSMNp_Max=('predict_ygsmn', 'max')
        ).reset_index()

        # Calculate confidence interval
        confidence_interval = 0.95
        aggregated_df[['YGSMNp_Lower95', 'YGSMNp_Upper95']] = np.nan

        for index, row in aggregated_df.iterrows():
            location = row[item]
            stage = row['stage']
            maturity = row['maturity']
            irrigation = row['watermgmt']
            group = df[(df[item] == location) & (df['stage'] == stage) & (df['maturity'] == maturity) & (
                        df['watermgmt'] == irrigation)]
            YGSMNp_values = group['predict_ygsmn'].values
            lower, upper = stats.t.interval(confidence_interval, len(YGSMNp_values) - 1, loc=np.mean(YGSMNp_values),
                                            scale=stats.sem(YGSMNp_values))
            aggregated_df.loc[index, 'YGSMNp_Lower95'] = lower
            aggregated_df.loc[index, 'YGSMNp_Upper95'] = upper

        # Rank hybrids based on Crop_Yield total points
        """
        Step 1:
        Calculate the performance points for each hybrid in each Env_Cluster based on
        their yield performances and the environment capacities. Use a simple
        formula to convert the yield into points, where higher yields earn more points.
        The points will be directly proportional to the yield values and multiplied by the environment capacity.
        """
        temp_df = df.groupby([item, 'stage', 'maturity', 'watermgmt', 'be_bid', 'Env_Cluster']).agg(
            {'predict_ygsmn': 'mean', 'Env_Capacity': 'first'}).reset_index()
        temp_df['Performance_Points'] = temp_df['predict_ygsmn'] * temp_df['Env_Capacity']

        """
        Step 2:
        Calculate the total points for each hybrid in each location and irrigation
        combination by summing up their performance points across all Env_Clusters for that combination.
        Sort temp_df based on location, irrigation, and Total_Points in descending order
        to get the hybrids with the highest total points at the top.
        """
        temp_df['Total_Points'] = temp_df.groupby([item, 'stage', 'maturity', 'watermgmt', 'be_bid'])[
            'Performance_Points'].transform('mean')
        temp_df.sort_values([item, 'stage', 'maturity', 'watermgmt', 'Total_Points'],
                            ascending=[True, True, True, True, False], inplace=True)

        # Assign variable ranks to hybrids in aggregated_df based on Total_Points
        grouped_df = temp_df.groupby([item, 'stage', 'maturity', 'watermgmt'])
        max_bids = grouped_df['be_bid'].nunique().max()
        ranks = [f"Rank{i}" for i in range(1, max_bids + 1)]
        aggregated_df[ranks] = ''

        for (location, stage, maturity, irrigation), group in grouped_df:
            group_sorted = group.sort_values('Total_Points', ascending=False, ignore_index=True)
            be_bids = group_sorted['be_bid'].values
            rank_counter = 0
            be_bid_counter = 0

            while rank_counter < len(ranks) and be_bid_counter < len(be_bids) and rank_counter < max_bids:
                be_bid = be_bids[be_bid_counter]
                if be_bid not in aggregated_df[(aggregated_df[item] == location) & (aggregated_df['stage'] == stage) &
                                               (aggregated_df['maturity'] == maturity) & (
                                                       aggregated_df['watermgmt'] == irrigation)][
                    ranks].values.flatten():
                    aggregated_df.loc[(aggregated_df[item] == location) & (aggregated_df['stage'] == stage) & (
                                aggregated_df['maturity'] == maturity) &
                                      (aggregated_df['watermgmt'] == irrigation), ranks[rank_counter]] = be_bid
                    rank_counter += 1
                be_bid_counter += 1

        # Get the name of the first column in aggregated_df
        first_col_name = aggregated_df.columns[0]

        # Rename the first column to 'region_name'
        aggregated_df.rename(columns={first_col_name: 'region_name'}, inplace=True)

        # Add a new column 'region_type' to the beginning of the DataFrame
        aggregated_df.insert(0, 'region_type', item)

        aggregated_dfs.append(aggregated_df)

    # Merge all aggregated dataframes
    final_df = pd.concat(aggregated_dfs, ignore_index=True)

    # Get the list of "Rank" columns from final_df
    rank_cols = [col for col in final_df.columns if col.startswith("Rank")]

    # Combine the "Rank" columns into one "Rank" column with numeric rank values
    final_df_melted = final_df.melt(
        id_vars=["region_type", "region_name", "stage", "maturity", "watermgmt"],
        value_vars=rank_cols,
        var_name="Rank_num",
        value_name="be_bid"
    )

    # Convert the rank numbers to integers
    final_df_melted["Rank"] = final_df_melted["Rank_num"].str.extract(r"(\d+)").astype(int)

    # Drop rows with NaN values in the "be_bid" column
    final_df_2 = final_df_melted.dropna(subset=["be_bid"])[
        ["region_type", "region_name", "stage", "maturity", "watermgmt", "Rank", "be_bid"]]

    # Drop rows where "be_bid" is empty in the "Rank" column
    final_df_2 = final_df_2[final_df_2["be_bid"].str.strip() != ""]

    # Reset the index of the final DataFrame
    final_df_2.reset_index(drop=True, inplace=True)

    return final_df_2


"""
ranking_post_process:  post processing on be_bid ranking results, need to update the code if there is any change in dataset or region type 
input:
    dt: panda dataframe, the output be-bid ranking table.
    ap_data_sector: string;
    analysis_year: integer.
output:
    dt: processed be-bid ranking table
"""


def ranking_post_process(dt, ap_data_sector, analysis_year):
    try:
        dt.rename(columns={'watermgmt': 'irrigation'}, inplace=True)
    except:
        pass
    dt['ap_data_sector'] = ap_data_sector
    dt['analysis_year'] = analysis_year
    dt['region_type'] = 'wce'
    try:
        dt.rename(columns={'be_bid': 'entry_id'}, inplace=True)
    except:
        pass
    try:
        dt.rename(columns={'Rank': 'rank'}, inplace=True)
    except:
        pass

    return dt
