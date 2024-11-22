import argparse
import json
import math
import os
import random

import numpy as np
import pandas as pd

from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix

"""
compute_trial_data_train()

Combines trial data, cropFact output, hetpool1 pca, and hetpool2 pca and performs a bit of preprocessing to 
create the training dataset, test dataset, and a trial info df to be able to use after the model is fit.
"""


def trial_data_train_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid,
                              file_path_pca_output_hetpool1, file_path_pca_output_hetpool2, args, logger):
    try:
        cropFact_df = pd.read_parquet(
            os.path.join(args.s3_input_cropFact_api_output_all_years_folder, DKU_DST_ap_data_sector,
                         'cropFact_api_output_all_years.parquet'))
        cropFact_df["year"] = cropFact_df["year"].astype('int')
        cropFact_df = cropFact_df.loc[cropFact_df["year"] <= int(DKU_DST_analysis_year),]
        print('cropFact_df["year"]: ', cropFact_df["year"].unique().tolist())

        trial_data_df = pd.read_parquet(os.path.join(args.s3_input_trial_data_all_years_folder, DKU_DST_ap_data_sector,
                                                     'trial_data_all_years.parquet'))
        trial_data_df["year"] = trial_data_df["year"].astype('int')
        trial_data_df = trial_data_df.loc[trial_data_df["year"] <= int(DKU_DST_analysis_year),]
        print('trial_data_df["year"]: ', trial_data_df["year"].unique().tolist())

        trial_data_keep_df = pd.read_parquet(
            os.path.join(args.s3_input_trial_feature_keep_array_folder, DKU_DST_ap_data_sector))
        keep_arr = np.ravel(trial_data_keep_df.to_numpy())
        print(keep_arr.shape)

        # Compute recipe outputs
        # Create separate df of all trial info that may be useful for viz/validation/explanation but isn't useful for modeling
        trial_data_df["analysis_year"] = np.full(trial_data_df.shape[0], int(DKU_DST_analysis_year))
        trial_data_df["irrflag"] = np.select([trial_data_df["irrigation"] == "NONE",
                                              trial_data_df["irrigation"] == "TILE",
                                              trial_data_df["irrigation"] == "LIRR",
                                              trial_data_df["irrigation"] == "IRR"], [0, 0, 1, 2])
        trial_data_df["tileflag"] = (trial_data_df["irrigation"] == "TILE").astype(int)
        trial_info_cols = ["experiment_id", "trial_status", "trial_stage", "irrigation", "plant_date", "harvest_date",
                           "x_longitude", "y_latitude", "state_province_code"]
        trial_info_df = trial_data_df.loc[:,
                        ["year", "ap_data_sector", "trial_id", "analysis_year"] + trial_info_cols].drop(
            columns=["plant_date", "harvest_date"]).drop_duplicates()

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            keep_arr = np.concatenate(([1, 1, 1], keep_arr))
            trial_data_df = trial_data_df.merge(cropFact_df.loc[:, keep_arr == 1],
                                                on=['year', 'ap_data_sector', 'cropfact_id'],
                                                how="inner") \
                .drop(
                columns=trial_info_cols + ["cropfact_id", "material_id", "match_be_bid", "GMSTP_cf", "GMSTP"])

            # read in appropriate year of hetpool1_pca data
            hetpool1_pca_df = pd.read_parquet(file_path_pca_output_hetpool1)

            # Merge in geno info
            trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                left_on="sample_id",
                                                right_on="sample_id_par1",
                                                how="left",
                                                suffixes=("", "_par1")).drop(
                columns=["sample_id", "sample_id_par1"])

            # Split into train and test
            # trial_data_train_df = trial_data_df.loc[(trial_data_df["year"] < int(DKU_DST_analysis_year)) & (
            #            ~(trial_data_df["comp_0_par1"].isna()) | ~(trial_data_df["comp_0_par2"].isna())), :]
            # trial_data_test_df = trial_data_df.loc[(trial_data_df["year"] == int(DKU_DST_analysis_year)) & (
            #             ~(trial_data_df["comp_0_par1"].isna()) | ~(trial_data_df["comp_0_par2"].isna())), :]
            trial_data_train_df_previous_years = trial_data_df.loc[(trial_data_df["year"] < int(
                DKU_DST_analysis_year)) & (~(trial_data_df["comp_0_par1"].isna())), :]

            trial_data_train_current_year = trial_data_df.loc[(trial_data_df["year"] == int(
                DKU_DST_analysis_year)) & (~(trial_data_df["comp_0_par1"].isna())),
                                            :]  # .sample(frac=0.7,random_state=0)
            print('trial_data_train_current_year', trial_data_train_current_year.shape)
        else:
            # Merge in cropFact info

            # keep_arr = np.concatenate(([1, 1, 1], keep_arr))

            if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "GMSTP_cf", "GMSTP"]
                keep_arr = np.concatenate(([1, 1, 1], keep_arr))
            elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "fp_het_group", "mp_het_group", "SDMCP"]
                keep_arr = np.concatenate(([1, 1, 1, 0, 0, 0, 0, 0], keep_arr))
            else:
                col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "fp_het_group", "mp_het_group", "GMSTP_cf",
                               "GMSTP"]
                keep_arr = np.concatenate(([1, 1, 1], keep_arr))
            trial_data_df = trial_data_df.merge(cropFact_df.loc[:, keep_arr == 1],
                                                on=['year', 'ap_data_sector', 'cropfact_id'],
                                                how="inner") \
                .drop(columns=trial_info_cols + col_to_drop)

            # read in appropriate year of hetpool1_pca data
            hetpool1_pca_df = pd.read_parquet(file_path_pca_output_hetpool1)

            # read in appropriate year of hetpool2_pca data
            hetpool2_pca_df = pd.read_parquet(file_path_pca_output_hetpool2)
            # Merge in geno info

            if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                    left_on="par_hp1_be_bid",
                                                    right_on="be_bid_par1",
                                                    how="left",
                                                    suffixes=("", "_par1")) \
                    .merge(hetpool2_pca_df.add_suffix("_par2"),
                           left_on="par_hp2_be_bid",
                           right_on="be_bid_par2",
                           how="left",
                           suffixes=("", "_par2")).drop(
                    columns=["par_hp1_be_bid", "be_bid_par1", "par_hp2_be_bid", "be_bid_par2"])
            else:
                trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                    left_on="par_hp1_sample",
                                                    right_on="sample_id_par1",
                                                    how="left",
                                                    suffixes=("", "_par1")) \
                    .merge(hetpool2_pca_df.add_suffix("_par2"),
                           left_on="par_hp2_sample",
                           right_on="sample_id_par2",
                           how="left",
                           suffixes=("", "_par2")).drop(
                    columns=["par_hp1_sample", "sample_id_par1", "par_hp2_sample", "sample_id_par2"])

            # Split into train and test
            # trial_data_train_df = trial_data_df.loc[(trial_data_df["year"] < int(DKU_DST_analysis_year)) & (
            #            ~(trial_data_df["comp_0_par1"].isna()) | ~(trial_data_df["comp_0_par2"].isna())), :]
            # trial_data_test_df = trial_data_df.loc[(trial_data_df["year"] == int(DKU_DST_analysis_year)) & (
            #             ~(trial_data_df["comp_0_par1"].isna()) | ~(trial_data_df["comp_0_par2"].isna())), :]
            trial_data_train_df_previous_years = trial_data_df.loc[(trial_data_df["year"] < int(
                DKU_DST_analysis_year)) &
                                                                   (~(trial_data_df["comp_0_par1"].isna()) | ~(
                                                                       trial_data_df["comp_0_par2"].isna())), :]

            trial_data_train_current_year = trial_data_df.loc[(trial_data_df["year"] == int(
                DKU_DST_analysis_year)) &
                                                              (~(trial_data_df["comp_0_par1"].isna()) | ~(
                                                                  trial_data_df["comp_0_par2"].isna())),
                                            :]  # .sample(frac=0.7,random_state=0)
            print('trial_data_train_current_year', trial_data_train_current_year.shape)

        loc_selector_list = trial_data_train_current_year['loc_selector'].unique().tolist()
        trial_data_train_current_year_loc_selector = random.sample(loc_selector_list, int(0.7 * len(loc_selector_list)))

        trial_data_train_current_year_subset = trial_data_train_current_year.loc[
            trial_data_train_current_year['loc_selector'].isin(trial_data_train_current_year_loc_selector)]
        print('trial_data_train_current_year_subset', trial_data_train_current_year_subset.shape)
        trial_data_train_df = pd.concat([trial_data_train_df_previous_years, trial_data_train_current_year_subset],
                                        ignore_index=True)

        # trial_data_test_df = trial_data_df.loc[(trial_data_df["year"]== int(dataiku.dku_flow_variables["DKU_DST_analysis_year"]))  &
        #                                        (~(trial_data_df["comp_0_par1"].isna()) | ~(trial_data_df["comp_0_par2"].isna())), :].drop(trial_data_train_current_year.index)

        trial_data_test_df = trial_data_train_current_year.loc[
            ~(trial_data_train_current_year['loc_selector'].isin(trial_data_train_current_year_loc_selector))]
        print('trial_data_test_df', trial_data_test_df.shape)

        # idea: stratify on loc_selector and maturity, hold out 20-30 % of loc codes within each maturity. Locations can have multiple maturities.
        # 30% of loc codes randomly *** method used

        train_data_by_year_dir_path = os.path.join('/opt/ml/processing/data/train_data', DKU_DST_ap_data_sector,
                                                   DKU_DST_analysis_year)
        train_data_by_year_data_path = os.path.join(train_data_by_year_dir_path, 'train_data.parquet')
        print('train_data_by_year_data_path: ', train_data_by_year_data_path)
        isExist = os.path.exists(train_data_by_year_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(train_data_by_year_dir_path)

        batch_size = 10000
        sample_n = trial_data_train_df.shape[0]
        sample_n_iter = int(math.ceil(sample_n / batch_size))
        for i in range(sample_n_iter):
            df = trial_data_train_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :]
            if i == 0:
                # trial_data_train.write_schema_from_dataframe(df)
                df.to_parquet(train_data_by_year_data_path, engine='fastparquet')
            # writer.write_dataframe(df)
            df.to_parquet(train_data_by_year_data_path, engine='fastparquet', append=True)

        test_data_by_year_dir_path = os.path.join('/opt/ml/processing/data/test_data', DKU_DST_ap_data_sector,
                                                  DKU_DST_analysis_year)
        test_data_by_year_data_path = os.path.join(test_data_by_year_dir_path, 'test_data.parquet')
        print('test_data_by_year_data_path: ', test_data_by_year_data_path)
        isExist = os.path.exists(test_data_by_year_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(test_data_by_year_dir_path)
        trial_data_test_df.to_parquet(test_data_by_year_data_path)

        trial_info_data_by_year_dir_path = os.path.join('/opt/ml/processing/data/trial_info_data',
                                                        DKU_DST_ap_data_sector, DKU_DST_analysis_year)
        trial_info_data_by_year_data_path = os.path.join(trial_info_data_by_year_dir_path, 'trial_info_data.parquet')
        print('trial_info_data_by_year_data_path: ', trial_info_data_by_year_data_path)
        isExist = os.path.exists(trial_info_data_by_year_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(trial_info_data_by_year_dir_path)
        trial_info_df.to_parquet(trial_info_data_by_year_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement trial_data_train error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_pca_output_hetpool1_folder', type=str,
                        help='s3 input pca output hetpool1 folder', required=True)
    parser.add_argument('--s3_input_pca_output_hetpool2_folder', type=str,
                        help='s3 input pca output hetpool2 folder', required=True)
    parser.add_argument('--s3_input_trial_feature_keep_array_folder', type=str,
                        help='s3 input trial feature keep array folder', required=True)
    parser.add_argument('--s3_input_trial_data_all_years_folder', type=str,
                        help='s3 input trial data all years folder', required=True)
    parser.add_argument('--s3_input_cropFact_api_output_all_years_folder', type=str,
                        help='s3 input cropFact api output all years folder', required=True)
    args = parser.parse_args()
    print('args collected')
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)
        # Read recipe inputs
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_trial_data_train/data/train_data',
                                     DKU_DST_ap_data_sector, input_year,
                                     'train_data.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            pca_output_hetpool1_file_path = os.path.join(args.s3_input_pca_output_hetpool1_folder,
                                                         DKU_DST_ap_data_sector, input_year, 'hetpool1_pca.parquet')

            pca_output_hetpool2_file_path = os.path.join(args.s3_input_pca_output_hetpool2_folder,
                                                         DKU_DST_ap_data_sector, input_year, 'hetpool2_pca.parquet')

            print('Creating file in the following location: ', file_path)
            trial_data_train_function(DKU_DST_ap_data_sector, input_year, pipeline_runid,
                                      pca_output_hetpool1_file_path, pca_output_hetpool2_file_path, args=args, logger=logger)
            print('File created')
            print()

            # else:
            #     print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
