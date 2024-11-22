import argparse
import json
import os

import numpy as np
import pandas as pd

from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.lgbm_utils import create_output, create_output_soy, create_output_cornsilage
from libs.placement_lib.lgbm_utils import load_object, find_latest_model, folder_files
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT


# Read recipe inputs
# load the model
def train_data_lgbm_1_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args, logger):
    try:

        model_fpath = os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                                   pipeline_runid)
        print('model_fpath: ', model_fpath)

        # estimator_path = os.path.join(partition_path, 'lightgbm_model.pkl')
        # estimator_file_name = 'lightgbm_model.pkl'
        files_in_folder = folder_files(
            os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm',
                         DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, ''))
        print('files_in_folder: ', files_in_folder)
        latest_model, m_date, m_time = find_latest_model(files_in_folder, False)
        # estimator = joblib.load(estimator_path)
        estimator = load_object(model_fpath, latest_model)
        print('estimator loaded')

        latest_model_allmodels, m_date, m_time = find_latest_model(files_in_folder, True)
        allmodels_estimator = load_object(model_fpath, latest_model_allmodels)
        print('allmodels_estimator loaded')

        allmodels_estimator_10 = allmodels_estimator['q 0.10']
        allmodels_estimator_50 = allmodels_estimator['q 0.50']
        allmodels_estimator_90 = allmodels_estimator['q 0.90']

        rfecv_mask_path = os.path.join(model_fpath, 'feature_mask.csv')
        print('rfecv_mask_path: ', rfecv_mask_path)
        rfecv_mask = np.loadtxt(rfecv_mask_path, dtype="float64", delimiter=",", usecols=0)
        print('rfecv_mask loaded')
        # load train data
        # train_data = dataiku.Dataset("train_data")
        # train_data_df = train_data.get_dataframe()
        train_data_df = pd.read_parquet(
            os.path.join(args.s3_input_trial_data_train_folder, 'train_data', DKU_DST_ap_data_sector,
                         DKU_DST_analysis_year, 'train_data.parquet'))
        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            train_data_df = train_data_df[train_data_df.tpp_region != 'B']
            print('train_data_df shape after filtering: ', train_data_df.shape)
        if DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER":
            # train_data_df = train_data_df[train_data_df.tpp_region != '4']
            print('train_data_df shape after filtering: ', train_data_df.shape)
        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            train_data_df = train_data_df[(train_data_df.tpp_region != 'B') & (train_data_df.tpp_region != 'P')]
            print('train_data_df shape after filtering: ', train_data_df.shape)
        # load the supplementary trial information
        # trial_info = dataiku.Dataset("trial_info")
        # trial_info_df = trial_info.get_dataframe()
        trial_info_df = pd.read_parquet(
            os.path.join(args.s3_input_trial_data_train_folder, 'trial_info_data', DKU_DST_ap_data_sector,
                         DKU_DST_analysis_year, 'trial_info_data.parquet'))

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            train_data_lgbm_df = create_output_soy(train_data_df, DKU_DST_ap_data_sector, trial_info_df, estimator,
                                                   allmodels_estimator_10, allmodels_estimator_50,
                                                   allmodels_estimator_90, rfecv_mask)
        elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            train_data_lgbm_df = create_output_cornsilage(train_data_df, trial_info_df, estimator,
                                                          allmodels_estimator_10, allmodels_estimator_50,
                                                          allmodels_estimator_90,
                                                          rfecv_mask,
                                                          DKU_DST_ap_data_sector)
        else:
            train_data_lgbm_df = create_output(train_data_df, trial_info_df, estimator,
                                               allmodels_estimator_10, allmodels_estimator_50, allmodels_estimator_90,
                                               rfecv_mask,
                                               DKU_DST_ap_data_sector)

        train_data_lgbm_df_dir_path = os.path.join('/opt/ml/processing/data/train_data_lgbm_1', DKU_DST_ap_data_sector,
                                                   DKU_DST_analysis_year)
        train_data_lgbm_df_data_path = os.path.join(train_data_lgbm_df_dir_path, 'train_data_lgbm.parquet')
        print('train_data_lgbm_importance_data_path: ', train_data_lgbm_df_data_path)
        isExist = os.path.exists(train_data_lgbm_df_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(train_data_lgbm_df_dir_path)

        # Write recipe outputs
        train_data_lgbm_df.to_parquet(train_data_lgbm_df_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement train_data_lgbm_1 error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm model folder', required=True)
    parser.add_argument('--s3_input_trial_data_train_folder', type=str,
                        help='s3 input trial data train folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        # years = ['2020', '2021', '2022']
        # years = ['2023']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm_1/data/train_data_lgbm_1',
                                     DKU_DST_ap_data_sector, input_year, 'train_data_lgbm.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            train_data_lgbm_1_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args, logger=logger)
            print('File created')
            # print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
