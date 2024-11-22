import argparse
import csv
import json
import os

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import GridSearchCV, GroupKFold

from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.lgbm_utils import create_model_input, create_model_input_soy, create_model_input_cornsilage
from libs.placement_lib.lgbm_utils import save_object, generate_file_identifier
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix


def train_data_lgbm_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, file_path_train_data, args, logger):
    try:
        # Read recipe inputs
        print("Reading inputs")
        train_df = pd.read_parquet(
            os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'train_data.parquet'))

        print('train_df shape: ', train_df.shape)
        print("Completed reading inputs")

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == "CORN_BRAZIL_SAFRINHA":
            train_df = train_df[train_df.tpp_region != 'B']
            print('train_df shape after filtering: ', train_df.shape)
        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            train_df = train_df[(train_df.tpp_region != 'B') & (train_df.tpp_region != 'P')]
            print('train_df shape after filtering: ', train_df.shape)
        if DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER":
            # train_df = train_df[(train_df.tpp_region != '4')]
            print('train_df shape after filtering: ', train_df.shape)
        # Compute recipe outputs from inputs
        trial_group, trial_names = pd.factorize(train_df.loc_selector)

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            train_df, y = create_model_input_soy(train_df, DKU_DST_ap_data_sector, np.empty(shape=0))
        elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            train_df, y = create_model_input_cornsilage(train_df, np.empty(shape=0), DKU_DST_ap_data_sector)
        else:
            train_df, y = create_model_input(train_df, np.empty(shape=0), DKU_DST_ap_data_sector)
        print('train_df shape after create model input: ', train_df.shape)
        print('y shape after create model input: ', y.shape)

        # Model Pipeline - LightGBM-based
        # cross-validation scheme
        gkf = GroupKFold(n_splits=3)

        estimator = lgb.LGBMRegressor(boosting_type='gbdt',
                                      importance_type='gain')

        rfecv_mask = np.full((train_df.shape[1]), True)
        # train_df = train_df.loc[:,rfecv_mask]

        # Build final model
        print("Building & tuning LightGBM model")

        # param_grid = {
        #     'num_leaves': [50, 150],
        #     'learning_rate': [0.05, 0.2],
        #     'n_estimators': [500, 1500]
        # }

        param_grid = {
            # 'num_leaves': [50, 100, 150],
            'num_leaves': [300, 700],
            'learning_rate': [0.05, 0.01],
            'n_estimators': [500, 1000, 750]
        }

        search = GridSearchCV(estimator,
                              param_grid,
                              cv=gkf.split(train_df, y, groups=trial_group),
                              refit=True,
                              verbose=3,
                              pre_dispatch='1*n_jobs',
                              scoring="neg_mean_squared_error")
        search.fit(train_df, y, groups=trial_group)

        print("Best parameter (CV score=%0.3f):" % (-search.cv_results_['mean_test_score'][search.best_index_]))
        print(search.cv_results_['params'][search.best_index_])

        params_opt = search.cv_results_['params'][search.best_index_]
        estimator = search.best_estimator_

        all_models = {}

        def param_convert(files):
            store = dict()
            for key, value in files.items():
                store[key] = [value]
            return store

        common_params = dict(search.cv_results_['params'][search.best_index_])
        print('common_params: ', common_params)
        common_params_convert = param_convert(common_params)
        print('common_params after convert: ', common_params_convert)

        for alpha in [0.1, 0.5, 0.9]:
            gbr = lgb.LGBMRegressor(boosting_type='gbdt',
                                    silent=True,
                                    importance_type='gain',
                                    objective='quantile', alpha=alpha)  # , **common_params)

            search_int = GridSearchCV(gbr,
                                      param_grid=common_params_convert,
                                      cv=gkf.split(train_df, y, groups=trial_group),
                                      refit=True,
                                      verbose=3,
                                      pre_dispatch='1*n_jobs',
                                      scoring="neg_mean_squared_error")

            search_int.fit(train_df, y, groups=trial_group)
            all_models["q %1.2f" % alpha] = search_int.best_estimator_

        print('all_models')
        print(all_models)

        feature_importance_df = pd.DataFrame({"analysis_year": np.full(train_df.shape[1], int(DKU_DST_analysis_year)),
                                              "feature": train_df.columns,
                                              "importance": search.best_estimator_.feature_importances_ / np.sum(
                                                  search.best_estimator_.feature_importances_)})

        train_data_lgbm_dir_path = os.path.join('/opt/ml/processing/data/train_data_lgbm', DKU_DST_ap_data_sector,
                                                DKU_DST_analysis_year, pipeline_runid)
        train_data_lgbm_importance_data_path = os.path.join(train_data_lgbm_dir_path, 'train_data_lgbm_importance.parquet')
        print('train_data_lgbm_importance_data_path: ', train_data_lgbm_importance_data_path)
        isExist = os.path.exists(train_data_lgbm_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(train_data_lgbm_dir_path)

        # Write recipe outputs
        feature_importance_df.to_parquet(train_data_lgbm_importance_data_path)
        file_identifier = generate_file_identifier()

        model_file_name = "lightgbm_model_" + file_identifier + '.pkl'
        all_models_file_name = "lightgbm_allmodels_" + file_identifier + '.pkl'

        save_object(estimator, train_data_lgbm_dir_path, model_file_name)
        save_object(all_models, train_data_lgbm_dir_path, all_models_file_name)

        np.savetxt(os.path.join(train_data_lgbm_dir_path, "feature_mask.csv"), rfecv_mask, delimiter=",")

        param_name = "params_opt_" + file_identifier + '.csv'
        # params_opt.to_csv(os.path.join(train_data_lgbm_dir_path, param_name))
        with open(os.path.join(train_data_lgbm_dir_path, param_name), 'w') as f:
            w = csv.writer(f)
            w.writerow(params_opt.keys())
            w.writerow(params_opt.values())

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement train_data_lgbm error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm model folder', required=True)
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
            # file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm', DKU_DST_ap_data_sector, input_year, 'lightgbm_model.pkl')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            train_data_file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_trial_data_train/data/train_data',
                                                DKU_DST_ap_data_sector, input_year, 'train_data.parquet')

            # print('Creating file in the following location: ', file_path)
            train_data_lgbm_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, train_data_file_path,
                                     args=args, logger=logger)
            print('File created')
            print()

            # else:
            # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
