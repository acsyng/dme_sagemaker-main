import argparse
import json
import os

import boto3
import numpy as np
import pandas as pd

from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.lgbm_utils import create_model_input, find_latest_model, folder_files, load_object, \
    create_model_input_soy, create_model_input_cornsilage
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_lib.ui_grid_utils import merge_parent_geno, merge_parent_geno_soy, merge_cfgrid_entry
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def grid_performance_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, DKU_DST_maturity, pipeline_runid, args,
                              logger):
    try:
        # Read recipe inputs
        model_fpath = os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                                   pipeline_runid)
        print('model_fpath: ', model_fpath)
        files_in_folder = folder_files(
            os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm',
                         DKU_DST_ap_data_sector,
                         DKU_DST_analysis_year, pipeline_runid, ''))
        print('files_in_folder: ', files_in_folder)
        latest_model, m_date, m_time = find_latest_model(files_in_folder)
        print('latest_model: ', latest_model)
        estimator = load_object(model_fpath, latest_model)
        print('estimator loaded')

        latest_model_allmodels, m_date, m_time = find_latest_model(files_in_folder, True)
        allmodels_estimator = load_object(model_fpath, latest_model_allmodels)
        print('allmodels_estimator loaded')

        allmodels_estimator_10 = allmodels_estimator['q 0.10']
        allmodels_estimator_50 = allmodels_estimator['q 0.50']
        allmodels_estimator_90 = allmodels_estimator['q 0.90']

        # cropFact_grid_for_model = dataiku.Dataset("cropFact_grid_for_model")
        # cropFact_grid_df = cropFact_grid_for_model.get_dataframe()
        s3 = boto3.resource('s3')
        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)
        cropFact_grid_df = pd.DataFrame()
        for obj in bucket.objects.filter(
                Prefix=os.path.join(get_s3_prefix(ENVIRONMENT),
                                    'compute_cropFact_grid_for_model/data/cropFact_grid_for_model',
                                    DKU_DST_ap_data_sector, DKU_DST_maturity)):
            # print(obj.key)
            df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                df['Market_Days'] = df['Market']
                print('shape after read: ', df.shape)
                df = df[df.irrigation == 'NON-IRRIGATED']
                print('shape after filtering: ', df.shape)
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                df['Market_Days'] = df['Market'].str[0].astype(int)
            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                df['Market_Days'] = df['maturity']
                # print('shape after read: ', df.shape)
                # df = df[df.irrigation == 'NON-IRRIGATED']
                # print('shape after filtering: ', df.shape)
            if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
                df['Market_Days'] = df['Market']
                print('shape after read: ', df.shape)
                df = df[df.irrigation == 'NON-IRRIGATED']
                print('shape after filtering: ', df.shape)

            # print('df shape: ', df.shape)
            cropFact_grid_df = pd.concat([cropFact_grid_df, df], ignore_index=True)

        print('cropFact_grid_df shape: ', cropFact_grid_df.shape)

        # print('maturity(s)', cropFact_grid_df['maturity'].unique().tolist())

        # dg_be_bids = dataiku.Dataset("dg_be_bids")
        # dg_be_bids_df = dg_be_bids.get_dataframe()
        dg_be_bids_df = pd.read_parquet(
            os.path.join(args.s3_input_grid_classification_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'dg_be_bids.parquet'))
        print('dg_be_bids_df: ', dg_be_bids_df.shape)
        # hetpool1_pca = dataiku.Dataset("hetpool1_pca")
        # hetpool1_pca_df = hetpool1_pca.get_dataframe()
        hetpool1_pca_df = pd.read_parquet(
            os.path.join(args.s3_input_hetpool1_pca_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'hetpool1_pca.parquet'))
        print('hetpool1_pca_df: ', hetpool1_pca_df.shape)

        # hetpool2_pca = dataiku.Dataset("hetpool2_pca")
        # hetpool2_pca_df = hetpool2_pca.get_dataframe()
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            pass
        else:
            hetpool2_pca_df = pd.read_parquet(
                os.path.join(args.s3_input_hetpool2_pca_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                             'hetpool2_pca.parquet'))
            print('hetpool2_pca_df: ', hetpool2_pca_df.shape)

        # test_data = dataiku.Dataset("test_data")
        # test_data_df = test_data.get_dataframe()
        test_data_df = pd.read_parquet(
            os.path.join(args.s3_input_test_data_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'test_data.parquet'))
        print('test_data_df: ', test_data_df.shape)

        rfecv_mask_path = os.path.join(model_fpath, 'feature_mask.csv')
        print('rfecv_mask_path: ', rfecv_mask_path)
        rfecv_mask = np.loadtxt(rfecv_mask_path, dtype="float64", delimiter=",", usecols=0)
        print('rfecv_mask loaded')
        print('rfecv_mask shape', rfecv_mask.shape)
        # Compute intermediate datasets
        # Create parent genotypic input
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            num_maturity = int(DKU_DST_maturity[2])
            target_market_days = num_maturity * 5 + 80
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            target_market_days = int(DKU_DST_maturity)
            num_maturity = target_market_days
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            target_market_days = int(DKU_DST_maturity[0])
            num_maturity = target_market_days
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            target_market_days = int(DKU_DST_maturity)
            num_maturity = target_market_days
        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            target_market_days = int(DKU_DST_maturity)
            num_maturity = target_market_days

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            dg_be_bids_df = merge_parent_geno_soy(dg_be_bids_df, hetpool1_pca_df, target_market_days, drop_stage=True)
            # Create template of what is needed to run the model
            print('test_data_df before create model input: ', test_data_df.shape)
            X_template, _ = create_model_input_soy(test_data_df, DKU_DST_ap_data_sector, rfecv_mask)
            print('X_template after create model input: ', X_template.shape)
        else:
            dg_be_bids_df = merge_parent_geno(dg_be_bids_df, hetpool1_pca_df, hetpool2_pca_df, target_market_days,
                                              drop_stage=True)
            # Create template of what is needed to run the model
            print('test_data_df before create model input: ', test_data_df.shape)
            if DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                X_template, _ = create_model_input_cornsilage(test_data_df, rfecv_mask, DKU_DST_ap_data_sector)
            else:
                X_template, _ = create_model_input(test_data_df, rfecv_mask, DKU_DST_ap_data_sector)
            print('X_template after create model input: ', X_template.shape)

        # Create & write recipe output
        grid_performance_dir_path = os.path.join('/opt/ml/processing/data/grid_performance', DKU_DST_ap_data_sector,
                                                 DKU_DST_analysis_year, pipeline_runid, DKU_DST_maturity)

        grid_performance_data_path = os.path.join(grid_performance_dir_path, 'grid_performance.parquet')

        grid_performance_last_three_years_data_path = os.path.join(grid_performance_dir_path,
                                                                   'grid_performance_last_three_years.parquet')
        print('grid_performance_data_path: ', grid_performance_data_path)
        isExist = os.path.exists(grid_performance_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(grid_performance_dir_path)

        # Initialize output dataset
        # model_prediction_input = dataiku.Dataset("grid_performance")
        idx_position_counter = 100000000 * (num_maturity + 10 * (
                int(DKU_DST_analysis_year) - 2021))  # used to generate "id" column required by pgsql output schema
        # with model_prediction_input.get_writer() as writer:
        year_series = cropFact_grid_df[["year", "irrigation"]].drop_duplicates()
        # Iterate through DG groups x cF grid years to create model output, then do other transformations
        rangeList = range(year_series.shape[0])

        for i in range(year_series.shape[0]):

            print(year_series.year.iloc[i], year_series.irrigation.iloc[i])
            print('shapes: ', cropFact_grid_df.shape, dg_be_bids_df.shape)
            if dg_be_bids_df.shape[0] == 0 or cropFact_grid_df.shape[0] == 0:
                continue
            df_out = merge_cfgrid_entry(cropFact_grid_df, dg_be_bids_df, X_template, estimator,
                                        year_series.year.iloc[i], year_series.irrigation.iloc[i],
                                        allmodels_estimator_10, allmodels_estimator_50, allmodels_estimator_90)

            # Perform transformations - in this case, select only columns of interest and write to output table.
            # select columns
            df_out["stage"] = None
            if DKU_DST_ap_data_sector != 'SOY_NA_SUMMER':
                df_out["previous_crop_corn"] = (df_out["previous_crop_corn"] == 2)
            print('df out columns')
            print(df_out.columns.values)
            if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                df_out = df_out[
                    ['place_id', 'be_bid', 'stage', 'maturity', 'year', 'cperf', 'predict_ygsmn', 'irrigation', 'HAVPN',
                     'previous_crop_corn', 'ap_data_sector', 'analysis_year', 'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90',
                     '10km_distance', '10km_p_value', '10km_outlier_ind', 'env_input_distance', 'env_input_p_value',
                     'env_input_outlier_ind']]
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                df_out['maturity'] = df_out['Market']
                df_out = df_out[
                    ['place_id', 'be_bid', 'stage', 'maturity', 'year', 'cperf', 'predict_ygsmn', 'irrigation', 'HAVPN',
                     'previous_crop_corn', 'ap_data_sector', 'analysis_year', 'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90',
                     '10km_distance', '10km_p_value', '10km_outlier_ind', 'env_input_distance', 'env_input_p_value',
                     'env_input_outlier_ind']]
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                df_out['maturity'] = df_out['Market']
                df_out = df_out[
                    ['place_id', 'be_bid', 'stage', 'maturity', 'year', 'cperf', 'predict_ygsmn', 'irrigation', 'HAVPN',
                     'previous_crop_corn', 'ap_data_sector', 'analysis_year', 'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90',
                     '10km_distance', '10km_p_value', '10km_outlier_ind', 'env_input_distance', 'env_input_p_value',
                     'env_input_outlier_ind']]

            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                df_out = df_out[
                    ['place_id', 'be_bid', 'stage', 'maturity', 'year', 'cperf', 'predict_ygsmn', 'irrigation', 'HAVPN',
                     'previous_crop_soy', 'ap_data_sector', 'analysis_year', 'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90',
                     '10km_distance', '10km_p_value', '10km_outlier_ind', 'env_input_distance', 'env_input_p_value',
                     'env_input_outlier_ind']]
            if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
                df_out['maturity'] = df_out['Market']
                df_out = df_out[
                    ['place_id', 'be_bid', 'stage', 'maturity', 'year', 'cperf', 'predict_ygsmn', 'irrigation', 'HAVPN',
                     'previous_crop_corn', 'ap_data_sector', 'analysis_year', 'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90',
                     '10km_distance', '10km_p_value', '10km_outlier_ind', 'env_input_distance', 'env_input_p_value',
                     'env_input_outlier_ind']]

            # append index
            df_out = df_out.reset_index(drop=True).reset_index()
            df_out["index"] = df_out["index"] + idx_position_counter
            idx_position_counter = df_out["index"].max() + 1
            print(idx_position_counter)

            # rename columns
            df_out = df_out.rename(columns={'be_bid': 'entry_id', 'HAVPN': 'density', 'index': 'id'})
            print("output df shape: ", df_out.shape, "  output df analysis year: ",
                  df_out.analysis_year.unique().tolist())

            # If you are writing to a S3 file, you will need to initialize the schema.
            # if (i == 0):
            #    model_prediction_input.write_schema_from_dataframe(df_out)
            print('i: ', i)
            # writer.write_dataframe(df_out)
            if i == 0:
                print('initial loop running')
                df_out.to_parquet(grid_performance_data_path, engine='fastparquet')

            else:  # Write recipe outputs
                df_out.to_parquet(grid_performance_data_path, engine='fastparquet', append=True)

            if i == rangeList[-3]:
                df_out.to_parquet(grid_performance_last_three_years_data_path, engine='fastparquet')
                print('output of three years prior')
            if i == rangeList[-2]:
                df_out.to_parquet(grid_performance_last_three_years_data_path, engine='fastparquet', append=True)
                print('output of two years prior')
            if i == rangeList[-1]:
                df_out.to_parquet(grid_performance_last_three_years_data_path, engine='fastparquet', append=True)
                print('output of prior year')

            df_out = ()

        print('Loop done')

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement grid_performance error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm folder', required=True)
    parser.add_argument('--s3_input_grid_classification_folder', type=str,
                        help='s3 input grid classification folder', required=True)
    parser.add_argument('--s3_input_hetpool1_pca_folder', type=str,
                        help='s3 input hetpool1 pca folder', required=True)
    parser.add_argument('--s3_input_hetpool2_pca_folder', type=str,
                        help='s3 input hetpool1 pca folder', required=True)
    parser.add_argument('--s3_input_test_data_folder', type=str,
                        help='s3 input test data folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            maturity_list = ['MG0', 'MG1', 'MG2', 'MG3', 'MG4', 'MG5', 'MG6', 'MG7', 'MG8']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            maturity_list = ['6C', '6L', '6M', '7C', '7L', '7M', '8C', '5L', '8M', '5M', '5C']  # , '4L']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or \
                DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            asec_df = pd.read_parquet(
                os.path.join(args.s3_input_grid_classification_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                             'asec_df_subset.parquet'))
            maturity_list = asec_df.decision_group_rm.dropna().unique().astype(int).astype(str).tolist()
            print('maturity_list: ', maturity_list)

        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7']
        # if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
        #     maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8']

        for input_year in years:
            for maturity in maturity_list:
                file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_grid_performance/data/grid_performance',
                                         DKU_DST_ap_data_sector, input_year, maturity, 'grid_performance.parquet')

                # check_file_exists = check_if_file_exists_s3(file_path)
                # if check_file_exists is False:
                print('Creating file in the following location: ', file_path)
                grid_performance_function(DKU_DST_ap_data_sector, input_year, maturity, pipeline_runid, args=args,
                                          logger=logger)
                print('File created')
                #    print()

                # else:
                #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
