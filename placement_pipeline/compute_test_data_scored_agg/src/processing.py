import argparse
import json
import os

from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.compute_aggregate import *
from libs.event_bridge.event import error_event
from libs.placement_lib.lgbm_utils import folder_files, find_latest_model
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix
from libs.config.config_vars import ENVIRONMENT


def test_data_scored_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args, logger):
    try:
        # Read recipe inputs
        files_in_folder = folder_files(
            os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm',
                         DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, ''))
        print('files_in_folder: ', files_in_folder)
        latest_model, m_date, m_time = find_latest_model(files_in_folder, False)

        file_n = "params_opt_" + m_date + "-" + m_time + ".csv"
        train_params = pd.read_csv(os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, file_n))

        test_data_scored_df = pd.read_parquet(os.path.join(args.s3_input_test_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year, 'test_data_lgbm.parquet'))

        # Call the calculate_aggregated_metrics function from the compute_aggregate library
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            test_data_scored_agg_df = aggregate_metrics_soy(test_data_scored_df, DKU_DST_ap_data_sector)
        else:
            test_data_scored_agg_df = aggregate_metrics(test_data_scored_df, DKU_DST_ap_data_sector)
        # Write recipe outputs
        # train_data_scored_agg_data_path = os.path.join('/opt/ml/processing/data', 'train_data_scored_agg.parquet')
        # train_data_scored_agg_df.to_parquet(train_data_scored_agg_data_path)

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            vals = 'maturity_group'
            test_data_scored_agg_df['maturity_group'] = test_data_scored_agg_df['maturity_group'].astype(str)
        elif DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            vals = 'maturity_zone'
            test_data_scored_agg_df['maturity_zone'] = test_data_scored_agg_df['maturity_zone'].astype(str)
        elif DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            vals = 'et'
            test_data_scored_agg_df['et'] = test_data_scored_agg_df['et'].astype(str)
        else:
            vals = 'tpp_region'
        test_data_scored_agg_df['trial_stage'] = test_data_scored_agg_df['trial_stage'].astype(str)
        test_data_scored_agg_df['ap_data_sector'] = DKU_DST_ap_data_sector

        print('test_data_scored_agg_df.dtypes: ', test_data_scored_agg_df.dtypes)

        test_data_scored_agg_dir_path = os.path.join('/opt/ml/processing/data/test_data_scored_agg', DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid)
        test_data_scored_agg_data_path = os.path.join(test_data_scored_agg_dir_path, 'test_data_scored_agg.parquet')
        print('test_data_lgbm_importance_data_path: ', test_data_scored_agg_data_path)
        isExist = os.path.exists(test_data_scored_agg_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(test_data_scored_agg_dir_path)

        # Write recipe outputs
        test_data_scored_agg_df.to_parquet(test_data_scored_agg_data_path)

        l = [latest_model, pipeline_runid]
        df1 = pd.DataFrame(l).transpose()
        df1.columns = ['model_name', 'pipeline_runid']

        test_data_scored_agg_df_sub = test_data_scored_agg_df[(test_data_scored_agg_df[vals] == 'all') & (test_data_scored_agg_df.par_geno == 'all') & (test_data_scored_agg_df.trial_stage == 'all')]
        test_data_scored_agg_df_sub_with_params = pd.concat([test_data_scored_agg_df_sub.reset_index(drop=True), train_params.reset_index(drop=True), df1.reset_index(drop=True)], axis=1)
        test_data_scored_agg_df_sub_with_params_data_path = os.path.join(test_data_scored_agg_dir_path, 'test_data_scored_agg_with_params.parquet')
        test_data_scored_agg_df_sub_with_params.to_parquet(test_data_scored_agg_df_sub_with_params_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement test_data_scored_agg error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_test_data_lgbm_folder', type=str,
                        help='s3 input test data lgbm model folder', required=True)
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm model folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        # years = ['2023']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_test_data_scored_agg/data/test_data_scored_agg', DKU_DST_ap_data_sector, input_year, 'test_data_scored_agg.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:

            print('Creating file in the following location: ', file_path)
            test_data_scored_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args, logger=logger)
            print('File created')
            print()
            # else:
            # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
