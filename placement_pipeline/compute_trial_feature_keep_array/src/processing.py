import json
import os
import pandas as pd
import numpy as np
from libs.event_bridge.event import error_event
import boto3

from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        try:

            s3 = boto3.resource('s3')
            # bucket_name = 'us.com.syngenta.ap.nonprod'
            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)

            full_df = pd.DataFrame()
            for obj in bucket.objects.filter(Prefix=os.path.join(get_s3_prefix(ENVIRONMENT),
                                                                 'compute_cropFact_api_output_by_year/data/cropFact_api_output_by_year',
                                                                 DKU_DST_ap_data_sector, '')):
                print(obj.key)
                df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                print('df shape: ', df.shape)
                full_df = pd.concat([full_df, df], ignore_index=True)
                print('full df shape: ', full_df.shape)

            print('years', full_df['year'].unique().tolist())
            full_df.head()

            # cropFact_api_output_all_years_data_path = os.path.join('/opt/ml/processing/data/cropFact_api_output_all_years', DKU_DST_ap_data_sector, 'cropFact_api_output_all_years.parquet')
            # full_df.to_parquet(cropFact_api_output_all_years_data_path)

            # Write recipe outputs
            cropFact_api_output_all_years_dir_path = os.path.join(
                '/opt/ml/processing/data/cropFact_api_output_all_years', DKU_DST_ap_data_sector)

            cropFact_api_output_all_years_data_path = os.path.join(cropFact_api_output_all_years_dir_path,
                                                                   'cropFact_api_output_all_years.parquet')
            print('cropFact_api_output_all_years_data_path: ', cropFact_api_output_all_years_data_path)
            isExist = os.path.exists(cropFact_api_output_all_years_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(cropFact_api_output_all_years_dir_path)

            full_df.to_parquet(cropFact_api_output_all_years_data_path)

            # cropFact_api_output_df = pd.read_parquet("/opt/ml/processing/input/data")
            cropFact_api_output_df = full_df
            # Due to Dataiku schema consistency requirements across schemas, perform this analysis assuming analysis_year = 2022
            cropFact_api_output_df = cropFact_api_output_df.loc[cropFact_api_output_df["year"] < 2022, :].set_index(
                'cropfact_id').drop(['ap_data_sector', 'year'], axis=1).drop_duplicates()
            print(cropFact_api_output_df.shape)
            if DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                cropFact_api_output_df = cropFact_api_output_df.dropna(how='all').drop(
                    columns=["wholeplantdrymattercontent", "wholeplantyieldsolarpotential",
                             "wholeplantyieldpotential",
                             "wholeplantyieldattainable", "wholeplantyield"])

            # Compute recipe outputs from inputs
            cropFact_api_output_arr = cropFact_api_output_df.to_numpy()
            train_corr = np.corrcoef(cropFact_api_output_arr.T)

            # Setup keep_arr to track which variables have not been eliminated
            keep_arr = (np.nanvar(cropFact_api_output_arr, axis=0) > 0)
            keep_arr[:6] = 0

            print("Initial number of features: " + str(np.sum(keep_arr)))

            if DKU_DST_ap_data_sector != 'CORNSILAGE_EAME_SUMMER' and DKU_DST_ap_data_sector != 'CORNGRAIN_EAME_SUMMER':
                # Feature reduction based on correlation and correlation with squared features
                for i in range(1, np.shape(train_corr)[0]):
                    keep_arr[i] = (np.all(np.square(train_corr[:i, i].T * keep_arr[:i]) < .92) &
                                   np.all(np.square(np.corrcoef(cropFact_api_output_arr[:, i],
                                                                np.square(cropFact_api_output_arr[:, :i]),
                                                                rowvar=False)[0][1:] * keep_arr[:i]) < .92) &
                                   np.all(np.square(np.corrcoef(np.square(cropFact_api_output_arr[:, i]),
                                                                cropFact_api_output_arr[:, :i], rowvar=False)[0][
                                                    1:] * keep_arr[:i]) < .92) &
                                   (keep_arr[i] == 1))

            print("Final number of features: " + str(np.sum(keep_arr)))

            # Cast into df
            trial_feature_keep_array_df = pd.DataFrame(data=np.array([keep_arr]),
                                                       columns=cropFact_api_output_df.columns)

            print('trial_feature_keep_array_df shape: ', trial_feature_keep_array_df.shape)

            # Write recipe outputs
            # trial_feature_keep_array_data_path = os.path.join('/opt/ml/processing/data/trial_feature_keep_array', DKU_DST_ap_data_sector, 'trial_feature_keep_array.parquet')
            # trial_feature_keep_array_df.to_parquet(trial_feature_keep_array_data_path)

            # Write recipe outputs
            trial_feature_keep_array_dir_path = os.path.join('/opt/ml/processing/data/trial_feature_keep_array',
                                                             DKU_DST_ap_data_sector)

            trial_feature_keep_array_data_path = os.path.join(trial_feature_keep_array_dir_path,
                                                              'trial_feature_keep_array.parquet')
            print('trial_feature_keep_array_data_path: ', trial_feature_keep_array_data_path)
            isExist = os.path.exists(trial_feature_keep_array_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(trial_feature_keep_array_dir_path)

            trial_feature_keep_array_df.to_parquet(trial_feature_keep_array_data_path)

        except Exception as e:
            logger.error(e)
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            message = f'Placement trial_feature_keep_array error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
            print('message: ', message)
            teams_notification(message, None, pipeline_runid)
            print('teams message sent')
            raise e


if __name__ == '__main__':
    main()
