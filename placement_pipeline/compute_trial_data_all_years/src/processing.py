import json
import os

import boto3
import pandas as pd

from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        try:
            s3 = boto3.resource('s3')
            # bucket_name = 'us.com.syngenta.ap.nonprod'
            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)
            all_df = pd.DataFrame()
            for obj in bucket.objects.filter(Prefix=os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_trial_data_by_year/data/trial_data_by_year', DKU_DST_ap_data_sector, '')):
                print(obj.key)
                df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                print('df shape: ', df.shape)
                all_df = pd.concat([all_df, df], ignore_index=True)
                print('all_df shape: ', all_df.shape)

            print('years', all_df['year'].unique().tolist())
            all_df['year'] = all_df['year'].astype('int32')

            print('years after convert to int', all_df['year'].unique().tolist())

            # trial_data_path_all = os.path.join('/opt/ml/processing/data/trial_data_all_years', DKU_DST_ap_data_sector, 'trial_data_all_years.parquet')
            # all_df.to_parquet(trial_data_path_all)

            # Write recipe outputs
            trial_data_dir_path = os.path.join('/opt/ml/processing/data/trial_data_all_years', DKU_DST_ap_data_sector)

            trial_data_data_path = os.path.join(trial_data_dir_path, 'trial_data_all_years.parquet')
            print('trial_data_data_path: ', trial_data_data_path)
            isExist = os.path.exists(trial_data_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(trial_data_dir_path)

            all_df.to_parquet(trial_data_data_path)

        except Exception as e:
            logger.error(e)
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            message = f'Placement trial_data_all_years error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
            print('message: ', message)
            teams_notification(message, None, pipeline_runid)
            print('teams message sent')
            raise e


if __name__ == '__main__':
    main()
