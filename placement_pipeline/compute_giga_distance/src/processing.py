import json
import os

import boto3
import pandas as pd

from libs.config.config_vars import ENVIRONMENT, CONFIG, S3_BUCKET
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.geno_queries import giga_sunflower
from libs.placement_s3_functions import get_s3_prefix


def giga_distance_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, logger, extra_args):
    try:

        giga_distance_dir_path_runid = os.path.join('/opt/ml/processing/data/giga_distance', DKU_DST_ap_data_sector,
                                                    pipeline_runid)
        s3_giga_distance_dir_path_runid = os.path.join(get_s3_prefix(ENVIRONMENT),
                                                       'compute_giga_distance/data/giga_distance',
                                                       DKU_DST_ap_data_sector, pipeline_runid)

        isExist = os.path.exists(giga_distance_dir_path_runid)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(giga_distance_dir_path_runid)

        giga_distance_fpath = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_giga_distance/data/giga_distance',
                                           DKU_DST_ap_data_sector, pipeline_runid)
        print('giga_distance_fpath: ', giga_distance_fpath)
        s3 = boto3.client('s3')
        bucket = S3_BUCKET

        def upload_to_s3(fname):
            s3.upload_file(os.path.join(giga_distance_dir_path_runid, fname),
                           Bucket=bucket,
                           Key=os.path.join(s3_giga_distance_dir_path_runid, fname),
                           ExtraArgs=extra_args)
            print("file written directly to s3")

        if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
            giga_distance_df = giga_sunflower(10)

        else:
            logger.info(f'Please define correct ap data sector')

        giga_distance_dir_path = os.path.join('/opt/ml/processing/data/giga_distance', DKU_DST_ap_data_sector)
        isExist = os.path.exists(giga_distance_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(giga_distance_dir_path)

        # Write recipe outputs
        giga_distance_data_path = os.path.join(giga_distance_dir_path, 'giga_distance.parquet')
        giga_distance_df.to_parquet(giga_distance_data_path)


    except Exception as e:
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        raise e


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        pipeline_runid = data['target_pipeline_runid']

        output_kms_key = CONFIG.get('output_kms_key')
        extra_args = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': output_kms_key}
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        logger.info(f'getting comp set for: {DKU_DST_ap_data_sector}')

        file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_giga_distance/data/giga_distance',
                                 DKU_DST_ap_data_sector, 'giga_distance.parquet')

        # check_file_exists = check_if_file_exists_s3(file_path)
        # if check_file_exists is False:
        logger.info(f'Creating file in the following location: {file_path}')
        # print('Creating file in the following location: ', file_path)
        giga_distance_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, logger=logger,
                               extra_args=extra_args)
        logger.info(f'File created')
        # print()
        # else:
        # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
