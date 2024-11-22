import argparse
import json
import os
import boto3
import time

from libs.config.config_vars import CONFIG, S3_DATA_PREFIX
from libs.event_bridge.event import error_event, create_event

bucket = CONFIG['bucket']
s3 = boto3.client('s3')


def find_prefix(prefixes, target):
    for prefix in prefixes:
        if target in prefix['Prefix']:
            return True
        if 'CommonPrefixes' in prefix:
            result = find_prefix(prefix['CommonPrefixes'], target)
            if result:
                return True
    return False


def folder_check(ap_data_sector,
                 write_output=1):
    # check if folder exists in s3
    s3_loc = f'{S3_DATA_PREFIX}/{ap_data_sector}/'
    s3_folder = f'{s3_loc}last_update/'

    # check if any obj were found
    response = s3.list_objects(Bucket=bucket, Prefix=s3_loc, Delimiter='/')
    out_last_update = '/opt/ml/processing/data/last_update/'
    if not os.path.exists(out_last_update):
        os.makedirs(out_last_update)
    lcl_out_fpath = os.path.join(out_last_update, 'last_update.json')

    if not find_prefix(response.get('CommonPrefixes', []), s3_folder):
        print('Creating last_update folder and adding empty json.')
        try:
            new_data_dict = {"row_count": '0'}
            if write_output == 1:
                with open(lcl_out_fpath, 'w') as outfile:
                    json.dump(new_data_dict, outfile)
        except:
            pass
            return False
    else:
        pass
        return True


def main():
    parser = argparse.ArgumentParser(description='sql variables')
    parser.add_argument('--ap_data_sector', help='Input String ', required=True)
    parser.add_argument('--analysis_type', help='Input String ', required=True)
    parser.add_argument('--input_years', help='Input String', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)
    parser.add_argument('--is_inferring', help='Input String', required=True)

    args = parser.parse_args()
    try:
        create_event(CONFIG.get('event_bus'), args.ap_data_sector, args.input_years, args.target_pipeline_runid,
                     'performance', 'null', 'START',
                     f'Start Sagemaker DME Performance pipeline: {args.target_pipeline_runid}')

        query_vars_dict = {
            'ap_data_sector': args.ap_data_sector,
            'analysis_type': args.analysis_type,
            'input_years': args.input_years.split(','),
            'target_pipeline_runid': args.target_pipeline_runid,
            'force_refresh': args.force_refresh,
            'is_inferring': args.is_inferring == '1',
            'analysis_run_group': 'performance'  # to get logger to work
        }

        # for some data sectors, run by season instead of all. POs and other systems combine the seasons
        # into one data sector, but the recommender pipeline treats each season separately. Kick off
        # pipeline runs for each season separately if listed below. Otherwise, continue normally
        data_sector_list = []

        do_split = False
        if args.ap_data_sector == 'CORN_BANGLADESH_ALL':
            data_sector_list = ['CORN_BANGLADESH_WET','CORN_BANGLADESH_DRY']
        elif args.ap_data_sector == 'CORN_THAILAND_ALL':
            data_sector_list = ['CORN_THAILAND_WET','CORN_THAILAND_DRY']
        elif args.ap_data_sector == 'CORN_INDONESIA_ALL':
            data_sector_list = ['CORN_INDONESIA_WET','CORN_INDONESIA_DRY']
        elif args.ap_data_sector == 'CORN_PHILIPPINES_ALL':
            data_sector_list = ['CORN_PHILIPPINES_WET','CORN_PHILIPPINES_DRY']
        elif args.ap_data_sector == 'CORN_PAKISTAN_ALL':
            data_sector_list = ['CORN_PAKISTAN_SPRING', 'CORN_PAKISTAN_AUTUMN']
        elif args.ap_data_sector == 'CORN_VIETNAM_ALL':
            data_sector_list = ['CORN_VIETNAM_WET','CORN_VIETNAM_DRY']
        # NOTE: corn india IS split into different data sectors naturally. It is included here for consistency
        elif args.ap_data_sector == 'CORN_INDIA_ALL':
            data_sector_list = ['CORN_INDIA_SPRING','CORN_INDIA_DRY','CORN_INDIA_WET']

        if len(data_sector_list) > 0: # if we are splitting pipeline runs
            do_split = True
            for data_sector in data_sector_list:
                sagemaker_client = boto3.client('sagemaker', region_name='us-east-1')
                pipeline_name = CONFIG['performance_ingestion_arn']
                execution_name = f'{data_sector}-{args.target_pipeline_runid}'.replace('_', '-')

                pipeline_parameters = {
                    'ap_data_sector': data_sector,
                    'analysis_type': args.analysis_type,
                    'input_years': args.input_years,
                    'target_pipeline_runid': args.target_pipeline_runid,
                    'force_refresh': args.force_refresh,
                    'is_inferring':args.is_inferring
                }
                response = sagemaker_client.start_pipeline_execution(
                    PipelineExecutionDescription=f'Sagemaker Performance pipeline',
                    PipelineExecutionDisplayName=execution_name,
                    PipelineName=pipeline_name,
                    PipelineParameters=[{'Name': k, 'Value': v} for k, v in pipeline_parameters.items()])

                # wait 30s to give some space between pipelines
                time.sleep(30)

        # output split json
        split_dict = {
            'analysis_split': do_split
        }
        out_dir_split = '/opt/ml/processing/data/ingestion/'
        if not os.path.exists(out_dir_split):
            os.makedirs(out_dir_split)

        with open(os.path.join(out_dir_split, 'do_split.json'), 'w') as outfile:
            json.dump(split_dict, outfile)

        # output query vars
        out_dir = '/opt/ml/processing/data/ingestion/{}/init_script_ingestion/'.format(args.target_pipeline_runid)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        with open(os.path.join(out_dir,'query_variables.json'), 'w') as outfile:
            json.dump(query_vars_dict, outfile)

        # check if last_update folder exists
        folder_exists = folder_check(ap_data_sector=args.ap_data_sector)

    except Exception as e:
        error_event(args.ap_data_sector, 0, args.target_pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
