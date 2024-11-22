# import packages
import os
import json
import boto3

from libs.event_bridge.event import error_event
from libs.config.config_vars import S3_DATA_PREFIX, CONFIG

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
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['input_years']
        pipeline_runid = data['target_pipeline_runid']
    try:
        folder_exists = folder_check(ap_data_sector=ap_data_sector)
        print('folder_exists: ', folder_exists)

    except Exception as e:
        error_event(ap_data_sector, input_years, pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
