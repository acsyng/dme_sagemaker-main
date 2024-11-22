# import packages
import os
import json
import boto3

from libs.config.config_vars import S3_DATA_PREFIX
from libs.snowflake.snowflake_connection import SnowflakeConnection


bucket = CONFIG['bucket']  # Replace with your s3 bucket name
s3 = boto3.client('s3')
print('Starting to check data for ingestion.')


def find_prefix(prefixes, target):
    for prefix in prefixes:
        if target in prefix['Prefix']:
            return True
        if 'CommonPrefixes' in prefix:
            result = find_prefix(prefix['CommonPrefixes'], target)
            if result:
                return True
    return False


def chk_data_for_ingestion(ap_data_sector,
                           analysis_type,
                           input_years,
                           is_inferring,
                           write_output=1):
    if is_inferring == 0:
        print('Go for training data ingestion.')
        return True

    else:
        # check if folder exists in s3
        # folder_loc = f'{S3_DATA_PREFIX}/{ap_data_sector}/'
        # update_folder = f'{folder_loc}last_update/'
        lcl_folder_path = 'opt/ml/processing/data/data_check/last_update/'
        if not os.path.exists(lcl_folder_path):
            os.makedirs(lcl_folder_path)
        lcl_fpath = os.path.join(lcl_folder_path, 'last_update.json')
        # check if any obj were found
        # response = s3.list_objects(Bucket=bucket, Prefix=folder_loc, Delimiter='/')
        # print('---->', response)
        for analysis_year in input_years:
            # if find_prefix(response.get('CommonPrefixes', []), update_folder):
            #     print('-----> "last_update" exists.')
            try:
                with open(lcl_fpath, 'r') as f:
                    data = json.load(f)

                    row_count = int(data.get('row_count'))
                    # row_count = int("3720000")
                    # print('-----> row_count changed from "3725198" to "3720000"')

                    print('-----> Starting denodo connection')
                    with SnowflakeConnection() as dc:
                        new_data = dc.get_data("""
                            SELECT count(*) as row_count
                            FROM rv_trial_pheno_analytic_dataset
                            WHERE ap_data_sector = {0}
                            AND EXTRACT(YEAR FROM pr_last_chg_date) = {1}
                            """.format("'" + ap_data_sector + "'", analysis_year))

                        analysis_rows = new_data["row_count"].iloc[0]

                    if analysis_rows <= row_count:
                        print('--^^---> No new data found for:', analysis_year)
                        return False

                    else:  # if new data exists then run ingestion
                        print('--^^---> New data found for: ', analysis_year, ' Proceeding to ingestion')

                        # save the new_data as a json file in s3 and run ingestion functions as it is
                        new_data_dict = {"row_count": str(new_data["row_count"].iloc[0])}
                        if write_output == 1:
                            with open(lcl_fpath, 'w') as a:
                                json.dump(new_data_dict, a)

                        print('--^^---> new json saved and setting ingestion for inference1')
                        return True

            except Exception as e:
                print('Error:', e)

            # else:  # if the folder does not exist in s3, then we'll create a new dir in s3
            #     print('----->Creating "'"last_update"'" folder and saving last_update.json.')
            #
            #     with DenodoConnection() as dc:
            #         new_data = dc.get_data("""
            #                     SELECT count(*) as row_count
            #                     FROM "managed"."rv_trial_pheno_analytic_dataset"
            #                     WHERE "ap_data_sector" = {0}
            #                     AND EXTRACT(YEAR FROM pr_last_chg_date) = {1}
            #                     """.format("'" + ap_data_sector + "'", analysis_year))
            #
            #         new_data_dict = {"row_count": str(new_data["row_count"].iloc[0])}
            #         os.makedirs('/opt/ml/processing/data/data_check/last_update', exist_ok=True)
            #
            #         if write_output == 1:
            #             with open(lcl_fpath, 'w') as outfile:
            #                 json.dump(new_data_dict, outfile)
            #
            #         print('--^^---> Setting ingestion for inference2')
            #         return True
