import json
import os

import boto3
import pandas as pd

from libs.event_bridge.event import error_event
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET, S3_PREFIX_PLACEMENT


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        # hetpool2_by_year_path = "/opt/ml/processing/input/data/hetpool2_by_year.parquet"

        s3 = boto3.resource('s3')
        # bucket_name = 'us.com.syngenta.ap.nonprod'
        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)

        try:
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                # pass
                # Write recipe outputs
                hetpool2_all_years_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_all_years', DKU_DST_ap_data_sector)

                hetpool2_all_years_data_path = os.path.join(hetpool2_all_years_dir_path, 'hetpool2_all_years.parquet')
                isExist = os.path.exists(hetpool2_all_years_dir_path)
                if not isExist:
                    # Create a new directory because it does not exist
                    os.makedirs(hetpool2_all_years_dir_path)
                all_df = pd.DataFrame()
                all_df.to_parquet(hetpool2_all_years_data_path)
            else:
                all_df = pd.DataFrame()
                if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                    prefix_used = os.path.join(S3_PREFIX_PLACEMENT, 'compute_sample_geno_output_giga/data/hetpool2_df_by_year', DKU_DST_ap_data_sector, '')
                else:
                    prefix_used = os.path.join(S3_PREFIX_PLACEMENT, 'compute_sample_geno_output/data/hetpool2_by_year', DKU_DST_ap_data_sector, '')
                print('prefix used: ', prefix_used)
                for obj in bucket.objects.filter(Prefix=prefix_used):
                    print(obj.key)
                    if "giga" not in obj.key[-30:] and (DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA'):
                        next
                    else:
                        df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                        print('df shape: ', df.shape)
                        all_df = pd.concat([all_df, df], ignore_index=True)
                        print('all_df shape: ', all_df.shape)

                print('years', all_df['year'].unique().tolist())

                all_df['year'] = all_df['year'].astype('int32')
                print('years after convert to int', all_df['year'].unique().tolist())
                all_df = all_df.fillna(1)
                # Write recipe outputs
                hetpool2_all_years_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_all_years', DKU_DST_ap_data_sector)

                hetpool2_all_years_data_path = os.path.join(hetpool2_all_years_dir_path, 'hetpool2_all_years.parquet')
                isExist = os.path.exists(hetpool2_all_years_dir_path)
                if not isExist:
                    # Create a new directory because it does not exist
                    os.makedirs(hetpool2_all_years_dir_path)
                all_df.to_parquet(hetpool2_all_years_data_path)

        except Exception as e:
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
