import argparse
import json
import os

import pandas as pd

from libs.event_bridge.event import error_event
from libs.placement_lib.method_vs_grm_pca import compute_ipca
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix
from libs.config.config_vars import ENVIRONMENT
"""
compute_pca_output_hetpool2

Performs iPCA on the sample id GRM for a set of training years and then applies this iPCA on the training+test year.
NOTE that the analysis_year = test year, and is used in the Inputs/Outputs tab partition logic to limit years of data pulled into this recipe.

Steps performed:
1. Get all years of data in hetpool2_by_year up to and including analysis_year
2. Fit Incremental PCA model on training years
3. Apply Incremental PCA model on training and test years to generate #n_comp components
4. Save results

"""


def pca_output_hetpool2_function(DKU_DST_ap_data_sector, analysis_year_param, pipeline_runid, hetpool2_by_year_path):
    try:
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            # Write recipe outputs
            hetpool2_pca_dir_path = os.path.join('/opt/ml/processing/data/pca_output_hetpool2', DKU_DST_ap_data_sector, analysis_year_param)

            hetpool2_pca_data_path = os.path.join(hetpool2_pca_dir_path, 'hetpool2_pca.parquet')
            print('hetpool2_pca_data_path: ', hetpool2_pca_data_path)
            isExist = os.path.exists(hetpool2_pca_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool2_pca_dir_path)

            pca_df = pd.DataFrame()
            pca_df.to_parquet(hetpool2_pca_data_path)

        else:
            n_comp = 20
            chunk_size = 3000
            # must change to take in s3_parquet_path

            if DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                label_names = 'be_bid'
            else:
                label_names = 'sample_id'

            hetpool2_pca_dir_path = os.path.join('/opt/ml/processing/data/pca_output_hetpool2', DKU_DST_ap_data_sector, analysis_year_param)

            # Write recipe outputs
            hetpool2_pca_data_path = os.path.join(hetpool2_pca_dir_path, 'hetpool2_pca.parquet')
            print('hetpool2_pca_data_path: ', hetpool2_pca_data_path)
            isExist = os.path.exists(hetpool2_pca_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool2_pca_dir_path)

            pca_df = compute_ipca(hetpool2_by_year_path, analysis_year_param, n_comp, chunk_size, label_name=label_names, dir_path=hetpool2_pca_dir_path)

            pca_df.to_parquet(hetpool2_pca_data_path)

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, analysis_year_param, pipeline_runid, str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_hetpool2_folder', type=str,
                        help='s3 input hetpool2 folder', required=False)
    args = parser.parse_args()
    print('args collected')
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        hetpool2_by_year_path = os.path.join(args.s3_input_hetpool2_folder, DKU_DST_ap_data_sector, 'hetpool2_all_years.parquet')

        # years = ['2020', '2021', '2022']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_pca_output_hetpool2/data/pca_output_hetpool2', DKU_DST_ap_data_sector, input_year, 'hetpool2_by_year.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            pca_output_hetpool2_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, hetpool2_by_year_path)
            print('File created')
            print()

            # else:
            #     print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
