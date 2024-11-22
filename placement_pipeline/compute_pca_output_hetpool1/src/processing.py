import json
import os
import argparse
from libs.event_bridge.event import error_event
from libs.placement_lib.method_vs_grm_pca import compute_ipca

from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix
from libs.config.config_vars import ENVIRONMENT
"""
compute_pca_output_hetpool1

Performs iPCA on the sample id GRM for a set of training years and then applies this iPCA on the training+test year.
NOTE that the analysis_year = test year, and is used in the Inputs/Outputs tab partition logic to limit years of data pulled into this recipe.

Steps performed:
1. Get all years of data in hetpool1_by_year up to and including analysis_year
2. Fit Incremental PCA model on training years
3. Apply Incremental PCA model on training and test years to generate #n_comp components
4. Save results

"""


def pca_output_hetpool1_function(DKU_DST_ap_data_sector, analysis_year_param, pipeline_runid, hetpool1_by_year_path):
    try:
        n_comp = 20
        chunk_size = 3000
        # must change to take in s3_parquet_path
        if DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            label_names = 'be_bid'
        else:
            label_names = 'sample_id'

        hetpool1_pca_dir_path = os.path.join('/opt/ml/processing/data/pca_output_hetpool1', DKU_DST_ap_data_sector, analysis_year_param)

        # Write recipe outputs
        hetpool1_pca_data_path = os.path.join(hetpool1_pca_dir_path, 'hetpool1_pca.parquet')
        print('hetpool1_pca_data_path: ', hetpool1_pca_data_path)
        isExist = os.path.exists(hetpool1_pca_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(hetpool1_pca_dir_path)

        pca_df = compute_ipca(hetpool1_by_year_path, analysis_year_param, n_comp, chunk_size, label_name=label_names, dir_path=hetpool1_pca_dir_path)

        pca_df.to_parquet(hetpool1_pca_data_path)

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, analysis_year_param, pipeline_runid, str(e))
        raise e


# Read recipe inputs
def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_hetpool1_folder', type=str,
                        help='s3 input hetpool1 folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        hetpool1_by_year_path = os.path.join(args.s3_input_hetpool1_folder, DKU_DST_ap_data_sector, 'hetpool1_all_years.parquet')
        print('hetpool1_by_year_path with args: ', hetpool1_by_year_path)

        # years = ['2020', '2021', '2022']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_pca_output_hetpool1/data/pca_output_hetpool1', DKU_DST_ap_data_sector, input_year, 'hetpool1_pca.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            pca_output_hetpool1_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, hetpool1_by_year_path)
            print('File created')
            print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
