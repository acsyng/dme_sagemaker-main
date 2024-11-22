import numpy as np

import json
import os

import pandas as pd
from libs.placement_lib.geno_queries import get_snp_data_batch, get_variant_set
from libs.placement_lib.method_vs_grm_pca import compute_grm_parallel
from libs.event_bridge.event import error_event
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix
from libs.config.config_vars import ENVIRONMENT

"""
compute_sample_comps_for_grm

This recipe extracts the SNP information for the comparison set of samples listed in "sample_topn" 
for use in GRM calculation later. The usage of a comparison set is to avoid calculating an n-by-n 
(where n is very large) GRM, which would take a lot of time.

The steps in this script are:
1. Read Dataiku inputs
2. Get rv_variant_set, as it is on Athena (slow)
3. Get comparison set SNP data
4. Filter comparison set SNPs to make sure each sample-variant_id combi is unique, and keep SNPs 
    that are in at least 1/5 of the samples. Due to the usage of np.uint16, we can only use ~64000 SNPs max.
5. Save the counts of each variant ID and sample ID in each set for downstream usage or troubleshooting.
6. Compute GRM
7. Remove highly correlated samples to improve speed and component diversity of PCA
"""


def sample_comps_for_grm_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid):
    try:

        batch_size = 500

        if DKU_DST_ap_data_sector == "CORN_NA_SUMMER" or DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or DKU_DST_ap_data_sector == "CORNGRAIN_EAME_SUMMER" or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            num_crop_id = 9
            variant_threshold = 999

        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == "SOY_NA_SUMMER":
            # Compute recipe outputs
            num_crop_id = 7
            variant_threshold = 300

        if DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER":
            # Compute recipe outputs
            num_crop_id = 12
            variant_threshold = 300

        variant_set_df = get_variant_set(num_crop_id)
        print('variant_set_df shape: ', variant_set_df.shape)

        comps_for_grm_df = pd.read_parquet(os.path.join('/opt/ml/processing/input/data', DKU_DST_ap_data_sector, 'sample_comp_set.parquet'))
        print('comps_for_grm_df shape: ', comps_for_grm_df.shape)

        comps_for_grm_df = get_snp_data_batch(comps_for_grm_df, variant_set_df, batch_size, num_crop_id)
        print('comps_for_grm_df shape after get_snp_data_batch: ', comps_for_grm_df.shape)
        # Compute recipe outputs
        # Keep SNPs/variants that show up in at least ~1/5 (whatever is set by variant_threshold) of the samples
        variant_count_df = comps_for_grm_df.groupby(level=['variant_id'])['geno_index'].count().reset_index()
        variant_count_df = variant_count_df.loc[variant_count_df.geno_index > variant_threshold, :]

        sample_comps_for_grm_dir_path = os.path.join('/opt/ml/processing/data/sample_comps_for_grm', DKU_DST_ap_data_sector)
        isExist = os.path.exists(sample_comps_for_grm_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(sample_comps_for_grm_dir_path)

        data_path_variant_id_count = os.path.join(sample_comps_for_grm_dir_path, 'variant_id_count.parquet')
        variant_count_df.to_parquet(data_path_variant_id_count)

        comps_for_grm_df = comps_for_grm_df.loc[comps_for_grm_df.groupby(level=['variant_id'])['geno_index'].transform(
            'count') > variant_threshold, :]
        sample_id_count_df = comps_for_grm_df.groupby(level=['sample_id'])['geno_index'].count().reset_index()

        data_path_sample_id_count = os.path.join(sample_comps_for_grm_dir_path, 'sample_id_count.parquet')
        sample_id_count_df.to_parquet(data_path_sample_id_count)

        print(comps_for_grm_df.head())
        comps_for_grm_df = comps_for_grm_df.unstack()
        comps_for_grm_df.columns = comps_for_grm_df.columns.droplevel(0)
        comps_for_grm_df = comps_for_grm_df.rename_axis(None, axis=1)
        print(comps_for_grm_df.iloc[:5, :5])

        # Reorder based on number of SNPs in each sample, descending
        comps_for_grm_df['sample_count'] = comps_for_grm_df.count(axis=1)
        comps_for_grm_df = comps_for_grm_df.sort_values(by=['sample_count'], ascending=False).drop(
            columns=['sample_count'])

        # Compute GRM to further reduce number of samples used as the standard comparative set
        sample_arr = comps_for_grm_df.index.values
        sample_comp_arr = comps_for_grm_df.to_numpy(dtype='int8')
        samp_arr_values = sample_comp_arr.copy()
        samp_arr_values[samp_arr_values == 0] = -15
        new_arr = sample_comp_arr
        new_arr[new_arr == 0] = 15
        sample_grm_df = compute_grm_parallel(samp_arr_values, new_arr, sample_arr, sample_arr)
        sample_grm_arr = sample_grm_df.iloc[:, 1:].to_numpy()

        # Filter final sample information to those that are not strongly correlated with others
        # Correlation-based feature reduction
        keep_arr = np.full(np.shape(sample_grm_arr)[0], 1)
        for i in range(1, np.shape(sample_grm_arr)[0]):
            keep_arr[i] = np.all((sample_grm_arr[:i, i].T * keep_arr[:i]) < 0.95) & (keep_arr[i] == 1)

        print(np.sum(keep_arr))

        comps_for_grm_df = comps_for_grm_df.loc[keep_arr == 1, :].reset_index()

        comps_for_grm_df.columns = comps_for_grm_df.columns.astype(str)
        print('column outputs before writing')
        print(comps_for_grm_df.columns)
        # Write recipe output
        data_path_sample_comps_for_grm = os.path.join(sample_comps_for_grm_dir_path, 'sample_comps_for_grm.parquet')
        comps_for_grm_df.to_parquet(data_path_sample_comps_for_grm)

        data_path_sample_comps_grm = os.path.join(sample_comps_for_grm_dir_path,  'sample_comps_grm.parquet')
        sample_grm_df.to_parquet(data_path_sample_comps_grm)

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        raise e


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        pipeline_runid = data['target_pipeline_runid']

        file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_sample_comps_for_grm/data/sample_comps_for_grm', DKU_DST_ap_data_sector, 'sample_comps_grm.parquet')

        # check_file_exists = check_if_file_exists_s3(file_path)
        # if check_file_exists is False:
        print('Creating file in the following location: ', file_path)
        sample_comps_for_grm_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid)
        print('File created')
        print()
        # else:
        #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
