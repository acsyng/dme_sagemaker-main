import json
import pandas as pd
import math as math
import os

from libs.placement_lib.comparison_set_queries import get_comparison_set_soy_year_bebid, \
    get_comparison_set_soy_subset_genotype_count, \
    get_comparison_set_soy_subset_sample_api, get_comparison_set_soy_subset_material_sdl
from libs.placement_lib.geno_queries import get_variant_set, get_het_pools, get_het_pools_sunflower_init, \
    get_het_pools_sunflower_donor, get_het_pools_sunflower_receiver
from libs.placement_lib.method_vs_grm_pca import get_snp_data_transform
from libs.event_bridge.event import error_event

from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT

"""
compute_sample_geno_output

This recipe determines what sample id's were first used in the specified year, fetches geno data for those samples,
and then computes the GRM vs the comparison set.

The steps in this script are:
1. Read Dataiku inputs and setup Denodo connection
2. Get comparison set SNP data stored in "sample_comps_for_grm" and variant_id's to use stored in "variant_id_count"
3. Get sample id's that were first used in the partition-specified year via geno_queries.get_het_pools()
4. Get rv_variant_set, as it is on Athena (slow), via geno_queries.get_variant_set(). This table maps marker_id to variant_id
5. Retrieve SNPs for het pool 1 samples, compute GRM, and write GRM
6. Retrieve SNPs for het pool 2 samples, compute GRM, and write GRM

Last updated: 2023 July 7, Keri Rehm
"""


def sample_geno_output_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, rec_df=None,
                                donor_df=None):
    try:
        # Read recipe inputs
        variant_id_df = pd.read_parquet(
            os.path.join('/opt/ml/processing/input/data', DKU_DST_ap_data_sector, 'variant_id_count.parquet'))
        print('variant_id_df shape: ', variant_id_df.shape)
        sample_comps_for_grm_df = pd.read_parquet(
            os.path.join('/opt/ml/processing/input/data', DKU_DST_ap_data_sector, 'sample_comps_for_grm.parquet'))
        print('sample_comps_for_grm_df shape: ', sample_comps_for_grm_df.shape)

        batch_size = 2000

        # Compute recipe outputs
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            val = 'NOAM'
            season = 'SUMR'
            print('val: ', val, ' season: ', season)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val, season)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)
        elif DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            val = 'LATAM'
            season = 'WNTR'
            print('val: ', val, ' season: ', season)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val, season)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)
        elif DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER':
            val = 'LATAM'
            season = 'SUMR'
            print('val: ', val, ' season: ', season)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val, season)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)
        elif DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            val = 'EAME'
            print('val: ', val)
            season = 'SUMR'
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val, season)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)
        elif DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            val = 'LATAM'
            print('val: ', val)
            num_crop_id = 7
            soy_batch_size = 2500
            genotype_count = get_comparison_set_soy_subset_genotype_count()
            print('genotype_count shape: ', genotype_count.shape)

            sample_api = get_comparison_set_soy_subset_sample_api(genotype_count, soy_batch_size)
            print('sample_api shape: ', sample_api.shape)

            material_sdl = get_comparison_set_soy_subset_material_sdl()
            print('material_sdl shape: ', material_sdl.shape)

            material_sample_merge = sample_api.merge(genotype_count, how='inner',
                                                     left_on='sample_code', right_on='sample_id')

            print('material_sample_merge shape: ', material_sample_merge.shape)
            material_sdl_df_material_sample = material_sdl.merge(material_sample_merge, how='inner',
                                                                 left_on='material_guid', right_on='germplasm_guid')
            print('material_sdl_df_material_sample shape: ', material_sdl_df_material_sample.shape)

            soy_year_bebid = get_comparison_set_soy_year_bebid(DKU_DST_analysis_year, val)

            het_pool_1_df = soy_year_bebid.merge(material_sdl_df_material_sample, how='inner', on='be_bid')
            het_pool_1_df = het_pool_1_df[['year', 'be_bid', 'sample_id']]
            het_pool_1_df['year'] = het_pool_1_df['year'].astype(int)
            # het_pool_1_df = get_het_pools_soy(DKU_DST_analysis_year, 'sample', val)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))

            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)

        elif DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            val = 'NOAM'
            print('val: ', val)
            num_crop_id = 7
            soy_batch_size = 2500
            genotype_count = get_comparison_set_soy_subset_genotype_count()
            print('genotype_count shape: ', genotype_count.shape)

            sample_api = get_comparison_set_soy_subset_sample_api(genotype_count, soy_batch_size)
            print('sample_api shape: ', sample_api.shape)

            material_sdl = get_comparison_set_soy_subset_material_sdl()
            print('material_sdl shape: ', material_sdl.shape)

            material_sample_merge = sample_api.merge(genotype_count, how='inner',
                                                     left_on='sample_code', right_on='sample_id')

            print('material_sample_merge shape: ', material_sample_merge.shape)
            material_sdl_df_material_sample = material_sdl.merge(material_sample_merge, how='inner',
                                                                 left_on='material_guid', right_on='germplasm_guid')
            print('material_sdl_df_material_sample shape: ', material_sdl_df_material_sample.shape)

            soy_year_bebid = get_comparison_set_soy_year_bebid(DKU_DST_analysis_year, val)

            het_pool_1_df = soy_year_bebid.merge(material_sdl_df_material_sample, how='inner', on='be_bid')
            het_pool_1_df = het_pool_1_df[['year', 'be_bid', 'sample_id']]
            het_pool_1_df['year'] = het_pool_1_df['year'].astype(int)
            # het_pool_1_df = get_het_pools_soy(DKU_DST_analysis_year, 'sample', val)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))

            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)

        elif DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
            num_crop_id = 12
            sun_year_bebid = get_het_pools_sunflower_init(DKU_DST_analysis_year)

            sun_year_bebid_rec_donor = sun_year_bebid.merge(donor_df, how='inner', on='be_bid').merge(rec_df,
                                                                                                    how='inner',
                                                                                                    on='be_bid')

            hp1_df = sun_year_bebid_rec_donor.loc[sun_year_bebid_rec_donor["par_hp1_sample"].notnull(), ["year", "par_hp1_sample"]] \
                .rename(columns={'par_hp1_sample': 'sample_id'}) \
                .groupby(['sample_id']).min().reset_index()

            hp2_df = sun_year_bebid_rec_donor.loc[sun_year_bebid_rec_donor["par_hp2_sample"].notnull(), ["year", "par_hp2_sample"]] \
                .rename(columns={'par_hp2_sample': 'sample_id'}) \
                .groupby(['sample_id']).min().reset_index()

            het_pool_1_df = hp1_df.loc[hp1_df["year"] == int(DKU_DST_analysis_year), ].reset_index(drop=True)
            het_pool_2_df = hp2_df.loc[hp2_df["year"] == int(DKU_DST_analysis_year), ].reset_index(drop=True)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))

            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)

            batch_size = 750
        else:
            val = ''

        print('het_pool_1_df.shape: ', het_pool_1_df.shape)
        print('variant_set_df.shape: ', variant_set_df.shape)

        variant_id_df = variant_id_df.merge(variant_set_df, on=['variant_id'], how='inner').drop(columns="geno_index")
        print('variant_id_df.shape: ', variant_id_df.shape)
        print(
            "Markers used in analysis (note that variants used is fewer due to multiple markers -> one variant):" + str(
                variant_id_df.shape[0]))

        # Write recipe outputs
        print("Creating GRM for hetpool 1")
        hetpool1_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool1_by_year', DKU_DST_ap_data_sector,
                                                 DKU_DST_analysis_year)
        hetpool1_by_year_data_path = os.path.join(hetpool1_by_year_dir_path, 'hetpool1_by_year.parquet')
        print('hetpool1_by_year_data_path: ', hetpool1_by_year_data_path)
        isExist = os.path.exists(hetpool1_by_year_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(hetpool1_by_year_dir_path)

        sample_n = het_pool_1_df.shape[0]
        print('sample_n: ', sample_n)
        sample_n_iter = int(math.ceil(sample_n / batch_size))
        print('sample_n_iter: ', sample_n_iter)
        for i in range(sample_n_iter):
            df_out = get_snp_data_transform(
                het_pool_1_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :],
                variant_id_df,
                sample_comps_for_grm_df,
                DKU_DST_analysis_year,
                num_crop_id)
            print('hetpool1 i: ', i)
            if i == 0:
                df_out.to_parquet(hetpool1_by_year_data_path, engine='fastparquet')
            else:
                # Write recipe outputs
                df_out.to_parquet(hetpool1_by_year_data_path, engine='fastparquet', append=True)

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            pass
        else:
            print("Creating GRM for hetpool 2")
            sample_n = het_pool_2_df.shape[0]
            sample_n_iter = int(math.ceil(sample_n / batch_size))
            # Write recipe outputs
            hetpool2_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_by_year', DKU_DST_ap_data_sector,
                                                     DKU_DST_analysis_year)
            hetpool2_by_year_data_path = os.path.join(hetpool2_by_year_dir_path, 'hetpool2_by_year.parquet')
            print('hetpool2_by_year_data_path: ', hetpool2_by_year_data_path)
            isExist = os.path.exists(hetpool2_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool2_by_year_dir_path)

            for i in range(sample_n_iter):
                df_out = get_snp_data_transform(
                    het_pool_2_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :],
                    variant_id_df,
                    sample_comps_for_grm_df,
                    DKU_DST_analysis_year)
                print('hetpool2 i: ', i)
                if i == 0:
                    df_out.to_parquet(hetpool2_by_year_data_path, engine='fastparquet')
                else:
                    df_out.to_parquet(hetpool2_by_year_data_path, engine='fastparquet', append=True)

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        raise e


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        DKU_DST_analysis_year = data['analysis_year']
        historical_build_param = data['historical_build']

        print('historical_build_param: ', historical_build_param)

        if historical_build_param == 'True':
            years = [str(x) for x in range(2014, int(DKU_DST_analysis_year) + 1)]
            print(years)
        else:
            years = [str(DKU_DST_analysis_year)]
            print(years)

        if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
            print('Starting query for rec and donor df')
            rec_df = get_het_pools_sunflower_receiver()
            rec_df = rec_df.rename(columns={'receiver_p': 'par_hp2_be_bid',
                                            'receiver_sample_id': 'par_hp2_sample'})
            print('rec_df.shape: ', rec_df.shape)
            donor_df = get_het_pools_sunflower_donor()
            donor_df = donor_df.rename(columns={'donor_p': 'par_hp1_be_bid',
                                                'donor_sample_id': 'par_hp1_sample'})
            print('donor_df.shape: ', donor_df.shape)
        else:
            rec_df = None
            donor_df = None

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_sample_geno_output/data/hetpool1_by_year',
                                     DKU_DST_ap_data_sector, input_year, 'hetpool1_by_year.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            sample_geno_output_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, rec_df, donor_df)
            print('File created')
            print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
