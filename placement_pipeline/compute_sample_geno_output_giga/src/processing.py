import json
import pandas as pd
import math as math
import os

from libs.placement_lib.comparison_set_queries import get_comparison_set_soy_year_bebid, \
    get_comparison_set_soy_subset_genotype_count, \
    get_comparison_set_soy_subset_sample_api, get_comparison_set_soy_subset_material_sdl
from libs.placement_lib.geno_queries import get_variant_set, get_het_pools, get_het_pools_sunflower_init, \
    get_het_pools_sunflower_donor, get_het_pools_sunflower_receiver, giga_sunflower, get_sunflower_bebids, giga_corn
from libs.placement_lib.method_vs_grm_pca import get_snp_data_transform
from libs.event_bridge.event import error_event

from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET, S3_PREFIX_PLACEMENT


def sample_geno_output_giga_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, rec_df=None,
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

        print('DKU_DST_ap_data_sector: ', DKU_DST_ap_data_sector)

        # Compute recipe outputs
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            val = 'NOAM'
            print('val: ', val)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val)
            print("Number of sample id's in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            variant_set_df = get_variant_set(num_crop_id)
            print('variant_set_df.shape: ', variant_set_df.shape)
        elif DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            print('inside loop DKU_DST_ap_data_sector: ', DKU_DST_ap_data_sector)
            val = 'LATAM'
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                season = 'WNTR'
            else:
                season = 'SUMR'
            print('val: ', val, ' season: ', season)

            def giga_convert(grm_giga):
                print('unique be_bid_1: ', len(grm_giga.be_bid_1.unique()))
                print('unique be_bid_2: ', len(grm_giga.be_bid_2.unique()))

                grm_giga = grm_giga.reset_index().pivot(columns='be_bid_2', index='be_bid_1', values='distance_shared_allele').reset_index()
                grm_giga.columns.name = None
                grm_giga = grm_giga.rename(columns={"be_bid_1": "be_bid"})

                grm_giga["year"] = DKU_DST_analysis_year

                return grm_giga

            val = 'LATAM'
            print('val: ', val)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'be_bid', val, season)
            print("Number of be_bids in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of be_bids in pool 2: " + str(het_pool_2_df.shape[0]))
            # Get marker_id -> variant_id mapping
            # variant_set_df = get_variant_set(num_crop_id)
            # print('variant_set_df.shape: ', variant_set_df.shape)

            be_bid_set = pd.read_parquet(os.path.join('s3://', S3_BUCKET, S3_PREFIX_PLACEMENT, 'compute_comparison_set/data/comparison_set', DKU_DST_ap_data_sector, 'bebid_comp_set.parquet'))
            sample_set = pd.read_parquet(os.path.join('s3://', S3_BUCKET, S3_PREFIX_PLACEMENT, 'compute_comparison_set/data/comparison_set', DKU_DST_ap_data_sector, 'sample_comp_set.parquet'))
            sample_set_w_be_bid = sample_set[['lbg_bid', 'sample_id']].merge(be_bid_set[['lbg_bid', 'be_bid']], how='inner')
            print('sample_set_w_be_bid.shape: ', sample_set_w_be_bid.shape)

            hetpool1_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool1_df_by_year', DKU_DST_ap_data_sector, DKU_DST_analysis_year)
            hetpool1_by_year_data_path = os.path.join(hetpool1_by_year_dir_path, 'hetpool1_df.parquet')
            print('hetpool1_by_year_data_path: ', hetpool1_by_year_data_path)
            isExist = os.path.exists(hetpool1_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool1_by_year_dir_path)

            het_pool_1_df.to_parquet(hetpool1_by_year_data_path)

            sample_set_w_be_bid_data_path = os.path.join(hetpool1_by_year_dir_path, 'sample_set_w_be_bid.parquet')
            sample_set_w_be_bid.to_parquet(sample_set_w_be_bid_data_path)

            grm_giga_hetpool1 = giga_corn(het_pool_1_df, sample_set_w_be_bid, 750).drop_duplicates()
            print('grm_giga_hetpool1.shape: ', grm_giga_hetpool1.shape)

            hetpool1_giga_grm = giga_convert(grm_giga_hetpool1)
            print('hetpool1_giga_grm.shape: ', hetpool1_giga_grm.shape)

            hetpool1_grm_giga_data_path = os.path.join(hetpool1_by_year_dir_path, 'grm_giga.parquet')
            hetpool1_giga_grm.to_parquet(hetpool1_grm_giga_data_path)

            hetpool2_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_df_by_year', DKU_DST_ap_data_sector, DKU_DST_analysis_year)
            hetpool2_by_year_data_path = os.path.join(hetpool2_by_year_dir_path, 'hetpool2_df.parquet')
            print('hetpool2_by_year_data_path: ', hetpool2_by_year_data_path)
            isExist = os.path.exists(hetpool2_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool2_by_year_dir_path)

            het_pool_2_df.to_parquet(hetpool2_by_year_data_path)

            grm_giga_hetpool2 = giga_corn(het_pool_2_df, sample_set_w_be_bid, 750).drop_duplicates()
            print('grm_giga_hetpool2.shape: ', grm_giga_hetpool2.shape)

            hetpool2_giga_grm = giga_convert(grm_giga_hetpool2)
            print('hetpool2_giga_grm.shape: ', hetpool2_giga_grm.shape)

            hetpool2_grm_giga_data_path = os.path.join(hetpool2_by_year_dir_path, 'grm_giga.parquet')
            hetpool2_giga_grm.to_parquet(hetpool2_grm_giga_data_path)

        elif DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
            val = 'EAME'
            print('val: ', val)
            num_crop_id = 9
            het_pool_1_df, het_pool_2_df = get_het_pools(DKU_DST_analysis_year, 'sample', val)
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

            be_bid_set = pd.read_parquet(os.path.join('s3://', S3_BUCKET, S3_PREFIX_PLACEMENT,
                                                      'compute_comparison_set/data/comparison_set/SUNFLOWER_EAME_SUMMER/bebid_comp_set.parquet'))
            # 's3://us.com.syngenta.ap.nonprod/uat/dme/placement/compute_comparison_set/data/comparison_set/SUNFLOWER_EAME_SUMMER/bebid_comp_set.parquet')
            sample_set = pd.read_parquet(os.path.join('s3://', S3_BUCKET, S3_PREFIX_PLACEMENT,
                                                      'compute_comparison_set/data/comparison_set/SUNFLOWER_EAME_SUMMER/sample_comp_set.parquet'))
            # 's3://us.com.syngenta.ap.nonprod/uat/dme/placement/compute_comparison_set/data/comparison_set/SUNFLOWER_EAME_SUMMER/sample_comp_set.parquet')
            sample_set_w_be_bid = sample_set[['lbg_bid', 'sample_id']].merge(be_bid_set[['lbg_bid', 'be_bid']],
                                                                             how='inner')
            print('sample_set_w_be_bid.shape: ', sample_set_w_be_bid.shape)

            sun_year_bebid = get_het_pools_sunflower_init(DKU_DST_analysis_year)

            sun_year_bebid_rec_donor = sun_year_bebid.merge(donor_df, how='inner',
                                                            on='be_bid')  # .merge(rec_df, how='inner', on='be_bid')

            def het_fun(sql_df, id_type):
                if id_type == 'sample':
                    hp1_df = sql_df.loc[sql_df["par_hp1_sample"].notnull(), ["year", "par_hp1_sample"]] \
                        .rename(columns={'par_hp1_sample': 'sample_id'}) \
                        .groupby(['sample_id']).min().reset_index()

                    hp2_df = sql_df.loc[sql_df["par_hp2_sample"].notnull(), ["year", "par_hp2_sample"]] \
                        .rename(columns={'par_hp2_sample': 'sample_id'}) \
                        .groupby(['sample_id']).min().reset_index()

                else:
                    hp1_df = sql_df.loc[sql_df["par_hp1_be_bid"].notnull(), ["year", "par_hp1_be_bid"]] \
                        .rename(columns={'par_hp1_be_bid': 'be_bid'}) \
                        .groupby(['be_bid']).min().reset_index()

                    hp2_df = sql_df.loc[sql_df["par_hp2_be_bid"].notnull(), ["year", "par_hp2_be_bid"]] \
                        .rename(columns={'par_hp2_be_bid': 'be_bid'}) \
                        .groupby(['be_bid']).min().reset_index()

                hp1_df = hp1_df.loc[hp1_df["year"] == int(DKU_DST_analysis_year),].reset_index(drop=True)
                hp2_df = hp2_df.loc[hp2_df["year"] == int(DKU_DST_analysis_year),].reset_index(drop=True)

                return hp1_df, hp2_df

            het_pool_1_df, het_pool_2_df = het_fun(sun_year_bebid_rec_donor, 'be_bid')

            print("Number of sample id's/be_bids in pool 1: " + str(het_pool_1_df.shape[0]))
            print("Number of sample id's/be_bids in pool 2: " + str(het_pool_2_df.shape[0]))

            hetpool1_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool1_df_by_year',
                                                     DKU_DST_ap_data_sector,
                                                     DKU_DST_analysis_year)
            hetpool1_by_year_data_path = os.path.join(hetpool1_by_year_dir_path, 'hetpool1_df.parquet')
            print('hetpool1_by_year_data_path: ', hetpool1_by_year_data_path)
            isExist = os.path.exists(hetpool1_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool1_by_year_dir_path)

            het_pool_1_df.to_parquet(hetpool1_by_year_data_path)

            sample_set_w_be_bid_data_path = os.path.join(hetpool1_by_year_dir_path, 'sample_set_w_be_bid.parquet')
            sample_set_w_be_bid.to_parquet(sample_set_w_be_bid_data_path)

            grm_giga_hetpool1 = giga_sunflower(het_pool_1_df, sample_set_w_be_bid, 750)
            print('grm_giga_hetpool1.shape: ', grm_giga_hetpool1.shape)

            def giga_convert(grm_giga):
                print('unique be_bid_1: ', len(grm_giga.be_bid_1.unique()))
                print('unique be_bid_2: ', len(grm_giga.be_bid_2.unique()))

                grm_giga = grm_giga.reset_index().pivot(columns='be_bid_2', index='be_bid_1',
                                                        values='distance_shared_allele').reset_index()
                grm_giga.columns.name = None
                grm_giga = grm_giga.rename(columns={"be_bid_1": "be_bid"})

                grm_giga["year"] = DKU_DST_analysis_year

                return grm_giga

            hetpool1_giga_grm = giga_convert(grm_giga_hetpool1)

            hetpool1_grm_giga_data_path = os.path.join(hetpool1_by_year_dir_path, 'grm_giga.parquet')
            hetpool1_giga_grm.to_parquet(hetpool1_grm_giga_data_path)

            hetpool2_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_df_by_year',
                                                     DKU_DST_ap_data_sector,
                                                     DKU_DST_analysis_year)
            hetpool2_by_year_data_path = os.path.join(hetpool2_by_year_dir_path, 'hetpool2_df.parquet')
            print('hetpool2_by_year_data_path: ', hetpool2_by_year_data_path)
            isExist = os.path.exists(hetpool2_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(hetpool2_by_year_dir_path)

            het_pool_2_df.to_parquet(hetpool2_by_year_data_path)

            grm_giga_hetpool2 = giga_sunflower(het_pool_2_df, sample_set_w_be_bid, 750)
            print('grm_giga_hetpool2.shape: ', grm_giga_hetpool2.shape)
            hetpool2_giga_grm = giga_convert(grm_giga_hetpool2)

            hetpool2_grm_giga_data_path = os.path.join(hetpool2_by_year_dir_path, 'grm_giga.parquet')
            hetpool2_giga_grm.to_parquet(hetpool2_grm_giga_data_path)

            # Get marker_id -> variant_id mapping
            # variant_set_df = get_variant_set(num_crop_id)
            # print('variant_set_df.shape: ', variant_set_df.shape)

            batch_size = 750
        else:
            print('Data sector not identified properly.')
            val = ''

        # print('het_pool_1_df.shape: ', het_pool_1_df.shape)
        # print('variant_set_df.shape: ', variant_set_df.shape)
        #
        # variant_id_df = variant_id_df.merge(variant_set_df, on=['variant_id'], how='inner').drop(columns="geno_index")
        # print('variant_id_df.shape: ', variant_id_df.shape)
        # print(
        #     "Markers used in analysis (note that variants used is fewer due to multiple markers -> one variant):" + str(
        #         variant_id_df.shape[0]))
        #
        # # Write recipe outputs
        # print("Creating GRM for hetpool 1")
        # hetpool1_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool1_by_year', DKU_DST_ap_data_sector,
        #                                          DKU_DST_analysis_year)
        # hetpool1_by_year_data_path = os.path.join(hetpool1_by_year_dir_path, 'hetpool1_by_year.parquet')
        # print('hetpool1_by_year_data_path: ', hetpool1_by_year_data_path)
        # isExist = os.path.exists(hetpool1_by_year_dir_path)
        # if not isExist:
        #     # Create a new directory because it does not exist
        #     os.makedirs(hetpool1_by_year_dir_path)
        #
        # sample_n = het_pool_1_df.shape[0]
        # print('sample_n: ', sample_n)
        # sample_n_iter = int(math.ceil(sample_n / batch_size))
        # print('sample_n_iter: ', sample_n_iter)
        # for i in range(sample_n_iter):
        #     df_out = get_snp_data_transform(
        #         het_pool_1_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :],
        #         variant_id_df,
        #         sample_comps_for_grm_df,
        #         DKU_DST_analysis_year,
        #         num_crop_id)
        #     print('hetpool1 i: ', i)
        #     if i == 0:
        #         df_out.to_parquet(hetpool1_by_year_data_path, engine='fastparquet')
        #     else:
        #         # Write recipe outputs
        #         df_out.to_parquet(hetpool1_by_year_data_path, engine='fastparquet', append=True)

        # if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
        #     pass
        # else:
        #     print("Creating GRM for hetpool 2")
        #     sample_n = het_pool_2_df.shape[0]
        #     sample_n_iter = int(math.ceil(sample_n / batch_size))
        #     # Write recipe outputs
        #     hetpool2_by_year_dir_path = os.path.join('/opt/ml/processing/data/hetpool2_by_year', DKU_DST_ap_data_sector,
        #                                              DKU_DST_analysis_year)
        #     hetpool2_by_year_data_path = os.path.join(hetpool2_by_year_dir_path, 'hetpool2_by_year.parquet')
        #     print('hetpool2_by_year_data_path: ', hetpool2_by_year_data_path)
        #     isExist = os.path.exists(hetpool2_by_year_dir_path)
        #     if not isExist:
        #         # Create a new directory because it does not exist
        #         os.makedirs(hetpool2_by_year_dir_path)
        #
        #     for i in range(sample_n_iter):
        #         df_out = get_snp_data_transform(
        #             het_pool_2_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :],
        #             variant_id_df,
        #             sample_comps_for_grm_df,
        #             DKU_DST_analysis_year)
        #         print('hetpool2 i: ', i)
        #         if i == 0:
        #             df_out.to_parquet(hetpool2_by_year_data_path, engine='fastparquet')
        #         else:
        #             df_out.to_parquet(hetpool2_by_year_data_path, engine='fastparquet', append=True)

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
            # print('Starting query for rec and donor df')
            # rec_df = get_het_pools_sunflower_receiver()
            # rec_df = rec_df.rename(columns={'receiver_p': 'par_hp2_be_bid',
            #                                 'receiver_sample_id': 'par_hp2_sample'})
            # print('rec_df.shape: ', rec_df.shape)
            # donor_df = get_het_pools_sunflower_donor()
            # donor_df = donor_df.rename(columns={'donor_p': 'par_hp1_be_bid',
            #                                     'donor_sample_id': 'par_hp1_sample'})
            # print('donor_df.shape: ', donor_df.shape)

            print('Starting query for be_bids')
            donor_df = get_sunflower_bebids()
            rec_df = None
        else:
            rec_df = None
            donor_df = None

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                     'compute_sample_geno_output_giga/data/hetpool1_by_year',
                                     DKU_DST_ap_data_sector, input_year, 'hetpool1_by_year.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            sample_geno_output_giga_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, rec_df, donor_df)
            print('File created')
            print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
