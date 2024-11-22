import json
import os
import boto3
import pandas as pd
from libs.placement_lib.comparison_set_queries import get_comparison_set_corn_cda2_no_loop, \
    get_comparison_set_soy_cda2_no_loop, get_comparison_set_sunflower, \
    get_comparison_set_sunflower_subset_genotype_count, get_comparison_set_sunflower_subset_sample_api, \
    get_comparison_set_sunflower_subset_material_sdl, get_comparison_set_soy_subset_mint_material_giga_class, \
    get_comparison_set_sunflower_subset_agwc, get_comparison_set_sunflower_cda, get_comparison_set_sunflower_cda2

from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger

from libs.placement_s3_functions import get_s3_prefix, check_if_file_exists_s3
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET, CONFIG


def comparison_set_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, logger, extra_args):
    try:

        comparison_set_dir_path_runid = os.path.join('/opt/ml/processing/data/comparison_set_v1', DKU_DST_ap_data_sector,
                                                     pipeline_runid)
        s3_comparison_set_dir_path_runid = os.path.join(get_s3_prefix(ENVIRONMENT),
                                                        'compute_comparison_set_v1/data/comparison_set_v1',
                                                        DKU_DST_ap_data_sector, pipeline_runid)

        isExist = os.path.exists(comparison_set_dir_path_runid)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(comparison_set_dir_path_runid)

        comp_set_fpath = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_comparison_set_v1/data/comparison_set_v1',
                                      DKU_DST_ap_data_sector, pipeline_runid)
        print('comp_set_fpath: ', comp_set_fpath)
        s3 = boto3.client('s3')
        bucket = S3_BUCKET

        def upload_to_s3(fname):
            s3.upload_file(os.path.join(comparison_set_dir_path_runid, fname),
                           Bucket=bucket,
                           Key=os.path.join(s3_comparison_set_dir_path_runid, fname),
                           ExtraArgs=extra_args)
            print("file written directly to s3")
        # Compute recipe outputs
        if DKU_DST_ap_data_sector == "CORN_NA_SUMMER" or DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER":

            logger.info(f'getting comp set for corn section:  {DKU_DST_ap_data_sector}')

            corn_cda2_df = get_comparison_set_corn_cda2_no_loop()
            print('corn_cda2_df.shape: ', corn_cda2_df.shape)
            corn_cda2_df_data_path = os.path.join(comparison_set_dir_path_runid, 'corn_cda2_df.parquet')
            corn_cda2_df.to_parquet(corn_cda2_df_data_path)
            upload_to_s3('corn_cda2_df.parquet')

        elif DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            logger.info(f'getting comp set for in soy section: {DKU_DST_ap_data_sector}')

            soy_cda2_df = get_comparison_set_soy_cda2_no_loop()
            print('soy_cda2_df.shape: ', soy_cda2_df.shape)
            soy_cda2_df_data_path = os.path.join(comparison_set_dir_path_runid, 'soy_cda2_df.parquet')
            soy_cda2_df.to_parquet(soy_cda2_df_data_path)
            upload_to_s3('soy_cda2_df.parquet')

        elif DKU_DST_ap_data_sector == "SUNFLOWER_EAME_SUMMER":
            logger.info(f'getting comp set for in soy section: {DKU_DST_ap_data_sector}')

            # sun_comp_set = get_comparison_set_sunflower()
            # print('sun_comp_set.shape: ', sun_comp_set.shape)
            # sun_comp_set_data_path = os.path.join(comparison_set_dir_path_runid, 'sun_comp_set.parquet')
            # sun_comp_set.to_parquet(sun_comp_set_data_path)
            # upload_to_s3('sun_comp_set.parquet')

            logger.info(f'getting comp set for in soy section: {DKU_DST_ap_data_sector}')
            sunflower_batch_size = 2500
            mint_material_batch_size = 750

            genotype_count_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'genotype_count.parquet'))
            print('genotype_count_check_file_exists: ', genotype_count_check_file_exists)
            if genotype_count_check_file_exists is False:
                genotype_count = get_comparison_set_sunflower_subset_genotype_count().drop_duplicates()
                print('genotype_count shape: ', genotype_count.shape)
                genotype_count_data_path = os.path.join(comparison_set_dir_path_runid, 'genotype_count.parquet')
                genotype_count.to_parquet(genotype_count_data_path)
                upload_to_s3('genotype_count.parquet')
            else:
                print('genotype_count exists')
                genotype_count = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'genotype_count.parquet'))
                print('genotype_count.shape after reading: ', genotype_count.shape)

            sample_api_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'sample_api.parquet'))
            print('sample_api_check_file_exists: ', sample_api_check_file_exists)
            if sample_api_check_file_exists is False:
                sample_api = get_comparison_set_sunflower_subset_sample_api(genotype_count, sunflower_batch_size).drop_duplicates()
                print('sample_api shape: ', sample_api.shape)
                sample_api_data_path = os.path.join(comparison_set_dir_path_runid, 'sample_api.parquet')
                sample_api.to_parquet(sample_api_data_path)
                upload_to_s3('sample_api.parquet')
            else:
                print('sample_api exists')
                sample_api = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'sample_api.parquet'))
                print('sample_api.shape after reading: ', sample_api.shape)

            material_sdl_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'material_sdl.parquet'))
            print('material_sdl_check_file_exists: ', material_sdl_check_file_exists)
            if material_sdl_check_file_exists is False:
                material_sdl = get_comparison_set_sunflower_subset_material_sdl().drop_duplicates()
                print('material_sdl shape: ', material_sdl.shape)
                material_sdl_data_path = os.path.join(comparison_set_dir_path_runid, 'material_sdl.parquet')
                material_sdl.to_parquet(material_sdl_data_path)
                upload_to_s3('material_sdl.parquet')
            else:
                print('material_sdl exists')
                material_sdl = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'material_sdl.parquet'))
                print('material_sdl.shape after reading: ', material_sdl.shape)

            material_sample_merge_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'material_sample_merge.parquet'))
            print('material_sample_merge_check_file_exists: ', material_sample_merge_check_file_exists)
            if material_sample_merge_check_file_exists is False:
                material_sample_merge = sample_api.merge(genotype_count, how='inner',
                                                         left_on='sample_code', right_on='sample_id').drop_duplicates()
                print('material_sample_merge shape: ', material_sample_merge.shape)
                material_sample_merge_data_path = os.path.join(comparison_set_dir_path_runid,
                                                               'material_sample_merge.parquet')
                material_sample_merge.to_parquet(material_sample_merge_data_path)
                upload_to_s3('material_sample_merge.parquet')
            else:
                print('material_sample_merge exists')
                material_sample_merge = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'material_sample_merge.parquet'))
                print('material_sample_merge.shape after reading: ', material_sample_merge.shape)

            material_sdl_df_material_sample_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'material_sdl_df_material_sample.parquet'))
            print('material_sdl_df_material_sample_check_file_exists: ', material_sdl_df_material_sample_check_file_exists)
            if material_sdl_df_material_sample_check_file_exists is False:
                material_sdl_df_material_sample = material_sdl.merge(material_sample_merge, how='inner',
                                                                     left_on='material_guid', right_on='germplasm_guid').drop_duplicates()
                print('material_sdl_df_material_sample shape: ', material_sdl_df_material_sample.shape)
                material_sdl_df_material_sample_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                         'material_sdl_df_material_sample.parquet')
                material_sdl_df_material_sample.to_parquet(material_sdl_df_material_sample_data_path)
                upload_to_s3('material_sdl_df_material_sample.parquet')
            else:
                print('material_sdl_df_material_sample exists')
                material_sdl_df_material_sample = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'material_sdl_df_material_sample.parquet'))
                print('material_sdl_df_material_sample.shape after reading: ', material_sdl_df_material_sample.shape)

            mint_material_df_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'mint_material_df.parquet'))
            print('mint_material_df_check_file_exists: ', mint_material_df_check_file_exists)
            if mint_material_df_check_file_exists is False:
                mint_material_df, giga_class_df = get_comparison_set_soy_subset_mint_material_giga_class(
                    material_sdl_df_material_sample, mint_material_batch_size)
                print('mint_material_df shape: ', mint_material_df.shape)
                print('giga_class_df shape: ', giga_class_df.shape)
                mint_material_df_data_path = os.path.join(comparison_set_dir_path_runid, 'mint_material_df.parquet')
                mint_material_df.to_parquet(mint_material_df_data_path)
                upload_to_s3('mint_material_df.parquet')

                giga_class_df_data_path = os.path.join(comparison_set_dir_path_runid, 'giga_class_df.parquet')
                giga_class_df.to_parquet(giga_class_df_data_path)
                upload_to_s3('giga_class_df.parquet')
            else:
                print('mint_material_df exists')
                mint_material_df = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'mint_material_df.parquet'))
                print('mint_material_df.shape after reading: ', mint_material_df.shape)
                print('giga_class_df exists')
                giga_class_df = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'giga_class_df.parquet'))
                # giga_class_df = pd.read_csv(os.path.join('s3://', bucket, comp_set_fpath, 'giga_class_df.csv'))
                print('giga_class_df.shape after reading: ', giga_class_df.shape)

            comparison_set_df_mint_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'comparison_set_df_mint.parquet'))
            print('comparison_set_df_mint_check_file_exists: ', comparison_set_df_mint_check_file_exists)
            if comparison_set_df_mint_check_file_exists is False:
                comparison_set_df_mint = material_sdl_df_material_sample.merge(mint_material_df.drop_duplicates(), how='left', on='be_bid').drop_duplicates()
                print('comparison_set_df_mint shape: ', comparison_set_df_mint.shape)
                comparison_set_df_mint_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                'comparison_set_df_mint.parquet')
                comparison_set_df_mint.to_parquet(comparison_set_df_mint_data_path)
                upload_to_s3('comparison_set_df_mint.parquet')
            else:
                print('comparison_set_df_mint exists')
                comparison_set_df_mint = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'comparison_set_df_mint.parquet'))
                print('comparison_set_df_mint.shape after reading: ', comparison_set_df_mint.shape)

            comparison_set_df_mint_giga_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'comparison_set_df_mint_giga.parquet'))
            print('comparison_set_df_mint_giga_check_file_exists: ', comparison_set_df_mint_giga_check_file_exists)
            if comparison_set_df_mint_giga_check_file_exists is False:
                comparison_set_df_mint_giga = comparison_set_df_mint.merge(giga_class_df.drop_duplicates(), how='left', on='be_bid').drop_duplicates()
                print('comparison_set_df_mint_giga shape: ', comparison_set_df_mint_giga.shape)
                comparison_set_df_mint_giga_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                     'comparison_set_df_mint_giga.parquet')
                comparison_set_df_mint_giga.to_parquet(comparison_set_df_mint_giga_data_path)
                upload_to_s3('comparison_set_df_mint_giga.parquet')
            else:
                print('comparison_set_df_mint_giga exists')
                comparison_set_df_mint_giga = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'comparison_set_df_mint_giga.parquet'))
                print('comparison_set_df_mint_giga.shape after reading: ', comparison_set_df_mint_giga.shape)

            agwc_df_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'agwc_df.parquet'))
            print('agwc_df_check_file_exists: ', agwc_df_check_file_exists)
            if agwc_df_check_file_exists is False:
                agwc_df = get_comparison_set_sunflower_subset_agwc(comparison_set_df_mint_giga, sunflower_batch_size).drop_duplicates()
                print('agwc_df shape: ', agwc_df.shape)
                agwc_df_data_path = os.path.join(comparison_set_dir_path_runid, 'agwc_df.parquet')
                agwc_df.to_parquet(agwc_df_data_path)
                upload_to_s3('agwc_df.parquet')
            else:
                print('agwc_df exists')
                agwc_df = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'agwc_df.parquet'))
                print('agwc_df.shape after reading: ', agwc_df.shape)

            comparison_set_df_mint_giga_agwc_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'comparison_set_df_mint_giga_agwc.parquet'))
            print('comparison_set_df_mint_giga_agwc_check_file_exists: ', comparison_set_df_mint_giga_agwc_check_file_exists)
            if comparison_set_df_mint_giga_agwc_check_file_exists is False:
                comparison_set_df_mint_giga_agwc = comparison_set_df_mint_giga.merge(agwc_df, how='left', on='sample_id').drop_duplicates()
                print('comparison_set_df_mint_giga_agwc shape: ', comparison_set_df_mint_giga_agwc.shape)
                comparison_set_df_mint_giga_agwc_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                          'comparison_set_df_mint_giga_agwc.parquet')
                comparison_set_df_mint_giga_agwc.to_parquet(comparison_set_df_mint_giga_agwc_data_path)
                upload_to_s3('comparison_set_df_mint_giga_agwc.parquet')
            else:
                print('comparison_set_df_mint_giga_agwc exists')
                comparison_set_df_mint_giga_agwc = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'comparison_set_df_mint_giga_agwc.parquet'))
                print('comparison_set_df_mint_giga_agwc.shape after reading: ', comparison_set_df_mint_giga_agwc.shape)

            cda_df_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'cda_df.parquet'))
            print('cda_df_check_file_exists: ', cda_df_check_file_exists)
            if cda_df_check_file_exists is False:
                cda_df = get_comparison_set_sunflower_cda().drop_duplicates()
                print('cda_df shape: ', cda_df.shape)
                cda_df_data_path = os.path.join(comparison_set_dir_path_runid, 'cda_df.parquet')
                cda_df.to_parquet(cda_df_data_path)
                upload_to_s3('cda_df.parquet')
            else:
                print('cda_df exists')
                cda_df = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'cda_df.parquet'))
                print('cda_df.shape after reading: ', cda_df.shape)

            comparison_set_df_mint_giga_agwc_cda_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'comparison_set_df_mint_giga_agwc_cda.parquet'))
            print('comparison_set_df_mint_giga_agwc_cda_check_file_exists: ', comparison_set_df_mint_giga_agwc_cda_check_file_exists)
            if comparison_set_df_mint_giga_agwc_cda_check_file_exists is False:
                comparison_set_df_mint_giga_agwc_cda = comparison_set_df_mint_giga_agwc.merge(cda_df.drop_duplicates(), how='left',
                                                                                              on='be_bid').drop_duplicates()
                print('comparison_set_df_mint_giga_agwc_cda shape: ', comparison_set_df_mint_giga_agwc_cda.shape)
                comparison_set_df_mint_giga_agwc_cda_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                              'comparison_set_df_mint_giga_agwc_cda.parquet')
                comparison_set_df_mint_giga_agwc_cda.to_parquet(comparison_set_df_mint_giga_agwc_cda_data_path)
                upload_to_s3('comparison_set_df_mint_giga_agwc_cda.parquet')
            else:
                print('comparison_set_df_mint_giga_agwc_cda exists')
                comparison_set_df_mint_giga_agwc_cda = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'comparison_set_df_mint_giga_agwc_cda.parquet'))
                print('comparison_set_df_mint_giga_agwc_cda.shape after reading: ', comparison_set_df_mint_giga_agwc_cda.shape)

            cda2_df_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'cda2_df.parquet'))
            print('cda2_df_check_file_exists: ', cda2_df_check_file_exists)
            if cda2_df_check_file_exists is False:
                cda2_df = get_comparison_set_sunflower_cda2().drop_duplicates()
                print('cda2_df shape: ', cda2_df.shape)
                cda2_df_data_path = os.path.join(comparison_set_dir_path_runid, 'cda2_df.parquet')
                cda2_df.to_parquet(cda2_df_data_path)
                upload_to_s3('cda2_df.parquet')
            else:
                print('cda2_df exists')
                cda2_df = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'cda2_df.parquet'))
                print('cda2_df.shape after reading: ', cda2_df.shape)

            comparison_set_df_mint_giga_agwc_cda_cda2_check_file_exists = check_if_file_exists_s3(os.path.join(comp_set_fpath, 'comparison_set_df_mint_giga_agwc_cda_cda2.parquet'))
            print('comparison_set_df_mint_giga_agwc_cda_cda2_check_file_exists: ', comparison_set_df_mint_giga_agwc_cda_cda2_check_file_exists)
            if comparison_set_df_mint_giga_agwc_cda_cda2_check_file_exists is False:
                comparison_set_df_mint_giga_agwc_cda_cda2 = comparison_set_df_mint_giga_agwc_cda.merge(cda2_df, how='left',
                                                                                                       left_on='sample_id',
                                                                                                       right_on='distinct_samples').drop_duplicates()
                print('comparison_set_df_mint_giga_agwc_cda_cda2 shape: ', comparison_set_df_mint_giga_agwc_cda_cda2.shape)
                comparison_set_df_mint_giga_agwc_cda_cda2_data_path = os.path.join(comparison_set_dir_path_runid,
                                                                                   'comparison_set_df_mint_giga_agwc_cda_cda2.parquet')
                # comparison_set_df_mint_giga_agwc_cda_cda2.to_parquet(comparison_set_df_mint_giga_agwc_cda_cda2_data_path)
                # upload_to_s3('comparison_set_df_mint_giga_agwc_cda_cda2.parquet')
            else:
                print('comparison_set_df_mint_giga_agwc_cda_cda2 exists')
                comparison_set_df_mint_giga_agwc_cda_cda2 = pd.read_parquet(os.path.join('s3://', bucket, comp_set_fpath, 'comparison_set_df_mint_giga_agwc_cda_cda2.parquet'))
                print('comparison_set_df_mint_giga_agwc_cda_cda2.shape after reading: ', comparison_set_df_mint_giga_agwc_cda_cda2.shape)

            comparison_set_df = comparison_set_df_mint_giga_agwc_cda_cda2.drop_duplicates()
            print('comparison_set_df shape: ', comparison_set_df.shape)
            # comparison_set_df.info()

            comparison_set_df = comparison_set_df[['be_bid', 'lbg_bid', 'sample_id', 'cluster_idx',
                                                   'sample_variant_count', 'bebid_variant_count',
                                                   'cda_sample_variant_count']]

            comparison_set_df['fada_group'] = comparison_set_df['cluster_idx'].astype(str)
            comparison_set_df = comparison_set_df.drop(columns=['cluster_idx'])
            comparison_set_df[['sample_variant_count', 'bebid_variant_count', 'cda_sample_variant_count']] = \
                comparison_set_df[['sample_variant_count', 'bebid_variant_count',
                                   'cda_sample_variant_count']].astype(float)

            comparison_set_df = comparison_set_df[(~(comparison_set_df.sample_variant_count.isna())) &
                                                  (comparison_set_df.cda_sample_variant_count > 0) & (
                                                          comparison_set_df.bebid_variant_count > 0)]

            # comparison_set_df.info()
            print('comp_set_shape before upload: ', comparison_set_df.shape)
            comparison_set_df_data_path = os.path.join(comparison_set_dir_path_runid,
                                                       'comparison_set_df_end_chunk.parquet')
            comparison_set_df.to_parquet(comparison_set_df_data_path)
            upload_to_s3('comparison_set_df_end_chunk.parquet')



        else:
            logger.info(f'Please define correct ap data sector')

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

        # file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_comparison_set_v1/data/comparison_set', DKU_DST_ap_data_sector, 'comparison_set.parquet')

        # check_file_exists = check_if_file_exists_s3(file_path)
        # if check_file_exists is False:
        # logger.info(f'Creating file in the following location: {file_path}')
        # print('Creating file in the following location: ', file_path)
        comparison_set_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, logger=logger, extra_args=extra_args)
        logger.info(f'File created')
        # print()
        # else:
        # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
