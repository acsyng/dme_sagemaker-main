import argparse
import json
import os

import boto3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from libs.config.config_vars import ENVIRONMENT, S3_BUCKET
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix


def recommended_acres_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, DKU_DST_maturity, pipeline_runid, args, logger):
    try:

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            agg_levels = ['wce', 'tpp']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            agg_levels = ['tpp', 'meso', 'maturity']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            agg_levels = ['tpp', 'rec', 'maturity']
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            agg_levels = ['ecw', 'acs', 'maturity']
        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            agg_levels = ['country', 'mst', 'maturity']

        # Read recipe inputs
        gc_df = pd.read_parquet(
            os.path.join(args.s3_input_grid_classification_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'grid_classification.parquet'))[['place_id'] + agg_levels]
        print('gc_df: ', gc_df.shape)
        gc_df['maturity'] = gc_df['maturity'].astype(str)

        gmm_df = pd.read_parquet(
            os.path.join(args.s3_input_grid_classification_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'grid_market_maturity.parquet'))
        print('gmm_df: ', gmm_df.shape)
        gmm_df['maturity'] = gmm_df['maturity'].astype(str)

        temp_agg_df = pd.read_parquet(
            os.path.join(args.s3_input_temporal_aggregate_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid,
                         DKU_DST_maturity,
                         'temporal_aggregate.parquet'))
        print('temp_agg_df: ', temp_agg_df.shape)
        temp_agg_df['maturity'] = temp_agg_df['maturity'].astype(str)

        reg_agg_rank_df = pd.read_parquet(
            os.path.join(args.s3_input_regional_aggregate_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid,DKU_DST_maturity,
                         'regional_aggregate_no_rank.parquet'))
        print('reg_agg_rank_df: ', reg_agg_rank_df.shape)
        reg_agg_rank_df['maturity'] = reg_agg_rank_df['maturity'].astype(str)
        reg_agg_rank_df['stage'] = reg_agg_rank_df['stage'].astype(str)
        # reg_agg_rank_df = reg_agg_rank_df[reg_agg_rank_df.maturity == DKU_DST_maturity]
        # print('reg_agg_rank_df filtered for maturity: ', reg_agg_rank_df.shape)

        ta_gmm = temp_agg_df.merge(gmm_df, how='inner',
                                   on=['place_id', 'maturity', 'stage', 'analysis_year', 'ap_data_sector']).drop(
            columns=['id_y']).rename(columns={'id_x': 'id'}).merge(gc_df,
                                                                   how='inner', on=['place_id', 'maturity'])
        print(ta_gmm.shape)
        ta_gmm['rec_acres'] = np.where(ta_gmm['yield_metric'] > 60, ta_gmm['maturity_acres'], 0)

        rec_acres_df = pd.DataFrame()
        for i in agg_levels:
            agg_cols = ['entry_id', i, 'stage']
            print(agg_cols)
            agg_cols_select = agg_cols + ['rec_acres', 'maturity_acres']
            rec_acres_df1 = ta_gmm[agg_cols_select].groupby(agg_cols).agg(
                {'rec_acres': 'sum', 'maturity_acres': 'sum'}).reset_index().rename(columns={i: 'region_name'})

            rec_acres_df1[['region_name', 'stage']] = rec_acres_df1[['region_name', 'stage']].astype(str)
            rec_acres_df = pd.concat([rec_acres_df, rec_acres_df1], ignore_index=True)
            print(rec_acres_df.shape)

        reg_agg_rank_rec_acres_df = reg_agg_rank_df.merge(rec_acres_df, how='inner',
                                                          left_on=['entry_id', 'region_name', 'stage'],
                                                          right_on=['entry_id', 'region_name', 'stage'])
        print(reg_agg_rank_rec_acres_df[['yield_metric', 'rec_acres']].dropna().corr())
        # print()
        # print(reg_agg_rank_rec_acres_df.cperf.value_counts())
        # plt.scatter(reg_agg_rank_rec_acres_df['yield_metric'],
        #             reg_agg_rank_rec_acres_df['rec_acres'],
        #             c=pd.factorize(reg_agg_rank_rec_acres_df['cperf'])[0])

        # Create & write recipe output
        recommended_acres_dir_path = os.path.join('/opt/ml/processing/data/recommended_acres', DKU_DST_ap_data_sector,
                                                  DKU_DST_analysis_year, pipeline_runid, DKU_DST_maturity)

        recommended_acres_data_path = os.path.join(recommended_acres_dir_path, 'recommended_acres.parquet')

        print('recommended_acres_data_path: ', recommended_acres_data_path)
        isExist = os.path.exists(recommended_acres_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(recommended_acres_dir_path)

        reg_agg_rank_rec_acres_df.to_parquet(recommended_acres_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement recommended_acres error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_temporal_aggregate_folder', type=str,
                        help='s3 input temporal aggregate folder', required=True)
    parser.add_argument('--s3_input_grid_classification_folder', type=str,
                        help='s3 input grid classification folder', required=True)
    parser.add_argument('--s3_input_regional_aggregate_folder', type=str,
                        help='s3 input regional aggregate folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            maturity_list = ['MG0', 'MG1', 'MG2', 'MG3', 'MG4', 'MG5', 'MG6', 'MG7', 'MG8']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            maturity_list = ['6C', '6L', '6M', '7C', '7L', '7M', '8C', '5L', '8M', '5M', '5C']  # , '4L']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or \
                DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            s3 = boto3.resource('s3')
            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)
            fp_list = list()
            file_p = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_grid_performance/data/grid_performance',
                                  DKU_DST_ap_data_sector, str(DKU_DST_analysis_year), pipeline_runid)
            for obj in bucket.objects.filter(Prefix=file_p):
                fp = obj.key.replace(os.path.join(file_p, ''), '').split('/')[0]
                fp_list.append(fp)

            maturity_list = list(set(fp_list))
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7']
        # if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
        #    maturity_list = ['1', '2', '3', '4', '5', '6', '7']

        for input_year in years:
            for maturity in maturity_list:
                file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_recommended_acres/data/recommended_acres',
                                         DKU_DST_ap_data_sector, input_year, maturity, 'recommended_acres.parquet')

                # check_file_exists = check_if_file_exists_s3(file_path)
                # if check_file_exists is False:
                print('Creating file in the following location: ', file_path)
                recommended_acres_function(DKU_DST_ap_data_sector, input_year, maturity, pipeline_runid, args=args, logger=logger)
                print('File created')
                #    print()

                # else:
                #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
