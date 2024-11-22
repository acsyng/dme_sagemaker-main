import argparse
import json
import os

import boto3
from libs.postgres.postgres_connection import PostgresConnection
from sqlalchemy import create_engine
from libs.config.config_vars import CONFIG
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET
from libs.event_bridge.event import error_event, create_event
from libs.helper.parameters_helper import ParametersHelper
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.placement_lib.shap_summary_model import *


def regional_aggregate_with_ranking_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args, logger):
    try:
        # Read recipe inputs

        s3 = boto3.resource('s3')
        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)
        regional_aggregate_df = pd.DataFrame()
        for obj in bucket.objects.filter(
                Prefix=os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_recommended_acres/data/recommended_acres',
                                    DKU_DST_ap_data_sector,
                                    DKU_DST_analysis_year, pipeline_runid, '')):
            # print(obj.key)
            df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
            # print('df shape: ', df.shape)
            regional_aggregate_df = pd.concat([regional_aggregate_df, df], ignore_index=True)

        print('regional_aggregate_df shape: ', regional_aggregate_df.shape)
        print('maturity(s)', regional_aggregate_df['maturity'].unique().tolist())

        regional_aggregate_df['stage'] = regional_aggregate_df['stage'].astype(str)

        Be_bid_Ranking_df = pd.read_parquet(
            os.path.join(args.s3_input_shap_ui_output_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'Be_bid_Ranking.parquet'))
        print('Be_bid_Ranking_df: ', Be_bid_Ranking_df.shape)

        Be_bid_Ranking_df['stage'] = Be_bid_Ranking_df['stage'].astype(str)
        Be_bid_Ranking_df['maturity'] = Be_bid_Ranking_df['maturity'].astype(str)

        Be_bid_Ranking_joined_df = regional_aggregate_df.merge(Be_bid_Ranking_df, how='left',
                                                               on=['region_type',
                                                                   'region_name',
                                                                   'maturity',
                                                                   'irrigation',
                                                                   'entry_id',
                                                                   'stage',
                                                                   'ap_data_sector',
                                                                   'analysis_year'])

        list_of_columns = list(Be_bid_Ranking_joined_df.columns)
        list_of_columns.remove('id')
        list_of_columns.remove('cperf')
        list_of_columns.remove('predict_ygsmn')
        list_of_columns.remove('yield_pct_chk')
        list_of_columns.remove('yield_diff_vs_chk')
        list_of_columns.remove('yield_metric')
        list_of_columns.remove('stability_metric')
        list_of_columns.remove('YGSMNp_10')
        list_of_columns.remove('YGSMNp_50')
        list_of_columns.remove('YGSMNp_90')
        # list_of_columns.remove('predict_ygsmn_env')

        # Check into this filtering
        # Be_bid_Ranking_joined_df = Be_bid_Ranking_joined_df.loc[~(
        #         (Be_bid_Ranking_joined_df.duplicated(subset=list_of_columns, keep=False)) &
        #         (Be_bid_Ranking_joined_df['cperf'] == False)
        # )]

        # new filtering proposed
        # Be_bid_Ranking_joined_df1 = Be_bid_Ranking_joined_df.drop_duplicates(subset=list_of_columns)

        list_of_columns1 = list(Be_bid_Ranking_joined_df.columns)
        list_of_columns1.remove('id')
        list_of_columns1.remove('cperf')
        list_of_columns1.remove('predict_ygsmn')
        list_of_columns1.remove('yield_pct_chk')
        list_of_columns1.remove('yield_diff_vs_chk')
        list_of_columns1.remove('yield_metric')
        list_of_columns1.remove('stability_metric')
        list_of_columns1.remove('YGSMNp_10')
        list_of_columns1.remove('YGSMNp_50')
        list_of_columns1.remove('YGSMNp_90')
        list_of_columns1.remove('predict_ygsmn_env')

        Be_bid_Ranking_joined_df2 = Be_bid_Ranking_joined_df.drop_duplicates(subset=list_of_columns1, keep='last')

        print('Be_bid_Ranking_joined_df2.shape: ', Be_bid_Ranking_joined_df2.shape)
        print('Be_bid_Ranking_joined_df2.columns')
        print(Be_bid_Ranking_joined_df2.columns)
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            Be_bid_Ranking_joined_df = Be_bid_Ranking_joined_df2[[
                'id',
                'entry_id',
                'maturity',
                'stage',
                'cperf',
                'region_name',
                'region_type',
                'predict_ygsmn',
                'yield_pct_chk',
                'yield_diff_vs_chk',
                'yield_metric',
                'stability_metric',
                'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90', 'predict_ygsmn_env',
                'ap_data_sector',
                'analysis_year',
                'irrigation',
                'density',
                'previous_crop_soy',
                'rank',
                'rec_acres', 'maturity_acres']]
        else:
            Be_bid_Ranking_joined_df = Be_bid_Ranking_joined_df2[[
                'id',
                'entry_id',
                'maturity',
                'stage',
                'cperf',
                'region_name',
                'region_type',
                'predict_ygsmn',
                'yield_pct_chk',
                'yield_diff_vs_chk',
                'yield_metric',
                'stability_metric',
                'YGSMNp_10', 'YGSMNp_50', 'YGSMNp_90', 'predict_ygsmn_env',
                'ap_data_sector',
                'analysis_year',
                'irrigation',
                'density',
                'previous_crop_corn',
                'rank',
                'rec_acres', 'maturity_acres']]

        Be_bid_Ranking_joined_df['id'] = pd.to_numeric(Be_bid_Ranking_joined_df['id'], errors='coerce',
                                                       downcast='integer').fillna(-1).astype(int)

        regional_aggregate_with_ranking_dir_path = os.path.join(
            '/opt/ml/processing/data/regional_aggregate_with_ranking', DKU_DST_ap_data_sector, DKU_DST_analysis_year)
        regional_aggregate_with_ranking_data_path = os.path.join(regional_aggregate_with_ranking_dir_path,
                                                                 'regional_aggregate_with_ranking.parquet')
        print('regional_aggregate_with_ranking_data_path: ', regional_aggregate_with_ranking_data_path)
        isExist = os.path.exists(regional_aggregate_with_ranking_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(regional_aggregate_with_ranking_dir_path)

        # Write recipe outputs
        Be_bid_Ranking_joined_df.to_parquet(regional_aggregate_with_ranking_data_path)

        regional_aggregate_with_ranking_dir_path_runid = os.path.join(
            '/opt/ml/processing/data/regional_aggregate_with_ranking',
            DKU_DST_ap_data_sector,
            DKU_DST_analysis_year, pipeline_runid)

        isExist_runid = os.path.exists(regional_aggregate_with_ranking_dir_path_runid)
        if not isExist_runid:
            # Create a new directory because it does not exist
            os.makedirs(regional_aggregate_with_ranking_dir_path_runid)

        pipeline_runid_regional_aggregate_with_ranking_data_path = os.path.join(
            regional_aggregate_with_ranking_dir_path_runid,
            'regional_aggregate_with_ranking.parquet')
        Be_bid_Ranking_joined_df.to_parquet(pipeline_runid_regional_aggregate_with_ranking_data_path)
        upsert_regional_aggregate_postgres(pipeline_runid, Be_bid_Ranking_joined_df)

        # engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        #     username,
        #     password,
        #     localhost,
        #     port,
        #     database
        # )
        # )

        # Be_bid_Ranking_joined_df.to_sql(
        #    name='temp_sagemaker_dme_output_metrics',
        #    schema='public',
        #    con=engine,
        #    if_exists='overwrite',
        #    index=False
        # )
        create_event(CONFIG.get('event_bus'), DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid,
                     None, None, 'END',
                     f'Finish Sagemaker Placement pipeline: {pipeline_runid}', None)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement regional_aggregate_with_ranking error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def upsert_regional_aggregate_postgres(pipeline_runid, Be_bid_Ranking_joined_df):
    """Updates the regional aggregate table with the latest results

    First inserts the full df into a temp table whose name is handled by the upsert method on the PostgresConnection.  We
    then take the data from the temp table and provide upsert logic for how to handle conflicts.
    """
    Be_bid_Ranking_joined_df['pipeline_runid'] = pipeline_runid

    pc = PostgresConnection()

    upsert_query = """
            INSERT INTO placement.regional_aggregate 
                (entry_id, maturity, stage, irrigation, region_name, region_type, 
                ap_data_sector, analysis_year, cperf, predict_ygsmn, yield_pct_chk, 
                yield_diff_vs_chk, yield_metric, stability_metric, density, 
                previous_crop_corn, rank, pipeline_runid, rec_acres) 
            select
                entry_id, maturity, stage, irrigation, region_name, region_type, 
                ap_data_sector, analysis_year, cperf, predict_ygsmn, yield_pct_chk, 
                yield_diff_vs_chk, yield_metric, stability_metric, density, 
                previous_crop_corn, rank, pipeline_runid, rec_acres
            from public.{}
            ON CONFLICT (entry_id, maturity, stage, irrigation, region_name, region_type, ap_data_sector, analysis_year)
            DO UPDATE SET
            cperf = EXCLUDED.cperf, 
            predict_ygsmn = EXCLUDED.predict_ygsmn, 
            yield_pct_chk = EXCLUDED.yield_pct_chk, 
            yield_diff_vs_chk = EXCLUDED.yield_diff_vs_chk, 
            yield_metric = EXCLUDED.yield_metric, 
            stability_metric = EXCLUDED.stability_metric, 
            density = EXCLUDED.density, 
            previous_crop_corn = EXCLUDED.previous_crop_corn, 
            rank = EXCLUDED.rank,
            pipeline_runid = EXCLUDED.pipeline_runid,
            rec_acres = EXCLUDED.rec_acres
        """
    pc.upsert(upsert_query, 'regional_aggregate', Be_bid_Ranking_joined_df, is_spark=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_regional_aggregate_folder', type=str,
                        help='s3 input regional aggregate folder', required=True)
    parser.add_argument('--s3_input_shap_ui_output_folder', type=str,
                        help='s3 input shap ui output folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)
        # years = ['2023']
        DKU_DST_analysis_year = data['analysis_year']
        years = [str(DKU_DST_analysis_year)]

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                     'compute_regional_aggregate_with_ranking/data/regional_aggregate_with_ranking',
                                     DKU_DST_ap_data_sector,
                                     input_year, 'regional_aggregate_with_ranking.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            regional_aggregate_with_ranking_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args, logger=logger)
            print('File created')
            #    print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
