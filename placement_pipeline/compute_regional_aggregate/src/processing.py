import argparse
import json
import os

import boto3
import pandas as pd
from scipy import stats

from libs.config.config_vars import ENVIRONMENT, S3_BUCKET
from libs.dme_statistical_tests import norm_ratio_t_test, var_ratio_f_test
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix


def regional_aggregate_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, DKU_DST_maturity, pipeline_runid, args, logger):
    try:
        s3 = boto3.resource('s3')

        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)
        regional_aggregate_query_pyspark_df = pd.DataFrame()
        buckets_objects = bucket.objects.filter(
            Prefix=os.path.join(get_s3_prefix(ENVIRONMENT),
                                'compute_regional_aggregate_query/data/regional_aggregate_query',
                                DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, DKU_DST_maturity, ''))
        for obj in buckets_objects:
            if "_SUCCESS" in obj.key:
                next
            else:
                # print(obj.key)
                df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                # print('df shape: ', df.shape)
                regional_aggregate_query_pyspark_df = pd.concat([regional_aggregate_query_pyspark_df, df],
                                                                ignore_index=True)

        print('regional_aggregate_query_pyspark_df: ', regional_aggregate_query_pyspark_df.shape)

        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            agg_levels = ["maturity", "stage", "region_name", "region_type", "density", "irrigation",
                          "previous_crop_soy"]
        else:
            agg_levels = ["maturity", "stage", "region_name", "region_type", "density", "irrigation",
                          "previous_crop_corn"]

        def agg_function(agg_base_df):
            agg_df = agg_base_df.merge(agg_base_df.loc[agg_base_df.cperf == 1, :], how='left', on=agg_levels,
                                       suffixes=('', '_check'))

            # Calculate h2h yield difference
            agg_df["yield_diff_vs_chk"] = agg_df["predict_ygsmn"] - agg_df["predict_ygsmn_check"]

            # Calculate h2h metrics
            agg_df["yield_pct_chk"], _, agg_df["yield_metric"], _ = norm_ratio_t_test(
                ent_pred=agg_df["predict_ygsmn"].to_numpy(),
                chk_pred=agg_df["predict_ygsmn_check"].to_numpy(),
                ent_stddev=agg_df["std_ygsmn"].to_numpy(),
                chk_stddev=agg_df["std_ygsmn_check"].to_numpy(),
                ent_count=agg_df["count_ygsmn"].to_numpy(),
                chk_count=agg_df["count_ygsmn_check"].to_numpy(),
                threshold_factor=1,
                spread_factor=1,
                direction='left')
            _, agg_df["stability_metric"] = var_ratio_f_test(ent_pred=agg_df["predict_ygsmn"].to_numpy(),
                                                             chk_pred=agg_df["predict_ygsmn_check"].to_numpy(),
                                                             ent_stddev=agg_df["std_ygsmn"].to_numpy(),
                                                             chk_stddev=agg_df["std_ygsmn_check"].to_numpy(),
                                                             ent_count=agg_df["count_ygsmn"].to_numpy(),
                                                             chk_count=agg_df["count_ygsmn_check"].to_numpy(),
                                                             spread_factor=1)

            agg_df["yield_metric"] = agg_df["yield_metric"].astype(float)
            # , "YGSMNp_10", "YGSMNp_50", "YGSMNp_90"]] \
            # Aggregate h2h metrics across checks
            agg_df = agg_df[agg_levels + ["entry_id", "cperf", "predict_ygsmn", "yield_pct_chk", "yield_diff_vs_chk",
                                          "yield_metric", "stability_metric", "YGSMNp_10", "YGSMNp_50", "YGSMNp_90",
                                          "predict_ygsmn_env"]] \
                .groupby(agg_levels + ["entry_id", "cperf"]) \
                .agg(predict_ygsmn=pd.NamedAgg(column="predict_ygsmn", aggfunc="mean"),
                     yield_pct_chk=pd.NamedAgg(column="yield_pct_chk", aggfunc="mean"),
                     yield_diff_vs_chk=pd.NamedAgg(column="yield_diff_vs_chk", aggfunc="mean"),
                     yield_metric=pd.NamedAgg(column="yield_metric", aggfunc=(lambda x: stats.gmean(x))),
                     stability_metric=pd.NamedAgg(column="stability_metric",
                                                  aggfunc=(lambda x: stats.gmean(x))),
                     YGSMNp_10=pd.NamedAgg(column="YGSMNp_10", aggfunc="mean"),
                     YGSMNp_50=pd.NamedAgg(column="YGSMNp_50", aggfunc="mean"),
                     YGSMNp_90=pd.NamedAgg(column="YGSMNp_90", aggfunc="mean"),
                     predict_ygsmn_env=pd.NamedAgg(column="predict_ygsmn_env", aggfunc="mean")
                     ).reset_index() \
                .reset_index(drop=False).rename(columns={"index": "id"})
            agg_df["stage"] = agg_df["stage"].astype('str')

            return agg_df

        out_list = []

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            aggregate_levels = ['wce', 'tpp', 'market_segment']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            aggregate_levels = ['tpp', 'meso', 'maturity_group']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            aggregate_levels = ['tpp', 'rec', 'maturity_group']
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            aggregate_levels = ['ecw', 'acs']
        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            aggregate_levels = ['country', 'mst']

        for colname in aggregate_levels:
            agg_base_df = regional_aggregate_query_pyspark_df.loc[
                regional_aggregate_query_pyspark_df['region_type'] == colname,]

            print(agg_base_df.shape)

            df = agg_function(agg_base_df)
            df["ap_data_sector"] = DKU_DST_ap_data_sector
            df["analysis_year"] = int(DKU_DST_analysis_year)
            df = df.drop(columns=['id'])
            out_list.append(df)

        out_df = pd.concat(out_list, ignore_index=True).reset_index()
        print(out_df.shape)
        out_df.head()

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            num_maturity = int(DKU_DST_maturity[2])
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            num_maturity = int(DKU_DST_maturity)
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            num_maturity = int(DKU_DST_maturity[0])
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            num_maturity = int(DKU_DST_maturity)
        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            num_maturity = int(DKU_DST_maturity)

        out_df = out_df.rename(columns={"index": "id"})
        out_df["id"] = out_df["id"] + 100000 * (num_maturity + 10 * (int(DKU_DST_analysis_year) - 2021))

        regional_aggregate_dir_path = os.path.join('/opt/ml/processing/data/regional_aggregate', DKU_DST_ap_data_sector,
                                                   DKU_DST_analysis_year, DKU_DST_maturity)
        regional_aggregate_data_path = os.path.join(regional_aggregate_dir_path, 'regional_aggregate_no_rank.parquet')
        print('regional_aggregate_data_path: ', regional_aggregate_data_path)
        isExist = os.path.exists(regional_aggregate_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(regional_aggregate_dir_path)
        out_df.to_parquet(regional_aggregate_data_path)

        regional_aggregate_dir_path_runid = os.path.join('/opt/ml/processing/data/regional_aggregate',
                                                         DKU_DST_ap_data_sector,
                                                         DKU_DST_analysis_year, pipeline_runid, DKU_DST_maturity)

        isExist_runid = os.path.exists(regional_aggregate_dir_path_runid)
        if not isExist_runid:
            # Create a new directory because it does not exist
            os.makedirs(regional_aggregate_dir_path_runid)

        pipeline_runid_regional_aggregate_data_path = os.path.join(regional_aggregate_dir_path_runid,
                                                                   'regional_aggregate_no_rank.parquet')
        out_df.to_parquet(pipeline_runid_regional_aggregate_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement regional_aggregate error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_regional_aggregate_query_folder', type=str,
                        help='s3 input regional aggregate query folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        DKU_DST_analysis_year = data['analysis_year']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        years = [str(DKU_DST_analysis_year)]
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            maturity_list = ['MG0', 'MG1', 'MG2', 'MG3', 'MG4', 'MG5', 'MG6', 'MG7', 'MG8']
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            maturity_list = ['6C', '6L', '6M', '7C', '7L', '7M', '8C', '5L', '8M', '5M', '5C']  # , '4L']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
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
        #     maturity_list = ['1', '2', '3', '4', '5', '6', '7']

        for input_year in years:
            for maturity in maturity_list:
                file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                         'compute_regional_aggregate/data/regional_aggregate',
                                         DKU_DST_ap_data_sector,
                                         input_year, maturity, 'regional_aggregate_no_rank.parquet')

                # check_file_exists = check_if_file_exists_s3(file_path)
                # if check_file_exists is False:
                print('Creating file in the following location: ', file_path)
                regional_aggregate_function(DKU_DST_ap_data_sector, input_year, maturity, pipeline_runid, args=args, logger=logger)
                print('File created')
                #    print()

                # else:
                #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
