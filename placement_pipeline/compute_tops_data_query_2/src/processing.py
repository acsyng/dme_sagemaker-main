import json
import os
import pandas as pd
import gc

from libs.denodo.denodo_connection import DenodoConnection
from libs.placement_lib.geno_queries import get_cmt_bebid, get_laas_bebid
from libs.event_bridge.event import error_event
import boto3
import argparse
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def tops_data_query_2_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args):
    try:

        fpath = os.path.join(args.s3_input_tops_data_folder, 'full.parquet')

        S3_CORN_NOAM_SUMR_2023_df = pd.read_parquet(fpath)
        print('S3_CORN_NOAM_SUMR_2023_df.shape: ', S3_CORN_NOAM_SUMR_2023_df.shape)
        cols_to_drop = ['abbreviated_code', 'pedigree_stem', 'stable_variety_code', 'stable_line', 'early_stage_name',
                        'calculated', 'trait_guid', 'trait_uom', 'dec360_guid', 'created_timestamp',
                        'observation_timestamp', 'obs_created_user',
                        'obs_created_user_email', 'discarded_date', 'deactivated_date', 'research_station_name',
                        'research_station_code', 'spirit_trial_guid', 'loc360_trial_guid', 'spirit_location_guid',
                        'plot_id', 'plot_subplot_guid', 'num_rows', 'num_ranges', 'country_guid', 'material_batch_uuid',
                        'trial_design_type_code', 'no_plot', 'no_plot_in_trial', 'replication_no', 'block_no',
                        'entry_no', 'loc360_location_guid', 'seeds_per_packet', 'prefer_plant_no_plot', 'count_per_unit',
                        'changed_since', 'units_per_plot', 'county_name', 'harvest_area', 'continent_lid', 'harvest_length',
                        'no_of_rows_harvested', 'row_length', 'row_width',
                        'experiment_trial_no', 'trial_state', 'placd', 'crop_guid', 'crop_code', 'season_lid',
                        'min_range', 'min_row', 'Unnamed_Rename', 'result_date_value', 'result_alpha_value',
                        'cagrf', 'check_competitor', 'check_line_to_beat_f', 'check_line_to_beat_m', 'cmatf',
                        'bg_guid', 'state_province_name', 'country_name']
        S3_CORN_NOAM_SUMR_2023_df = S3_CORN_NOAM_SUMR_2023_df.drop(columns=cols_to_drop)
        s3 = boto3.resource('s3')

        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)
        coord_bak_df = pd.DataFrame()
        buckets_objects = bucket.objects.filter(
            Prefix=os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_tops_data_query/data/tops_data_query',
                                DKU_DST_ap_data_sector, DKU_DST_analysis_year, ''))
        for obj in buckets_objects:
            if "_SUCCESS" in obj.key:
                next
            else:
                # print(obj.key)
                df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                # print('df shape: ', df.shape)
                coord_bak_df = coord_bak_df.append(df)
                # print('grid_df shape: ', grid_df.shape)

        print('coord_bak_df.shape: ', coord_bak_df.shape)
        coord_bak_df.head(3)

        with DenodoConnection() as dc:
            rv_fe_query = dc.get_data("""
                    SELECT
                        source_id,
                        value
                    FROM managed.rv_feature_export
                    WHERE feature_name = 'MATURITY_ZONE'
                    AND feature_level = 'LOCATION'
                    -- AND feature_provider = 'SPIRIT'
                    AND market_name = 'CORN_NA_SUMMER'
                    AND CAST(year AS integer) = 2023
                    """)

        rv_fe = rv_fe_query
        print('rv_fe.shape: ', rv_fe.shape)

        with DenodoConnection() as dc:
            rv_fe_pcrpc_query = dc.get_data("""
                    SELECT
                        source_id,
                        value
                    FROM managed.rv_feature_export
                    WHERE feature_name = 'PCRPC'
                      AND feature_level = 'LOCATION'
                      AND feature_provider = 'SPIRIT'
                      AND market_name = 'CORN_NA_SUMMER'
                      AND CAST(year AS integer) = 2023
                      """)

        rv_fe_pcrpc = rv_fe_pcrpc_query
        print('rv_fe_pcrpc.shape: ', rv_fe_pcrpc.shape)

        results = S3_CORN_NOAM_SUMR_2023_df.merge(coord_bak_df,
                                                  on='loc_code',
                                                  how='left').merge(rv_fe,
                                                                    left_on='loc_selector',
                                                                    right_on='source_id',
                                                                    how='left').merge(rv_fe_pcrpc,
                                                                                      left_on='loc_selector',
                                                                                      right_on='source_id',
                                                                                      how='left')
        print('results.shape: ', results.shape)

        results_filtered_1 = results[(results.year == 2023) & (results.ap_data_sector == 'CORN_NOAM_SUMR') &
                                     (results.crop_type_code.str.contains('YG'))]

        print('results_filtered_1 shape: ', results_filtered_1.shape)

        results_filtered_2 = results_filtered_1[((results_filtered_1.trait_measure_code == 'YGSMN') & (
                results.result_numeric_value > 0) & (results.result_numeric_value < 300)) |
                                                ((results_filtered_1.trait_measure_code == 'GMSTP') & (
                                                        results.result_numeric_value >= 4.5) & (
                                                         results.result_numeric_value < 35)) | (
                                                        results_filtered_1.trait_measure_code == 'HAVPN')]
        print('results_filtered_2 shape: ', results_filtered_2.shape)

        results_filtered = results_filtered_2[
            (results_filtered_2['pr_exclude'] == 0) & (results_filtered_2['psp_exclude'] == 0) & (
                    results_filtered_2['tr_exclude'] == 0) & (
                ~(results_filtered_2['material_id'].str.lower().isin(['filler', 'purple'])))]

        del [[S3_CORN_NOAM_SUMR_2023_df, results, results_filtered_1, results_filtered_2]]
        gc.collect()
        S3_CORN_NOAM_SUMR_2023_df = pd.DataFrame()
        results = pd.DataFrame()
        results_filtered_1 = pd.DataFrame()
        results_filtered_2 = pd.DataFrame()

        print('results_filtered.shape: ', results_filtered.shape)
        results_filtered.head(3)

        # Read in rv_corn_material_tester, left join on be_bid with be_bid

        cmt_df = get_cmt_bebid(results_filtered, 2000)
        print('cmt_df shape: ', cmt_df.shape)
        laas_df = get_laas_bebid(results_filtered, 2000)
        print('laas_df shape: ', laas_df.shape)

        print('results_filtered shape: ', results_filtered.shape)
        results_filtered.info(memory_usage='deep')
        results_joined_int = results_filtered.merge(cmt_df, on='be_bid', how='left')
        results_joined_int.info(memory_usage='deep')

        del [[results_filtered]]
        gc.collect()
        results_filtered = pd.DataFrame()

        results_joined = results_joined_int.merge(laas_df, on='be_bid', how='left')
        print('results_joined shape: ', results_joined.shape)
        results_joined.info(memory_usage='deep')

        print('results_joined.dtypes: ', results_joined.dtypes)
        list_str_obj_cols = results_joined.columns[results_joined.dtypes == "datetime64[ns]"].tolist()
        for str_obj_col in list_str_obj_cols:
            results_joined[str_obj_col] = results_joined[str_obj_col].astype("object")

        # Write recipe outputs
        tops_data_query_2_dir_path = os.path.join('/opt/ml/processing/data/tops_data_query_2', DKU_DST_ap_data_sector,
                                                  DKU_DST_analysis_year)

        tops_data_query_2_data_path = os.path.join(tops_data_query_2_dir_path, 'tops_data_query_2.parquet')
        print('tops_data_query_2_data_path: ', tops_data_query_2_data_path)
        isExist = os.path.exists(tops_data_query_2_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(tops_data_query_2_dir_path)

        results_joined.to_parquet(tops_data_query_2_data_path)

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_tops_data_query_folder', type=str,
                        help='s3 input tops data query folder', required=True)
    parser.add_argument('--s3_input_tops_data_folder', type=str,
                        help='s3 input tops data folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']

        # years = ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
        years = ['2023']
        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_tops_data_query_2/data/tops_data_query_2',
                                     DKU_DST_ap_data_sector,
                                     input_year, 'tops_data_query_2.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            tops_data_query_2_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args)
            print('File created')
            #    print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
