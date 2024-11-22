import asyncio
import json
import os
import time

import nest_asyncio
import numpy as np
import pandas as pd

from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib import cropfact_async
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET
from libs.event_bridge.event import error_event
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix


def cropFact_api_output_by_year_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid,
                                         trial_data_input_file_path, logger):
    try:

        if DKU_DST_ap_data_sector != 'CORNSILAGE_EAME_SUMMER':
            # os.path.join('/opt/ml/processing/input/data/', DKU_DST_ap_data_sector, DKU_DST_analysis_year)

            # trial_data_by_year_df = pd.read_parquet("/opt/ml/processing/input/data")
            # trial_data_by_year_df = pd.read_parquet(os.path.join('s3://us.com.syngenta.ap.nonprod', trial_data_input_file_path))
            trial_data_by_year_df = pd.read_parquet(os.path.join('s3://', S3_BUCKET, trial_data_input_file_path))

            df = trial_data_by_year_df[
                ["ap_data_sector", "cropfact_id", "x_longitude", "y_latitude", "year", "plant_date", "harvest_date",
                 "GMSTP_cf", "irrigation"]].drop_duplicates()

            print('input df shape: ', df.shape)
            # df = df.head(1000)

            # Compute recipe outputs from inputs
            df_uniq_input = cropfact_async.prepareDataFrameForCropFact(df.rename(columns={"GMSTP_cf": "GMSTP"}))

            # parse dataframe and build jsons
            jsons = cropfact_async.getCropFactJSONWrapper(df_uniq_input)

            # get crop fact data
            my_api = "https://cropfact.syngentaaws.org/services/cropphysiology"
            my_header = {'x-api-key': 'N1sgY9MO7O2vky35RBSLL20napGp3qRH6LWOAdKT', 'Content-Type': 'application/json'}
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            # df_cropfact = loop.run_until_complete(cropfact_async.getRecords(jsons, my_api, my_header))

            max_tries = 50
            n_tries = 0
            n_error = 1000  # initial value to start the while loop, just needs to be bigger than 0 to start
            batch_size = 50

            # so cropfact sometimes complains about 'too many requests' all at once.
            # this code will retry the calls that returned 'too many requests' at a slightly smaller batch_size
            # until either the number of retries is exceeded or there are no more 'too many requests' errors
            df_cropfact = pd.DataFrame()

            jsons_use = jsons.copy()
            while n_tries < max_tries and n_error > 0:
                print('n_tries: ', n_tries, ' of max_tries: ', max_tries)
                start_time = time.time()
                df_cropfact_temp = loop.run_until_complete(
                    cropfact_async.getRecords(jsons_use, my_api, my_header, batch_size=batch_size));
                print('n_tries: ', n_tries, '  df_cropfact_temp.shape: ', df_cropfact_temp.shape)

                dur = time.time() - start_time

                n_calls = len(jsons_use)
                # get idx of 'too many requests' error
                error_idx = np.nonzero((df_cropfact_temp['error'].values == 'Too Many Requests') | (
                            df_cropfact_temp['error'].values == 'Endpoint request timed out'))[0]
                jsons_use = list(np.array(jsons_use)[error_idx])

                # drop rows of df_cropfact_temp with errors, append to df_cropfact
                df_cropfact_temp = df_cropfact_temp[df_cropfact_temp['error'].isnull()]
                df_cropfact = pd.concat((df_cropfact, df_cropfact_temp), axis=0)
                print('n_tries: ', n_tries, '  remove errors from temp df_cropfact.shape: ', df_cropfact.shape)
                # update while loop parameters
                n_error = error_idx.shape[0]
                n_tries = n_tries + 1
                print('n_error: ', n_error)
                print('n_tries: ', n_tries)
                # update batch_size
                batch_size = np.minimum(batch_size, np.ceil(n_error / 5).astype(int))
                if batch_size <= 0:
                    batch_size = 1

            print("done")

            print('df_cropfact shape after loop run until complete: ', df_cropfact.shape)
            df_cropfact = df_cropfact[df_cropfact['error'].isnull()]  # only keep calls that returned data

            # preprocess df before merging, convert date format, convert irr format, perform rounding,
            # replace plant_date with plant_date_as_date, same with harvest_date. This is consistent with df_cropfact format
            df = cropfact_async.prepareDataFrameForCropFact(df, to_dos=['convert_date_format', 'convert_irr_format',
                                                                        'perform_rounding'])
            df = df.drop(columns=['plant_date', 'harvest_date'])
            df = df.rename(columns={'plant_date_as_date': 'plant_date', 'harvest_date_as_date': 'harvest_date'})
            # convert irrigation/drainage column to presence or absence
            df['irr_stat'] = 'Absence'
            df['irr_stat'][(df['irr_conv'] == 'IRR') | (df['irr_conv'] == 'LIRR')] = 'Presence'
            df['drain_stat'] = 'Absence'
            df['drain_stat'][df['irr_conv'] == 'TILE'] = 'Presence'
            # update df_crop's irrigation to the same presence/absence format

            print(df_cropfact.head())
            df_cropfact.Irrigation.loc[~df_cropfact['Irrigation'].isnull()] = 'Presence'
            df_cropfact.Irrigation.loc[df_cropfact['Irrigation'].isnull()] = 'Absence'
            # rename columns in df_crop
            df_crop_mapper = {'Planting': 'plant_date', 'Harvest': 'harvest_date', 'Irrigation': 'irr_stat',
                              'DrainageSystem': 'drain_stat', 'GMSTP_input': 'GMSTP_cf'}
            df_crop_pre_merge = df_cropfact.rename(columns=df_crop_mapper)

            # do merge
            on_list = ['x_longitude', 'y_latitude', 'plant_date', 'harvest_date', 'irr_stat', 'drain_stat', 'GMSTP_cf']

            df_out = df.merge(right=df_crop_pre_merge, how='left', on=on_list)
            # drop rows if missing data
            # grain yield potential will be empty if no cropfact data
            df_out = df_out.dropna(subset=['GMSTP_cf', 'GrainYieldPotential'])
            # df_out=df_out.drop_duplicates()

            date_cols = df_out.columns[df_out.columns.str.contains('PhenologicalStage')]

            for colname in date_cols:
                df_out[colname] = pd.to_datetime(df_out[colname], yearfirst=True).dt.dayofyear

            col_header_dict = {'PhenologicalStage': 'Stage', 'Planting': 'P', 'Harvest': 'H'}

            for key, newstr in col_header_dict.items():
                df_out.columns = df_out.columns.str.replace(key, newstr)

            df_out = df_out.drop(labels=["x_longitude", "y_latitude", "GMSTP_cf", "irrigation", "plant_date",
                                         "harvest_date", "irr_conv", "irr_stat", "drain_stat", "country",
                                         "countrySubDivision",
                                         "crop", "GrainMoistureContent", "GrainYieldPotential",
                                         "GrainYieldSolarPotential", "GrainYieldAttainable",
                                         "GrainYield", "RadiationUseEfficiency", "WaterUseEfficiency", "Texture",
                                         "error"], axis=1)
            df_out.columns = df_out.columns.str.lower()
            print('output dataframe shape: ', df_out.shape)

            # Write recipe outputs
            # data_path_cropFact_api_output_by_year = os.path.join('/opt/ml/processing/data', 'cropFact_api_output_by_year.parquet')
            # df_out.to_parquet(data_path_cropFact_api_output_by_year)

            cropFact_api_output_by_year_dir_path = os.path.join('/opt/ml/processing/data/cropFact_api_output_by_year',
                                                                DKU_DST_ap_data_sector, DKU_DST_analysis_year)

            cropFact_api_output_by_year_data_path = os.path.join(cropFact_api_output_by_year_dir_path,
                                                                 'cropFact_api_output_by_year.parquet')
            print('cropFact_api_output_by_year_data_path: ', cropFact_api_output_by_year_data_path)
            isExist = os.path.exists(cropFact_api_output_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(cropFact_api_output_by_year_dir_path)

            df_out.info()
            df_out['calciumcarbonatecontent'] = df_out['calciumcarbonatecontent'].astype(float)
            df_out.to_parquet(cropFact_api_output_by_year_data_path)

        else:
            # os.path.join('/opt/ml/processing/input/data/', DKU_DST_ap_data_sector, DKU_DST_analysis_year)

            # trial_data_by_year_df = pd.read_parquet("/opt/ml/processing/input/data")
            # trial_data_by_year_df = pd.read_parquet(os.path.join('s3://us.com.syngenta.ap.nonprod', trial_data_input_file_path))
            trial_data_by_year_df = pd.read_parquet(os.path.join('s3://', S3_BUCKET, trial_data_input_file_path))

            df = trial_data_by_year_df[
                ["ap_data_sector", "cropfact_id", "x_longitude", "y_latitude", "year", "plant_date", "harvest_date",
                 "irrigation"]].drop_duplicates()

            print('input df shape: ', df.shape)
            # df = df.head(1000)

            # Compute recipe outputs from inputs
            df_uniq_input = cropfact_async.prepareDataFrameForCropFact(df)

            # parse dataframe and build jsons
            jsons = cropfact_async.getCropFactJSONWrapper(df_uniq_input)

            # get crop fact data
            my_api = "https://cropfact.syngentaaws.org/services/cropphysiology"
            my_header = {'x-api-key': 'N1sgY9MO7O2vky35RBSLL20napGp3qRH6LWOAdKT', 'Content-Type': 'application/json'}
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            # df_cropfact = loop.run_until_complete(cropfact_async.getRecords(jsons, my_api, my_header))

            max_tries = 50
            n_tries = 0
            n_error = 1000  # initial value to start the while loop, just needs to be bigger than 0 to start
            batch_size = 50

            # so cropfact sometimes complains about 'too many requests' all at once.
            # this code will retry the calls that returned 'too many requests' at a slightly smaller batch_size
            # until either the number of retries is exceeded or there are no more 'too many requests' errors
            df_cropfact = pd.DataFrame()

            jsons_use = jsons.copy()
            while n_tries < max_tries and n_error > 0:
                print('n_tries: ', n_tries, ' of max_tries: ', max_tries)
                start_time = time.time()
                df_cropfact_temp = loop.run_until_complete(
                    cropfact_async.getRecords(jsons_use, my_api, my_header, batch_size=batch_size));
                print('n_tries: ', n_tries, '  df_cropfact_temp.shape: ', df_cropfact_temp.shape)

                dur = time.time() - start_time

                n_calls = len(jsons_use)
                # get idx of 'too many requests' error
                error_idx = np.nonzero((df_cropfact_temp['error'].values == 'Too Many Requests') | (
                            df_cropfact_temp['error'].values == 'Endpoint request timed out'))[0]
                jsons_use = list(np.array(jsons_use)[error_idx])

                # drop rows of df_cropfact_temp with errors, append to df_cropfact
                df_cropfact_temp = df_cropfact_temp[df_cropfact_temp['error'].isnull()]
                df_cropfact = pd.concat((df_cropfact, df_cropfact_temp), axis=0)
                print('n_tries: ', n_tries, '  remove errors from temp df_cropfact.shape: ', df_cropfact.shape)
                # update while loop parameters
                n_error = error_idx.shape[0]
                n_tries = n_tries + 1
                print('n_error: ', n_error)
                print('n_tries: ', n_tries)
                # update batch_size
                batch_size = np.minimum(batch_size, np.ceil(n_error / 5).astype(int))
                if batch_size <= 0:
                    batch_size = 1

            print("done")

            print('df_cropfact shape after loop run until complete: ', df_cropfact.shape)
            df_cropfact = df_cropfact[df_cropfact['error'].isnull()]  # only keep calls that returned data

            # preprocess df before merging, convert date format, convert irr format, perform rounding,
            # replace plant_date with plant_date_as_date, same with harvest_date. This is consistent with df_cropfact format
            df = cropfact_async.prepareDataFrameForCropFact(df, to_dos=['convert_date_format', 'convert_irr_format',
                                                                        'perform_rounding'])
            df = df.drop(columns=['plant_date', 'harvest_date'])
            df = df.rename(columns={'plant_date_as_date': 'plant_date', 'harvest_date_as_date': 'harvest_date'})
            # convert irrigation/drainage column to presence or absence
            df['irr_stat'] = 'Absence'
            df['irr_stat'][(df['irr_conv'] == 'IRR') | (df['irr_conv'] == 'LIRR')] = 'Presence'
            df['drain_stat'] = 'Absence'
            df['drain_stat'][df['irr_conv'] == 'TILE'] = 'Presence'
            # update df_crop's irrigation to the same presence/absence format

            print(df_cropfact.head())
            df_cropfact.Irrigation.loc[~df_cropfact['Irrigation'].isnull()] = 'Presence'
            df_cropfact.Irrigation.loc[df_cropfact['Irrigation'].isnull()] = 'Absence'
            # rename columns in df_crop
            df_crop_mapper = {'Planting': 'plant_date', 'Harvest': 'harvest_date', 'Irrigation': 'irr_stat',
                              'DrainageSystem': 'drain_stat'}
            df_crop_pre_merge = df_cropfact.rename(columns=df_crop_mapper)

            # do merge
            on_list = ['x_longitude', 'y_latitude', 'plant_date', 'harvest_date', 'irr_stat', 'drain_stat']

            df_out = df.merge(right=df_crop_pre_merge, how='left', on=on_list)
            # drop rows if missing data
            # grain yield potential will be empty if no cropfact data
            df_out = df_out  # .dropna(subset=['GMSTP_cf', 'GrainYieldPotential'])
            # df_out=df_out.drop_duplicates()

            date_cols = df_out.columns[df_out.columns.str.contains('PhenologicalStage')]

            for colname in date_cols:
                df_out[colname] = pd.to_datetime(df_out[colname], yearfirst=True).dt.dayofyear

            col_header_dict = {'PhenologicalStage': 'Stage', 'Planting': 'P', 'Harvest': 'H'}

            for key, newstr in col_header_dict.items():
                df_out.columns = df_out.columns.str.replace(key, newstr)

            df_out = df_out.drop(labels=["x_longitude", "y_latitude", "irrigation", "plant_date",
                                         "harvest_date", "irr_conv", "irr_stat", "drain_stat", "country",
                                         "countrySubDivision",
                                         "crop", "RadiationUseEfficiency", "WaterUseEfficiency", "Texture", "error"], axis=1)
            df_out.columns = df_out.columns.str.lower()
            print('output dataframe shape: ', df_out.shape)

            # Write recipe outputs
            # data_path_cropFact_api_output_by_year = os.path.join('/opt/ml/processing/data', 'cropFact_api_output_by_year.parquet')
            # df_out.to_parquet(data_path_cropFact_api_output_by_year)

            cropFact_api_output_by_year_dir_path = os.path.join('/opt/ml/processing/data/cropFact_api_output_by_year',
                                                                DKU_DST_ap_data_sector, DKU_DST_analysis_year)

            cropFact_api_output_by_year_data_path = os.path.join(cropFact_api_output_by_year_dir_path,
                                                                 'cropFact_api_output_by_year.parquet')
            print('cropFact_api_output_by_year_data_path: ', cropFact_api_output_by_year_data_path)
            isExist = os.path.exists(cropFact_api_output_by_year_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(cropFact_api_output_by_year_dir_path)

            df_out.info()
            df_out['calciumcarbonatecontent'] = df_out['calciumcarbonatecontent'].astype(float)
            df_out.to_parquet(cropFact_api_output_by_year_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement cropFact_api_output_by_year error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        historical_build = data['historical_build']

        # years = ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']
        if historical_build == 'True':
            years = [str(x) for x in range(2014, int(DKU_DST_analysis_year) + 1)]
        else:
            years = [str(DKU_DST_analysis_year)]

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                     'compute_cropFact_api_output_by_year/data/cropFact_api_output_by_year',
                                     DKU_DST_ap_data_sector, input_year, 'cropFact_api_output_by_year.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            trial_data_input_file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                                      'compute_trial_data_by_year/data/trial_data_by_year',
                                                      DKU_DST_ap_data_sector, input_year, 'trial_data_by_year.parquet')

            print('Creating file in the following location: ', file_path)
            cropFact_api_output_by_year_function(DKU_DST_ap_data_sector, input_year, pipeline_runid,
                                                 trial_data_input_file_path, logger)
            print('File created')

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
