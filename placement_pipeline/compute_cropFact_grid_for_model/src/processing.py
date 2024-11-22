import argparse
import json
import os

import numpy as np
import pandas as pd
import pynndescent

from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix


def cropFact_grid_for_model_function(DKU_DST_ap_data_sector, maturity_group, pipeline_runid, args, logger):
    try:

        # Read recipe inputs
        # trial_feature_keep_array = dataiku.Dataset("trial_feature_keep_array")
        # feature_keep_df = trial_feature_keep_array.get_dataframe()
        feature_keep_df = pd.read_parquet(
            os.path.join(args.s3_input_trial_feature_keep_array_folder, 'trial_feature_keep_array',
                         DKU_DST_ap_data_sector,
                         'trial_feature_keep_array.parquet'))
        print('feature_keep_df shape: ', feature_keep_df.shape)

        cropfact_df_all_years = pd.read_parquet(
            os.path.join(args.s3_input_trial_feature_keep_array_folder, 'cropFact_api_output_all_years',
                         DKU_DST_ap_data_sector,
                         'cropFact_api_output_all_years.parquet'))
        print('cropfact_df_all_years shape: ', cropfact_df_all_years.shape)

        feature_keep_df = feature_keep_df.loc[:, feature_keep_df.iloc[0, :] > 0]
        print('feature_keep_df shape after filtering: ', feature_keep_df.shape)
        # cropFact_grid = dataiku.Dataset("cropFact_grid")
        # cropFact_grid_df = cropFact_grid.get_dataframe()
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            cropFact_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                        'Grid_10km_RM_Density_Final/cropFact_grid_10km.gz'), sep='\t',
                                           names=['FID', '_uid_', 'row_id', 'grid_id', 'region_c', 'place_id', 'crop_n',
                                                  'harvest_y', 'season_n',
                                                  'watermgmt', 'timestamp', 'start_date', 'end_date', 'ysolar', 'ypot',
                                                  'yatt', 'tt',
                                                  'tempavg', 'tempmin', 'tempmax', 'rhavg', 'rhmin', 'rhmax', 'precsum',
                                                  'radsum', 'parisum', 'et0sum', 'etpsum', 'etasum', 'vpdmax', 'wsmax',
                                                  'swd', 'swe', 'scd', 'sht', 'sptr', 'svpd', 'irrig', 'geometry'])

            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            cropFact_grid_df = cropFact_grid_df.drop(columns=['_uid_'])

            soil_grid_df = pd.read_csv(
                os.path.join(args.s3_input_grid_classification_folder, 'Grid_10km_RM_Density_Final/mio095_soil.gz'),
                sep='\t',
                names=['place_id', 'latitude', 'longitude', 'sandcontent', 'siltcontent',
                       'claycontent', 'phwater', 'cationexchangecapacity',
                       'coarsefragmentcontent',
                       'calciumcarbonatecontent', 'organicmattercontent', 'bulkdensity',
                       'rootdepthconstraint',
                       'availablewateratfieldcapacity', 'hydraulicconductivityatsaturation'])
            print('soil_grid_df shape: ', soil_grid_df.shape)

            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_RM_Density_Final/Grid_10km_RM_Density_Final.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)
            grid_10km_df = grid_10km_df.drop(columns=["LATITUDE", "LONGITUDE", "Core_North_South"])
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            cropFact_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                        'Grid_10km_Soy_NA_Summer/EnvChar-CropFact-Soybean-Features-2009ToPresent-Annual-NA-10km-SYN-v1.csv'))

            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            # cropFact_grid_df = cropFact_grid_df.drop(columns=['_uid_'])
            cropFact_grid_df = cropFact_grid_df.rename(columns={'geom': 'geometry'})

            soil_grid_df = pd.read_csv(
                os.path.join(args.s3_input_grid_classification_folder, 'Grid_10km_Soy_NA_Summer/mio095_soil.gz'),
                sep='\t',
                names=['place_id', 'latitude', 'longitude', 'sandcontent', 'siltcontent',
                       'claycontent', 'phwater', 'cationexchangecapacity',
                       'coarsefragmentcontent',
                       'calciumcarbonatecontent', 'organicmattercontent', 'bulkdensity',
                       'rootdepthconstraint',
                       'availablewateratfieldcapacity', 'hydraulicconductivityatsaturation'])
            print('soil_grid_df shape: ', soil_grid_df.shape)

            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Soy_NA_Summer/BaseMap_Grid_10km_NA_Soy_2024.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            grid_10km_df = grid_10km_df.rename(columns={'Market': 'Market_Segment'})
            grid_10km_df = grid_10km_df.rename(columns={'RM_num': 'Market'})
            grid_10km_df['Market'] = grid_10km_df['Market']
            maturity_group = int(maturity_group)
            # grid_10km_df = grid_10km_df.drop(columns=["LATITUDE", "LONGITUDE", "Core_North_South"])

        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER':
            cropFact_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                        'Grid_10km_Corn_Brazil_Summer/Corn-Brazil-Summer-EnvChar-CropFact-Corngrain-Features-2009ToPresent-Annual-LATAM-10km-SYN-v1 (2).csv'))
            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            cropFact_grid_df = cropFact_grid_df.drop(columns=['gid'])
            cropFact_grid_df = cropFact_grid_df.rename(columns={'geom': 'geometry'})
            cropFact_grid_df['timestamp'] = cropFact_grid_df['timestamp'].str.replace('Planting', 'P')
            cropFact_grid_df['timestamp'] = cropFact_grid_df['timestamp'].str.replace('Harvest', 'H')

            tpp7_cropFact_grid_df = pd.read_parquet(os.path.join(args.s3_input_corn_cropfact_grid_folder,
                                                                 'CORN_BRAZIL_SUMMER/corn_cropfact_grid_prep.parquet'))
            print('tpp7_cropFact_grid_df shape: ', tpp7_cropFact_grid_df.shape)

            tpp7_cropFact_grid_df['FID'] = 'FID'
            tpp7_cropFact_grid_df['row_id'] = 'row_id'
            tpp7_cropFact_grid_df['grid_id'] = 'grid_id'
            tpp7_cropFact_grid_df['start_date'] = 'start_date'
            tpp7_cropFact_grid_df['end_date'] = 'end_date'

            tpp7_cropFact_grid_df['irrigation'] = 'NON-IRRIGATED'

            tpp7_cropFact_grid_df['harvest_y'] = pd.DatetimeIndex(tpp7_cropFact_grid_df['harvest_y']).year
            tpp7_cropFact_grid_df['timestamp'] = tpp7_cropFact_grid_df['timestamp'].str.replace('_', '-')
            tpp7_cropFact_grid_df = tpp7_cropFact_grid_df.drop(
                columns=['crop_n', 'season_n', 'plantingdate',
                         'grainmoisturecontent', 'grainyield',
                         'floweringstagecode', 'management_season', 'management_planting_date',
                         'management_harvest_date', 'management_irrigation_status',
                         'management_drainagesystem_status'])

            tpp7_cropFact_grid_df = tpp7_cropFact_grid_df.rename(
                columns={'irrigation': 'watermgmt', 'geom': 'geometry'})

            cropFact_grid_df = pd.concat([cropFact_grid_df, tpp7_cropFact_grid_df], ignore_index=True)

            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corn_Brazil_Summer/BaseMap_Grid_10km_Brazil_Corn_Summer_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f', 'latitude', 'longitude',
                         'market_seg', 'Grid_area', 'value_ratio', 'Market_Area', 'Market_Volume'])

            grid_10km_df = grid_10km_df.rename(columns={'RM_BAND': 'Market', "Market_Value": 'Market_Pct'})

            maturity_group = int(maturity_group)

            soil_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corn_Brazil_Summer/data-mart-smartmart_run_1_stmt_1_0_lon_less_neg23_lat_less_18.csv'))
            print('soil_grid_df shape: ', soil_grid_df.shape)

        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            cropFact_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                        'Grid_10km_Corn_Brazil_Safrinha/Corn-Brazil-Safrinha-EnvChar-CropFact-Corngrain-Features-2009ToPresent-Annual-LATAM-10km-SYN-v1.csv'))
            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            cropFact_grid_df = cropFact_grid_df.drop(columns=['gid'])
            cropFact_grid_df = cropFact_grid_df.rename(columns={'geom': 'geometry'})
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corn_Brazil_Safrinha/BaseMap_Grid_10km_Brazil_Corn_Safrinha_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f', 'latitude', 'longitude',
                         'market_seg', 'Grid_area', 'value_ratio', 'Market_Area', 'Market_Volume'])

            grid_10km_df = grid_10km_df.rename(columns={'RM_BAND': 'Market', "Market_Value": 'Market_Pct'})

            maturity_group = int(maturity_group)

            soil_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corn_Brazil_Safrinha/data-mart-smartmart_run_1_stmt_1_0_lon_less_neg23_lat_less_18.csv'))
            print('soil_grid_df shape: ', soil_grid_df.shape)

        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
            cropFact_grid_df = pd.read_parquet(os.path.join(args.s3_input_grid_classification_folder,
                                                            'Grid_10km_Corngrain_EAME_Summer/corngrain_full_file.parquet'))
            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            cropFact_grid_df = cropFact_grid_df.rename(columns={'geom': 'geometry'})
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corngrain_EAME_Summer/eame_market_based_on_cropland_5.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            grid_10km_df = grid_10km_df[grid_10km_df.crop == 'grain']
            grid_10km_df['lat_i'], grid_10km_df['lat_d'] = grid_10km_df['latitude'].divmod(1)
            grid_10km_df[['lat_i', 'lat_d']] = grid_10km_df[['lat_i', 'lat_d']].astype(str)
            grid_10km_df['lat_d'] = grid_10km_df['lat_d'].astype(float) * 10
            grid_10km_df['lat_d'] = grid_10km_df['lat_d'].round(0).astype(int)
            grid_10km_df['lat_i'] = grid_10km_df['lat_i'].astype(float).astype(int)
            grid_10km_df.lat_i = grid_10km_df.lat_i.map('{:04d}'.format)
            grid_10km_df.lat_d = grid_10km_df.lat_d.map('{:<05}'.format)
            grid_10km_df['lat_id'] = grid_10km_df['lat_i'] + '.' + grid_10km_df['lat_d']

            grid_10km_df['long_i'], grid_10km_df['long_d'] = grid_10km_df['longitude'].divmod(1)
            grid_10km_df[['long_i', 'long_d']] = grid_10km_df[['long_i', 'long_d']].astype(str)
            grid_10km_df['long_d'] = grid_10km_df['long_d'].astype(float) * 10
            grid_10km_df['long_d'] = grid_10km_df['long_d'].round(0).astype(int)
            grid_10km_df['long_i'] = grid_10km_df['long_i'].astype(float).astype(int)
            grid_10km_df.long_i = grid_10km_df.long_i.map('{:04d}'.format)
            grid_10km_df.long_d = grid_10km_df.long_d.map('{:<05}'.format)
            grid_10km_df['long_id'] = grid_10km_df['long_i'] + '.' + grid_10km_df['long_d']

            grid_10km_df['place_id'] = 'gridpoint_' + grid_10km_df['lat_id'] + '_' + grid_10km_df['long_id']
            grid_10km_df = grid_10km_df.drop(columns=['lat_i', 'lat_d', 'long_i', 'long_d', 'lat_id', 'long_id'])

            grid_10km_df = grid_10km_df.drop(columns=['FID', 'region'])

            grid_10km_df = grid_10km_df.rename(columns={'MaturityGroup': 'Market', "ha": 'Market_Pct',
                                                        'country_c3': 'country'})

            maturity_group = int(maturity_group)

            soil_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Corngrain_EAME_Summer/data-mart-smartmart_run_6_stmt_1_0.csv'))
            print('soil_grid_df shape: ', soil_grid_df.shape)

        if DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            cropFact_grid_df = pd.read_parquet(os.path.join(args.s3_input_grid_classification_folder,
                                                            'Grid_10km_Cornsilage_EAME_Summer/cornsilage_full_file.parquet'))
            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)
            cropFact_grid_df = cropFact_grid_df.rename(columns={'geom': 'geometry'})
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Cornsilage_EAME_Summer/eame_market_based_on_cropland_5.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            grid_10km_df = grid_10km_df[grid_10km_df.crop == 'silage']
            grid_10km_df['lat_i'], grid_10km_df['lat_d'] = grid_10km_df['latitude'].divmod(1)
            grid_10km_df[['lat_i', 'lat_d']] = grid_10km_df[['lat_i', 'lat_d']].astype(str)
            grid_10km_df['lat_d'] = grid_10km_df['lat_d'].astype(float) * 10
            grid_10km_df['lat_d'] = grid_10km_df['lat_d'].round(0).astype(int)
            grid_10km_df['lat_i'] = grid_10km_df['lat_i'].astype(float).astype(int)
            grid_10km_df.lat_i = grid_10km_df.lat_i.map('{:04d}'.format)
            grid_10km_df.lat_d = grid_10km_df.lat_d.map('{:<05}'.format)
            grid_10km_df['lat_id'] = grid_10km_df['lat_i'] + '.' + grid_10km_df['lat_d']

            grid_10km_df['long_i'], grid_10km_df['long_d'] = grid_10km_df['longitude'].divmod(1)
            grid_10km_df[['long_i', 'long_d']] = grid_10km_df[['long_i', 'long_d']].astype(str)
            grid_10km_df['long_d'] = grid_10km_df['long_d'].astype(float) * 10
            grid_10km_df['long_d'] = grid_10km_df['long_d'].round(0).astype(int)
            grid_10km_df['long_i'] = grid_10km_df['long_i'].astype(float).astype(int)
            grid_10km_df.long_i = grid_10km_df.long_i.map('{:04d}'.format)
            grid_10km_df.long_d = grid_10km_df.long_d.map('{:<05}'.format)
            grid_10km_df['long_id'] = grid_10km_df['long_i'] + '.' + grid_10km_df['long_d']

            grid_10km_df['place_id'] = 'gridpoint_' + grid_10km_df['lat_id'] + '_' + grid_10km_df['long_id']
            grid_10km_df = grid_10km_df.drop(columns=['lat_i', 'lat_d', 'long_i', 'long_d', 'lat_id', 'long_id'])

            grid_10km_df = grid_10km_df.drop(columns=['FID', 'region'])

            grid_10km_df = grid_10km_df.rename(columns={'MaturityGroup': 'Market', "ha": 'Market_Pct',
                                                        'country_c3': 'country'})

            maturity_group = int(maturity_group)

            soil_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Cornsilage_EAME_Summer/data-mart-smartmart_run_6_stmt_1_0.csv'))
            print('soil_grid_df shape: ', soil_grid_df.shape)

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            cropFact_grid_df = pd.read_parquet(os.path.join(args.s3_input_soy_cropfact_grid_folder,
                                                            'SOY_BRAZIL_SUMMER/soy_cropfact_grid_prep.parquet'))
            print('cropFact_grid_df shape: ', cropFact_grid_df.shape)

            cropFact_grid_df['FID'] = 'FID'
            cropFact_grid_df['row_id'] = 'row_id'
            cropFact_grid_df['grid_id'] = 'grid_id'
            cropFact_grid_df['start_date'] = 'start_date'
            cropFact_grid_df['end_date'] = 'end_date'

            cropFact_grid_df['irrigation'] = 'NON-IRRIGATED'

            cropFact_grid_df = cropFact_grid_df.drop(
                columns=['crop_n', 'season_n', 'plantingdate',
                         'grainmoisturecontent', 'grainyield',
                         'floweringstagecode', 'management_season', 'management_planting',
                         'management_harvest', 'management_irrigation',
                         'management_drainagesystem'])

            cropFact_grid_df = cropFact_grid_df.rename(
                columns={'harvest_y': 'year', 'irrigation': 'watermgmt', 'geom': 'geometry'})

            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Soy_Brazil_Summer/BaseMap_Grid_10km_Brazil_Soy_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f', 'latitude', 'longitude', 'mu_code',
                         'st_code', 'Grid_area', 'value_ratio', 'Market_Area', 'Market_Volume'])

            grid_10km_df = grid_10km_df.rename(columns={'Syngenta_Cycle': 'Market', "Market_Value": 'Market_Pct'})

            # maturity_group = int(maturity_group)

            soil_grid_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder,
                                                    'Grid_10km_Soy_Brazil_Summer/data-mart-smartmart_run_1_stmt_1_0_lon_less_neg23_lat_less_18.csv'))
            print('soil_grid_df shape: ', soil_grid_df.shape)

        # Grid_10km_RM_Density_Final = dataiku.Dataset("TRIALNETWORKDESIGNNACORN2023.Grid_10km_RM_Density_Final")
        # grid_10km_df = Grid_10km_RM_Density_Final.get_dataframe()

        # grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_classification_folder, 'Grid_10km_RM_Density_Final.csv'))
        # print('grid_10km_df shape: ', grid_10km_df.shape)

        # soil_grid = dataiku.Dataset("mio095_soil")
        # soil_grid_df = soil_grid.get_dataframe()

        # Compute recipe outputs from inputs
        # Drop unneeded rows/columns from both tables
        # grid_10km_df = grid_10km_df.drop(columns=["LATITUDE", "LONGITUDE", "Core_North_South"]) \
        #                    .loc[(grid_10km_df["Market_Pct"] >= 0.01) & (grid_10km_df['Market'] == maturity_group),
        #                :].drop_duplicates()

        grid_10km_df.info()
        print('grid_10km_df shape before filtering: ', grid_10km_df.shape)
        grid_10km_df = grid_10km_df.loc[
                       (grid_10km_df["Market_Pct"] >= 0.01) & (grid_10km_df['Market'] == maturity_group),
                       :].drop_duplicates()

        print('grid_10km_df shape after filtering: ', grid_10km_df.shape)

        cropFact_grid_df = cropFact_grid_df.drop(
            columns=["FID", "row_id", "grid_id", "start_date", "end_date", "ysolar", "ypot", "yatt",
                     "geometry"]).loc[(cropFact_grid_df.timestamp != "Harvest") & (cropFact_grid_df.timestamp.notna()),
                           :]

        # Rename variables & columns to be compatible with existing API pull format
        cropFact_grid_df["timestamp"] = cropFact_grid_df["timestamp"].str.replace("Harvest", "h")
        cropFact_grid_df["timestamp"] = cropFact_grid_df["timestamp"].str.replace("Planting", "p")
        cropFact_grid_df = cropFact_grid_df.rename(columns={"tt": "thermaltime",
                                                            "swd": "waterdeficit",
                                                            "swe": "waterlogging",
                                                            "scd": "cold",
                                                            "sht": "heat",
                                                            "sptr": "photothermalratio",
                                                            "svpd": "vaporpressuredeficit",
                                                            "harvest_y": "year"})

        # Perform pivot
        # print(cropFact_grid_df.head())
        cropFact_grid_df = pd.pivot_table(cropFact_grid_df,
                                          values=["thermaltime", "tempmin", "tempmax", "tempavg", "rhmin", "rhmax",
                                                  "rhavg", "precsum", "radsum", "parisum", "et0sum", "etpsum", "etasum",
                                                  "vpdmax", "wsmax", "waterdeficit", "waterlogging", "cold", "heat",
                                                  "vaporpressuredeficit", "photothermalratio"],
                                          index=["year", "place_id", "watermgmt"], columns=["timestamp"]).reset_index()
        cropFact_grid_df.columns = cropFact_grid_df.columns.map(''.join).str.lower()
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            cropFact_grid_df.columns = cropFact_grid_df.columns.str.replace('_', '-')
            cropFact_grid_df = cropFact_grid_df.rename(columns={"place-id": "place_id"})
        # print(cropFact_grid_df.head())

        # Join with feature filter, trial_grid & soil_grid information
        feature_keep_df.columns = feature_keep_df.columns.str.lower()
        print('cropFact_grid_df shape before merges: ', cropFact_grid_df.shape)

        cropFact_grid_df = cropFact_grid_df.reindex(columns=np.concatenate(
            (["year", "place_id", "watermgmt"], np.intersect1d(cropFact_grid_df.columns, feature_keep_df.columns)),
            axis=None)) \
            .merge(soil_grid_df, on="place_id", how="left") \
            .merge(grid_10km_df, on="place_id", how="inner")

        print('cropFact_grid_df shape after merges: ', cropFact_grid_df.shape)
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            # Drop rows with irrigation mismatches
            cropFact_grid_df = cropFact_grid_df.loc[((cropFact_grid_df["Irrigated_Pct"] >= .02) & (
                    cropFact_grid_df["watermgmt"] == "IRRIGATED")) |
                                                    ((cropFact_grid_df["Irrigated_Pct"] <= .98) & (
                                                            cropFact_grid_df["watermgmt"] == "NON-IRRIGATED")), :]

        # Add other required inputs
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            cropFact_grid_df["previous_crop_soy"] = 0
        else:
            cropFact_grid_df["previous_crop_corn"] = 0

        cropFact_grid_df["HAVPN"] = 84000
        cropFact_grid_df["irrflag"] = 2 * (cropFact_grid_df["watermgmt"] == "IRRIGATED")
        cropFact_grid_df["tileflag"] = 0

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            cropFact_grid_df["maturity_group"] = (cropFact_grid_df["RM_Band"] - 80) / 5
            cropFact_grid_df = cropFact_grid_df.rename(columns={"Market": "maturity",
                                                                'watermgmt': 'irrigation',
                                                                'Market_Acres': 'maturity_acres',
                                                                'Irrigated_Pct': 'irrigated_pct',
                                                                'Market_Pct': 'maturity_pct',
                                                                'WCE': 'wce',
                                                                'TPP': 'tpp',
                                                                'Market_Segment': 'market_segment',
                                                                'SubCCode': 'sub_c_code',
                                                                'SpatialUnitCode': 'spatial_unit_code'}) \
                .drop(columns=['RSTCD', 'tier', 'latitude', 'longitude'])

            dist_cols = ['precsum30dbp-p', 'precsump-ve',
                         'precsumr2-r4', 'precsumr4-r6', 'precsumr6-h', 'precsumv6-vt',
                         'precsumve-v6', 'precsumvt-r2', 'radsum30dbp-p', 'radsump-ve',
                         'radsumr2-r4', 'radsumr4-r6', 'radsumr6-h', 'radsumv6-vt',
                         'radsumve-v6', 'radsumvt-r2', 'rhavgr4-r6', 'rhmax30dbp-p',
                         'rhmaxp-ve', 'rhmaxr2-r4', 'rhmaxr4-r6', 'rhmaxr6-h', 'rhmaxv6-vt',
                         'rhmaxve-v6', 'rhmaxvt-r2', 'rhmin30dbp-p', 'rhminp-ve',
                         'rhminr2-r4', 'rhminr4-r6', 'rhminr6-h', 'rhminv6-vt',
                         'rhminve-v6', 'rhminvt-r2', 'tempavgr2-r4', 'tempavgr6-h',
                         'tempavgvt-r2', 'tempmax30dbp-p', 'tempmaxp-ve', 'tempmaxr2-r4',
                         'tempmaxr4-r6', 'tempmaxr6-h', 'tempmaxv6-vt', 'tempmaxve-v6',
                         'tempmaxvt-r2', 'tempmin30dbp-p', 'tempminp-ve', 'tempminr2-r4',
                         'tempminr4-r6', 'tempminr6-h', 'tempminv6-vt', 'tempminve-v6',
                         'tempminvt-r2', 'thermaltimep-ve', 'thermaltimer6-h']

        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            cropFact_grid_df = cropFact_grid_df.rename(columns={"Market": "maturity",
                                                                'watermgmt': 'irrigation',
                                                                'Market_Acres': 'maturity_acres',
                                                                'Irrigated_Pct': 'irrigated_pct',
                                                                'Market_Pct': 'maturity_pct',
                                                                'ECW': 'ecw',
                                                                'ACS': 'acs',
                                                                'Market_Segment': 'market_segment',
                                                                'SubCCode': 'sub_c_code',
                                                                'SpatialUnitCode': 'spatial_unit_code'}) \
                .drop(columns=['RSTCD', 'tier', 'latitude', 'longitude'])

            dist_cols = ['thermaltimep-ve', 'thermaltimeve-v5', 'thermaltimer1-r3',
                         'thermaltimer3-r6', 'thermaltimer6-r8', 'thermaltimer8-h',
                         'sandcontent', 'siltcontent', 'claycontent', 'phwater',
                         'cationexchangecapacity', 'coarsefragmentcontent',
                         'calciumcarbonatecontent', 'organicmattercontent', 'bulkdensity',
                         'rootdepthconstraint', 'availablewateratfieldcapacity',
                         'hydraulicconductivityatsaturation', 'tempmin30dbp-p',
                         'tempminp-ve', 'tempminve-v5', 'tempminv5-r1', 'tempminr1-r3',
                         'tempminr3-r6', 'tempminr6-r8', 'tempminr8-h', 'tempmaxp-ve',
                         'tempmaxve-v5', 'tempmaxv5-r1', 'tempmaxr1-r3', 'tempmaxr3-r6',
                         'tempmaxr6-r8', 'tempmaxr8-h', 'rhmin30dbp-p', 'rhminp-ve',
                         'rhminve-v5', 'rhminv5-r1', 'rhminr1-r3', 'rhminr3-r6',
                         'rhminr6-r8', 'rhminr8-h', 'rhmax30dbp-p', 'rhmaxp-ve',
                         'rhmaxve-v5', 'rhmaxv5-r1', 'rhmaxr1-r3', 'rhmaxr3-r6',
                         'rhmaxr6-r8', 'rhmaxr8-h', 'rhavgp-ve', 'rhavgv5-r1', 'rhavgr1-r3',
                         'rhavgr3-r6', 'rhavgr6-r8', 'rhavgr8-h', 'precsum30dbp-p',
                         'precsump-ve', 'precsumve-v5', 'precsumv5-r1', 'precsumr1-r3',
                         'precsumr3-r6', 'precsumr6-r8', 'precsumr8-h', 'radsum30dbp-p',
                         'radsump-ve', 'radsumve-v5', 'radsumv5-r1', 'radsumr1-r3',
                         'radsumr3-r6', 'radsumr6-r8', 'radsumr8-h']
        print('cropFact_grid_df shape before column rename: ', cropFact_grid_df.shape)

        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER':
            cropFact_grid_df = cropFact_grid_df.rename(columns={
                'watermgmt': 'irrigation',
                # 'Market_Acres': 'maturity_acres',
                'Market_Pct': 'maturity_pct',
            })

            dist_cols = ['precsum30dbp-p', 'precsump-ve',
                         'precsumr2-r4', 'precsumr4-r6', 'precsumr6-h', 'precsumv6-vt',
                         'precsumve-v6', 'precsumvt-r2', 'radsum30dbp-p', 'radsump-ve',
                         'radsumr2-r4', 'radsumr4-r6', 'radsumr6-h', 'radsumv6-vt',
                         'radsumve-v6', 'radsumvt-r2',
                         'rhavgr4-r6', 'rhmax30dbp-p',
                         'rhmaxp-ve', 'rhmaxr2-r4', 'rhmaxr4-r6', 'rhmaxr6-h', 'rhmaxv6-vt',
                         'rhmaxve-v6', 'rhmaxvt-r2', 'rhmin30dbp-p', 'rhminp-ve',
                         'rhminr2-r4', 'rhminr4-r6', 'rhminr6-h', 'rhminv6-vt',
                         'rhminve-v6', 'rhminvt-r2', 'tempavgr2-r4', 'tempavgr6-h',
                         'tempavgvt-r2', 'tempmax30dbp-p', 'tempmaxp-ve', 'tempmaxr2-r4',
                         'tempmaxr4-r6', 'tempmaxr6-h', 'tempmaxv6-vt', 'tempmaxve-v6',
                         'tempmaxvt-r2', 'tempmin30dbp-p', 'tempminp-ve', 'tempminr2-r4',
                         'tempminr4-r6', 'tempminr6-h', 'tempminv6-vt', 'tempminve-v6',
                         'tempminvt-r2', 'thermaltimep-ve', 'thermaltimer6-h']

        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            cropFact_grid_df = cropFact_grid_df.rename(columns={
                'watermgmt': 'irrigation',
                # 'Market_Acres': 'maturity_acres',
                'Market_Pct': 'maturity_pct',
            })

            dist_cols = ['thermaltimep-ve', 'thermaltimer6-h',
                         'tempmin30dbp-p', 'tempminp-ve', 'tempminve-v6', 'tempminv6-vt',
                         'tempminvt-r2', 'tempminr2-r4', 'tempminr4-r6', 'tempminr6-h',
                         'tempmax30dbp-p', 'tempmaxp-ve', 'tempmaxve-v6', 'tempmaxv6-vt',
                         'tempmaxvt-r2', 'tempmaxr2-r4', 'tempmaxr4-r6', 'tempmaxr6-h',
                         'tempavgp-ve', 'tempavgve-v6', 'tempavgv6-vt', 'rhmin30dbp-p',
                         'rhminp-ve', 'rhminve-v6', 'rhminv6-vt', 'rhminvt-r2', 'rhminr2-r4',
                         'rhminr4-r6', 'rhminr6-h', 'rhmax30dbp-p', 'rhmaxp-ve', 'rhmaxve-v6',
                         'rhmaxv6-vt', 'rhmaxvt-r2', 'rhmaxr2-r4', 'rhmaxr4-r6', 'rhmaxr6-h',
                         'rhavgp-ve', 'precsum30dbp-p', 'precsump-ve', 'precsumve-v6',
                         'precsumv6-vt', 'precsumvt-r2', 'precsumr2-r4', 'precsumr4-r6',
                         'precsumr6-h', 'radsum30dbp-p', 'radsump-ve', 'radsumve-v6',
                         'radsumv6-vt', 'radsumvt-r2', 'radsumr2-r4', 'radsumr4-r6']

        if DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            cropFact_grid_df = cropFact_grid_df.rename(columns={
                'watermgmt': 'irrigation',
                # 'Market_Acres': 'maturity_acres',
                'Market_Pct': 'maturity_pct',
            })

            dist_cols = ['tempmin30dbp-p', 'thermaltimep-ve',
                         'thermaltimeve-v6', 'thermaltimev6-vt', 'thermaltimevt-r2',
                         'thermaltimer2-r4', 'thermaltimer4-r5.5', 'thermaltimer5.5-h',
                         'tempminp-ve', 'tempminve-v6', 'tempminv6-vt',
                         'tempminvt-r2', 'tempminr2-r4', 'tempminr4-r5.5', 'tempminr5.5-h',
                         'tempmax30dbp-p', 'tempmaxp-ve', 'tempmaxve-v6', 'tempmaxv6-vt',
                         'tempmaxvt-r2', 'tempmaxr2-r4', 'tempmaxr4-r5.5', 'tempmaxr5.5-h',
                         'tempavg30dbp-p', 'tempavgp-ve', 'tempavgve-v6', 'tempavgv6-vt',
                         'tempavgvt-r2', 'tempavgr2-r4', 'tempavgr4-r5.5', 'tempavgr5.5-h',
                         'rhmin30dbp-p', 'rhminp-ve', 'rhminve-v6', 'rhminv6-vt',
                         'rhminvt-r2', 'rhminr2-r4', 'rhminr4-r5.5', 'rhminr5.5-h',
                         'rhmax30dbp-p', 'rhmaxp-ve', 'rhmaxve-v6', 'rhmaxv6-vt',
                         'rhmaxvt-r2', 'rhmaxr2-r4', 'rhmaxr4-r5.5', 'rhmaxr5.5-h',
                         'rhavg30dbp-p', 'rhavgp-ve', 'rhavgve-v6', 'rhavgv6-vt',
                         'rhavgvt-r2', 'rhavgr2-r4', 'rhavgr4-r5.5', 'rhavgr5.5-h',
                         'precsum30dbp-p', 'precsump-ve', 'precsumve-v6', 'precsumv6-vt',
                         'precsumvt-r2', 'precsumr2-r4', 'precsumr4-r5.5', 'precsumr5.5-h',
                         'radsum30dbp-p', 'radsump-ve', 'radsumve-v6', 'radsumv6-vt',
                         'radsumvt-r2', 'radsumr2-r4', 'radsumr4-r5.5', 'radsumr5.5-h',
                         'parisump-ve', 'parisumve-v6', 'parisumv6-vt', 'parisumvt-r2',
                         'parisumr2-r4', 'parisumr4-r5.5', 'parisumr5.5-h', 'et0sum30dbp-p',
                         'et0sump-ve', 'et0sumve-v6', 'et0sumv6-vt', 'et0sumvt-r2',
                         'et0sumr2-r4', 'et0sumr4-r5.5', 'et0sumr5.5-h', 'etpsum30dbp-p',
                         'etpsump-ve', 'etpsumve-v6', 'etpsumv6-vt', 'etpsumvt-r2',
                         'etpsumr2-r4', 'etpsumr4-r5.5', 'etpsumr5.5-h', 'etasum30dbp-p',
                         'etasump-ve', 'etasumve-v6', 'etasumv6-vt', 'etasumvt-r2',
                         'etasumr2-r4', 'etasumr4-r5.5', 'etasumr5.5-h', 'vpdmax30dbp-p',
                         'vpdmaxp-ve', 'vpdmaxve-v6', 'vpdmaxv6-vt', 'vpdmaxvt-r2',
                         'vpdmaxr2-r4', 'vpdmaxr4-r5.5', 'vpdmaxr5.5-h', 'wsmax30dbp-p',
                         'wsmaxp-ve', 'wsmaxve-v6', 'wsmaxv6-vt', 'wsmaxvt-r2',
                         'wsmaxr2-r4', 'wsmaxr4-r5.5', 'wsmaxr5.5-h']

        if DKU_DST_ap_data_sector == "CORNGRAIN_EAME_SUMMER":
            cropFact_grid_df = cropFact_grid_df.rename(columns={
                'watermgmt': 'irrigation',
                # 'Market_Acres': 'maturity_acres',
                'Market_Pct': 'maturity_pct',
            })

            dist_cols = ['thermaltimep-ve',
                         'thermaltimeve-v6', 'thermaltimev6-vt', 'thermaltimevt-r2',
                         'thermaltimer2-r4', 'thermaltimer4-r6', 'thermaltimer6-h']

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            cropFact_grid_df = cropFact_grid_df.rename(columns={
                'watermgmt': 'irrigation',
                'Market_Acres': 'maturity_acres',
                'Market_Pct': 'maturity_pct',
                'REC': 'rec',
                'TPP': 'tpp',
            })

            dist_cols = ['precsum30dbp-p', 'precsump-ve',
                         'precsumr1-r3', 'precsumr3-r6', 'precsumr6-r8', 'precsumr8-h',
                         'precsumv5-r1', 'precsumve-v5', 'radsum30dbp-p', 'radsump-ve',
                         'radsumr1-r3', 'radsumr3-r6', 'radsumr6-r8', 'radsumr8-h',
                         'radsumv5-r1', 'radsumve-v5', 'rhmax30dbp-p', 'rhmaxp-ve',
                         'rhmaxr1-r3', 'rhmaxr3-r6', 'rhmaxr6-r8', 'rhmaxr8-h',
                         'rhmaxv5-r1', 'rhmaxve-v5', 'rhmin30dbp-p', 'rhminp-ve',
                         'rhminr1-r3', 'rhminr3-r6', 'rhminr6-r8', 'rhminr8-h',
                         'rhminv5-r1', 'rhminve-v5', 'tempavgp-ve', 'tempavgr1-r3',
                         'tempavgr3-r6', 'tempavgr6-r8', 'tempavgr8-h', 'tempavgv5-r1',
                         'tempavgve-v5', 'tempmax30dbp-p', 'tempmaxp-ve', 'tempmaxr1-r3',
                         'tempmaxr3-r6', 'tempmaxr6-r8', 'tempmaxr8-h', 'tempmaxv5-r1',
                         'tempmaxve-v5', 'tempmin30dbp-p', 'tempminp-ve', 'tempminr1-r3',
                         'tempminr3-r6', 'tempminr6-r8', 'tempminr8-h', 'tempminv5-r1',
                         'tempminve-v5', 'thermaltimep-ve', 'thermaltimer1-r3',
                         'thermaltimer3-r6', 'thermaltimer6-r8', 'thermaltimer8-h',
                         'thermaltimev5-r1', 'thermaltimeve-v5']

        print('cropFact_grid_df shape before output: ', cropFact_grid_df.shape)
        # Write recipe outputs
        maturity_group = str(maturity_group)
        cropFact_grid_for_model_dir_path = os.path.join('/opt/ml/processing/data/cropFact_grid_for_model',
                                                        DKU_DST_ap_data_sector, maturity_group)
        cropFact_grid_for_model_data_path = os.path.join(cropFact_grid_for_model_dir_path,
                                                         'cropFact_grid_for_model.parquet')
        print('cropFact_grid_for_model_data_path: ', cropFact_grid_for_model_data_path)
        isExist = os.path.exists(cropFact_grid_for_model_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(cropFact_grid_for_model_dir_path)

        cropFact_grid_df[['10km_distance', '10km_p_value', '10km_outlier_ind']] = None
        # cropFact_grid_df[['10km_distance', '10km_p_value', '10km_outlier_ind']] = mal_distance_calc(cropFact_grid_df,
        #            cropFact_grid_df,
        #            dist_cols)
        print('Outlier percentage: ', round(cropFact_grid_df['10km_outlier_ind'].mean() * 100, 2))

        cropFact_grid_df[['place_id_name', 'lat', 'lon']] = cropFact_grid_df['place_id'].str.split('_', expand=True)
        cropFact_grid_df['lat'] = cropFact_grid_df['lat'].astype(float)
        cropFact_grid_df['lon'] = cropFact_grid_df['lon'].astype(float)
        print(cropFact_grid_df.shape)

        trial_df = pd.read_parquet(os.path.join(args.s3_input_trial_data_all_years_folder, DKU_DST_ap_data_sector,
                                                'trial_data_all_years.parquet'))[
            ['cropfact_id', 'x_longitude', 'y_latitude']]
        print(trial_df.shape)

        cropfact_df_all_years_with_loc = cropfact_df_all_years.merge(trial_df, on='cropfact_id',
                                                                     how='inner').drop_duplicates()
        print(cropfact_df_all_years_with_loc.shape)
        cropfact_df_all_years_with_loc[['lat', 'lon']] = cropfact_df_all_years_with_loc[['y_latitude', 'x_longitude']]

        # years = cropfact_df_all_years_with_loc.year.unique().tolist()
        # fig, axs = plt.subplots(len(years), figsize=(12, 20), sharex=True, sharey=True)
        # for yr in years:
        # matrix = cropfact_df_all_years_with_loc[cropfact_df_all_years_with_loc.year == yr][dist_cols]
        matrix = cropfact_df_all_years_with_loc[dist_cols]
        matrix = matrix.fillna(matrix.mean())
        nnd = pynndescent.NNDescent(matrix, metric='euclidean')
        nnd.prepare()
        # print(cropFact_grid_df[cropFact_grid_df.year == yr].shape)
        k = 3
        # indices, distances = nnd.query(cropFact_grid_df[cropFact_grid_df.year == yr][dist_cols], k=k)
        indices, distances = nnd.query(cropFact_grid_df[dist_cols], k=k)

        # aa = np.hstack([*distances])
        # y, x = np.histogram(aa, bins= range(0,5000,50))
        # axs[i].plot(x[:-1], y)
        # axs[i].set_title(yr)

        distances_bool = distances > 2000

        arr = distances_bool.any(axis=1)
        arr_length = len(arr)
        # print('year: ', yr)
        print("Length of the array using len:", arr_length)
        outliers_n = np.sum(arr)
        print("Number of outliers:", outliers_n)
        print('Outlier percentage: ', outliers_n / arr_length * 100)
        print()
        cropFact_grid_df['env_input_distance'] = None
        cropFact_grid_df['env_input_p_value'] = None
        # cropFact_grid_df['env_input_outlier_ind'] = False
        # cropFact_grid_df.loc[cropFact_grid_df.year == yr, 'env_input_outlier_ind'] = arr
        cropFact_grid_df['env_input_outlier_ind'] = arr
        cropFact_grid_df.head()

        print(cropFact_grid_df['env_input_outlier_ind'].value_counts(dropna=False))

        # cropFact_grid_df[['env_input_distance', 'env_input_p_value', 'env_input_outlier_ind']] = mal_distance_calc(
        #    cropfact_df_all_years, cropFact_grid_df, dist_cols)
        print('Outlier percentage: ', round(cropFact_grid_df['env_input_outlier_ind'].mean() * 100, 2))

        # cropFact_grid_df[['env_input_distance', 'env_input_p_value', 'env_input_outlier_ind']] = mal_distance_calc_knn(
        #     cropfact_df_all_years_with_loc, cropFact_grid_df, dist_cols, 3)
        # print('Outlier percentage: ', round(cropFact_grid_df['env_input_outlier_ind'].mean() * 100, 2))

        # cropFact_grid_for_model_data_path = os.path.join('/opt/ml/processing/data', 'cropFact_grid_for_model.parquet')
        cropFact_grid_df.to_parquet(cropFact_grid_for_model_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, maturity_group, pipeline_runid, str(e))
        message = f'Placement cropFact_grid_for_model error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {maturity_group}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_grid_classification_folder', type=str,
                        help='s3 input grid classification folder', required=True)
    parser.add_argument('--s3_input_trial_feature_keep_array_folder', type=str,
                        help='s3 input trial feature keep array folder', required=True)
    parser.add_argument('--s3_input_soy_cropfact_grid_folder', type=str,
                        help='s3 input soy cropfact grid folder', required=True)
    parser.add_argument('--s3_input_corn_cropfact_grid_folder', type=str,
                        help='s3 input corn cropfact grid folder', required=True)
    parser.add_argument('--s3_input_trial_data_all_years_folder', type=str,
                        help='s3 input trial data all years folder', required=True)
    args = parser.parse_args()
    print('args collected')

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        DKU_DST_analysis_year = data['analysis_year']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            maturity_list = ['MG0', 'MG1', 'MG2', 'MG3', 'MG4', 'MG5', 'MG6', 'MG7', 'MG8']
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            asec_df = pd.read_parquet(
                os.path.join(args.s3_input_grid_classification_folder, 'grid_classification', DKU_DST_ap_data_sector,
                             DKU_DST_analysis_year,
                             'asec_df_subset.parquet'))
            maturity_list = asec_df.decision_group_rm.dropna().unique().astype(int).astype(str).tolist()
            print('maturity_list: ', maturity_list)
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            maturity_list = ['6C', '6L', '6M', '7C', '7L', '7M', '8C', '5L', '8M', '5M', '5C', '4L']
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7']
        # if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
        #     maturity_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8']

        for maturity in maturity_list:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT),
                                     'compute_cropFact_grid_for_model/data/cropFact_grid_for_model',
                                     DKU_DST_ap_data_sector,
                                     maturity, 'cropFact_grid_for_model.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:

            print('Creating file in the following location: ', file_path)
            cropFact_grid_for_model_function(DKU_DST_ap_data_sector, maturity, pipeline_runid, args=args, logger=logger)
            print('File created')
            print()

            # else:
            # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
