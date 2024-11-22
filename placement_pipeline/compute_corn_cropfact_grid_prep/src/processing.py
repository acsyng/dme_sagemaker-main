import json
import os

import boto3
import pandas as pd

from libs.event_bridge.event import error_event
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']

        cols_of_interest = ['Trait_ThermalTime_Sum_P_VE',
                            'Trait_ThermalTime_Sum_VE_V6', 'Trait_ThermalTime_Sum_V6_VT',
                            'Trait_ThermalTime_Sum_VT_R2', 'Trait_ThermalTime_Sum_R2_R4',
                            'Trait_ThermalTime_Sum_R4_R6', 'Trait_ThermalTime_Sum_R6_H',
                            'Condition_Temperature_200cm_Min_30DBP_P',
                            'Condition_Temperature_200cm_Min_P_VE',
                            'Condition_Temperature_200cm_Min_VE_V6',
                            'Condition_Temperature_200cm_Min_V6_VT',
                            'Condition_Temperature_200cm_Min_VT_R2',
                            'Condition_Temperature_200cm_Min_R2_R4',
                            'Condition_Temperature_200cm_Min_R4_R6',
                            'Condition_Temperature_200cm_Min_R6_H',
                            'Condition_Temperature_200cm_Max_30DBP_P',
                            'Condition_Temperature_200cm_Max_P_VE',
                            'Condition_Temperature_200cm_Max_VE_V6',
                            'Condition_Temperature_200cm_Max_V6_VT',
                            'Condition_Temperature_200cm_Max_VT_R2',
                            'Condition_Temperature_200cm_Max_R2_R4',
                            'Condition_Temperature_200cm_Max_R4_R6',
                            'Condition_Temperature_200cm_Max_R6_H',
                            'Condition_Temperature_200cm_Avg_30DBP_P',
                            'Condition_Temperature_200cm_Avg_P_VE',
                            'Condition_Temperature_200cm_Avg_VE_V6',
                            'Condition_Temperature_200cm_Avg_V6_VT',
                            'Condition_Temperature_200cm_Avg_VT_R2',
                            'Condition_Temperature_200cm_Avg_R2_R4',
                            'Condition_Temperature_200cm_Avg_R4_R6',
                            'Condition_Temperature_200cm_Avg_R6_H',
                            'Condition_RelativeHumidity_200cm_Min_30DBP_P',
                            'Condition_RelativeHumidity_200cm_Min_P_VE',
                            'Condition_RelativeHumidity_200cm_Min_VE_V6',
                            'Condition_RelativeHumidity_200cm_Min_V6_VT',
                            'Condition_RelativeHumidity_200cm_Min_VT_R2',
                            'Condition_RelativeHumidity_200cm_Min_R2_R4',
                            'Condition_RelativeHumidity_200cm_Min_R4_R6',
                            'Condition_RelativeHumidity_200cm_Min_R6_H',
                            'Condition_RelativeHumidity_200cm_Max_30DBP_P',
                            'Condition_RelativeHumidity_200cm_Max_P_VE',
                            'Condition_RelativeHumidity_200cm_Max_VE_V6',
                            'Condition_RelativeHumidity_200cm_Max_V6_VT',
                            'Condition_RelativeHumidity_200cm_Max_VT_R2',
                            'Condition_RelativeHumidity_200cm_Max_R2_R4',
                            'Condition_RelativeHumidity_200cm_Max_R4_R6',
                            'Condition_RelativeHumidity_200cm_Max_R6_H',
                            'Condition_RelativeHumidity_200cm_Avg_30DBP_P',
                            'Condition_RelativeHumidity_200cm_Avg_P_VE',
                            'Condition_RelativeHumidity_200cm_Avg_VE_V6',
                            'Condition_RelativeHumidity_200cm_Avg_V6_VT',
                            'Condition_RelativeHumidity_200cm_Avg_VT_R2',
                            'Condition_RelativeHumidity_200cm_Avg_R2_R4',
                            'Condition_RelativeHumidity_200cm_Avg_R4_R6',
                            'Condition_RelativeHumidity_200cm_Avg_R6_H',
                            'Condition_Precipitation_Surface_Sum_30DBP_P',
                            'Condition_Precipitation_Surface_Sum_P_VE',
                            'Condition_Precipitation_Surface_Sum_VE_V6',
                            'Condition_Precipitation_Surface_Sum_V6_VT',
                            'Condition_Precipitation_Surface_Sum_VT_R2',
                            'Condition_Precipitation_Surface_Sum_R2_R4',
                            'Condition_Precipitation_Surface_Sum_R4_R6',
                            'Condition_Precipitation_Surface_Sum_R6_H',
                            'Condition_ShortwaveRadiation_Surface_Sum_30DBP_P',
                            'Condition_ShortwaveRadiation_Surface_Sum_P_VE',
                            'Condition_ShortwaveRadiation_Surface_Sum_VE_V6',
                            'Condition_ShortwaveRadiation_Surface_Sum_V6_VT',
                            'Condition_ShortwaveRadiation_Surface_Sum_VT_R2',
                            'Condition_ShortwaveRadiation_Surface_Sum_R2_R4',
                            'Condition_ShortwaveRadiation_Surface_Sum_R4_R6',
                            'Condition_ShortwaveRadiation_Surface_Sum_R6_H',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_30DBP_P',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_P_VE',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_VE_V6',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_V6_VT',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_VT_R2',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_R2_R4',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_R4_R6',
                            'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum_R6_H',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_30DBP_P',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_P_VE',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_VE_V6',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_V6_VT',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_VT_R2',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_R2_R4',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_R4_R6',
                            'Condition_ReferenceEvapotranspiration_200cm_Sum_R6_H',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_30DBP_P',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_P_VE',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_VE_V6',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_V6_VT',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_VT_R2',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_R2_R4',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_R4_R6',
                            'Condition_CropEvapotranspirationPotential_200cm_Sum_R6_H',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_30DBP_P',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_P_VE',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_VE_V6',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_V6_VT',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_VT_R2',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_R2_R4',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_R4_R6',
                            'Condition_CropEvapotranspirationActual_200cm_Sum_R6_H',
                            'Condition_VaporPressureDeficit_200cm_Max_30DBP_P',
                            'Condition_VaporPressureDeficit_200cm_Max_P_VE',
                            'Condition_VaporPressureDeficit_200cm_Max_VE_V6',
                            'Condition_VaporPressureDeficit_200cm_Max_V6_VT',
                            'Condition_VaporPressureDeficit_200cm_Max_VT_R2',
                            'Condition_VaporPressureDeficit_200cm_Max_R2_R4',
                            'Condition_VaporPressureDeficit_200cm_Max_R4_R6',
                            'Condition_VaporPressureDeficit_200cm_Max_R6_H',
                            'Condition_WindSpeed_200cm_Max_30DBP_P',
                            'Condition_WindSpeed_200cm_Max_P_VE',
                            'Condition_WindSpeed_200cm_Max_VE_V6',
                            'Condition_WindSpeed_200cm_Max_V6_VT',
                            'Condition_WindSpeed_200cm_Max_VT_R2',
                            'Condition_WindSpeed_200cm_Max_R2_R4',
                            'Condition_WindSpeed_200cm_Max_R4_R6',
                            'Condition_WindSpeed_200cm_Max_R6_H',
                            'Stress_WaterDeficit_RootZone_Avg_30DBP_P',
                            'Stress_WaterDeficit_RootZone_Avg_P_VE',
                            'Stress_WaterDeficit_RootZone_Avg_VE_V6',
                            'Stress_WaterDeficit_RootZone_Avg_V6_VT',
                            'Stress_WaterDeficit_RootZone_Avg_VT_R2',
                            'Stress_WaterDeficit_RootZone_Avg_R2_R4',
                            'Stress_WaterDeficit_RootZone_Avg_R4_R6',
                            'Stress_WaterDeficit_RootZone_Avg_R6_H',
                            'Stress_WaterLogging_RootZone_Avg_30DBP_P',
                            'Stress_WaterLogging_RootZone_Avg_P_VE',
                            'Stress_WaterLogging_RootZone_Avg_VE_V6',
                            'Stress_WaterLogging_RootZone_Avg_V6_VT',
                            'Stress_WaterLogging_RootZone_Avg_VT_R2',
                            'Stress_WaterLogging_RootZone_Avg_R2_R4',
                            'Stress_WaterLogging_RootZone_Avg_R4_R6',
                            'Stress_WaterLogging_RootZone_Avg_R6_H',
                            'Stress_Cold_200cm_Avg_30DBP_P', 'Stress_Cold_200cm_Avg_P_VE',
                            'Stress_Cold_200cm_Avg_VE_V6', 'Stress_Cold_200cm_Avg_V6_VT',
                            'Stress_Cold_200cm_Avg_VT_R2', 'Stress_Cold_200cm_Avg_R2_R4',
                            'Stress_Cold_200cm_Avg_R4_R6', 'Stress_Cold_200cm_Avg_R6_H',
                            'Stress_Heat_200cm_Avg_30DBP_P', 'Stress_Heat_200cm_Avg_P_VE',
                            'Stress_Heat_200cm_Avg_VE_V6', 'Stress_Heat_200cm_Avg_V6_VT',
                            'Stress_Heat_200cm_Avg_VT_R2', 'Stress_Heat_200cm_Avg_R2_R4',
                            'Stress_Heat_200cm_Avg_R4_R6', 'Stress_Heat_200cm_Avg_R6_H',
                            'Stress_VaporPressureDeficit_200cm_Avg_30DBP_P',
                            'Stress_VaporPressureDeficit_200cm_Avg_P_VE',
                            'Stress_VaporPressureDeficit_200cm_Avg_VE_V6',
                            'Stress_VaporPressureDeficit_200cm_Avg_V6_VT',
                            'Stress_VaporPressureDeficit_200cm_Avg_VT_R2',
                            'Stress_VaporPressureDeficit_200cm_Avg_R2_R4',
                            'Stress_VaporPressureDeficit_200cm_Avg_R4_R6',
                            'Stress_VaporPressureDeficit_200cm_Avg_R6_H',
                            'Stress_PhotoThermalRatio_200cm_Avg_30DBP_P',
                            'Stress_PhotoThermalRatio_200cm_Avg_P_VE',
                            'Stress_PhotoThermalRatio_200cm_Avg_VE_V6',
                            'Stress_PhotoThermalRatio_200cm_Avg_V6_VT',
                            'Stress_PhotoThermalRatio_200cm_Avg_VT_R2',
                            'Stress_PhotoThermalRatio_200cm_Avg_R2_R4',
                            'Stress_PhotoThermalRatio_200cm_Avg_R4_R6',
                            'Stress_PhotoThermalRatio_200cm_Avg_R6_H']

        id_cols = ['GlobalID', 'CropName', 'SeasonName', 'Longitude', 'Latitude',
                   'PlantingDate', 'FloweringDate', 'HarvestDate',
                   'GrainMoistureContent', 'Irrigation', 'GrainYield',
                   'FloweringStageCode', 'Management_Season',
                   'Management_Planting_Date', 'Management_Harvest_Date',
                   'Management_Irrigation_Status', 'Management_DrainageSystem_Status']

        cols_of_interest_df = pd.DataFrame(cols_of_interest, columns=['name'])
        cols_of_interest_df['sub'] = cols_of_interest_df['name'].str.split('_').str[:-2].str.join('_')
        cols_of_interest_df_sub = cols_of_interest_df['sub'].unique().tolist() + ['Trait_GrainYieldSolarPotential',
                                                                                  'Trait_GrainYieldPotential',
                                                                                  'Trait_GrainYieldAttainable']

        try:
            s3 = boto3.resource('s3')
            # bucket_name = 'us.com.syngenta.ap.nonprod'
            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)
            all_df = pd.DataFrame()
            for obj in bucket.objects.filter(Prefix=os.path.join(get_s3_prefix(ENVIRONMENT),
                                                                 'compute_grid_classification/data/corn_cropfact_grid_data_files',
                                                                 '')):
                if '.txt' in obj.key:
                    print(obj.key)
                    df = pd.read_csv(os.path.join('s3://', bucket_name, obj.key), delimiter="\t")

                    df.columns = df.columns.str.replace('_Value', '')
                    print('df shape: ', df.shape)
                    df1 = df[id_cols + ['Trait_GrainYieldSolarPotential_Harvest',
                                        'Trait_GrainYieldPotential_Harvest',
                                        'Trait_GrainYieldAttainable_Harvest'] + cols_of_interest]

                    print(df1.shape)

                    df1_long = pd.wide_to_long(df1, stubnames=cols_of_interest_df_sub, i=id_cols, j='timestamp',
                                               suffix=r'\w+', sep="_").reset_index()
                    print(df1_long.shape)
                    print(df1_long.timestamp.unique().tolist())
                    df1_long = df1_long.rename(
                        columns={'GlobalID': "place_id", "CropName": "crop_n", 'SeasonName': "season_n",
                                 "HarvestDate": "harvest_y", "watermgmt": "Management_Irrigation",
                                 "Trait_ThermalTime_Sum": 'tt',
                                 "Condition_Temperature_200cm_Min": "TEMPMIN",
                                 "Condition_Temperature_200cm_Max": "TEMPMAX",
                                 "Condition_Temperature_200cm_Avg": "TEMPAVG",
                                 "Condition_RelativeHumidity_200cm_Min": "RHMIN",
                                 "Condition_RelativeHumidity_200cm_Max": "RHMAX",
                                 "Condition_RelativeHumidity_200cm_Avg": "RHAVG",
                                 'Condition_Precipitation_Surface_Sum': "PRECSUM",
                                 'Condition_ShortwaveRadiation_Surface_Sum': "RADSUM",
                                 'Condition_PhotosyntheticallyActiveRadiationIntercepted_Surface_Sum': "PARISUM",
                                 'Condition_ReferenceEvapotranspiration_200cm_Sum': "ET0SUM",
                                 'Condition_CropEvapotranspirationPotential_200cm_Sum': "ETPSUM",
                                 'Condition_CropEvapotranspirationActual_200cm_Sum': "ETASUM",
                                 'Condition_VaporPressureDeficit_200cm_Max': "VPDMAX",
                                 'Condition_WindSpeed_200cm_Max': "WSMAX",
                                 'Stress_WaterDeficit_RootZone_Avg': "SWD",
                                 'Stress_WaterLogging_RootZone_Avg': "SWE",
                                 'Stress_Cold_200cm_Avg': "SCD",
                                 'Stress_Heat_200cm_Avg': "SHT",
                                 'Stress_VaporPressureDeficit_200cm_Avg': "SVPD",
                                 'Stress_PhotoThermalRatio_200cm_Avg': "SPTR",
                                 'Trait_GrainYieldSolarPotential': "YSOLAR",
                                 'Trait_GrainYieldPotential': "YPOT",
                                 'Trait_GrainYieldAttainable': "YATT"
                                 })

                    df1_long.columns = df1_long.columns.str.lower()
                    df1_long['geom'] = 'geom'
                    df1_long = df1_long.drop(columns=['longitude', 'latitude', 'floweringdate'])
                    print(df1_long.shape)
                    all_df = pd.concat([all_df, df1_long], ignore_index=True)
                    print('all_df shape: ', all_df.shape)
                else:
                    pass

            # print('years', all_df['year'].unique().tolist())
            # all_df['year'] = all_df['year'].astype('int32')

            # Write recipe outputs
            soy_cropfact_grid_prep_dir_path = os.path.join('/opt/ml/processing/data/corn_cropfact_grid_prep',
                                                           DKU_DST_ap_data_sector)

            soy_cropfact_grid_prep_data_path = os.path.join(soy_cropfact_grid_prep_dir_path,
                                                            'corn_cropfact_grid_prep.parquet')
            print('corn_cropfact_grid_prep_data_path: ', soy_cropfact_grid_prep_data_path)
            isExist = os.path.exists(soy_cropfact_grid_prep_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(soy_cropfact_grid_prep_dir_path)

            all_df.to_parquet(soy_cropfact_grid_prep_data_path)

        except Exception as e:
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
