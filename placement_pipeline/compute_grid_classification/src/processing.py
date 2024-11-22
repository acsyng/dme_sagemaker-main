import argparse
import json
import os

import pandas as pd

from libs.config.config_vars import ENVIRONMENT
from libs.denodo.denodo_connection import DenodoConnection
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.geno_queries import get_highname_soy
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import check_if_file_exists_s3, get_s3_prefix


def grid_classification_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args, logger):
    try:
        # Read recipe inputs
        # grid_10km_RM_Density_Final = dataiku.Dataset("TRIALNETWORKDESIGNNACORN2023.Grid_10km_RM_Density_Final")
        # grid_10km_df = grid_10km_RM_Density_Final.get_dataframe()
        if DKU_DST_ap_data_sector == "CORN_NA_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_RM_Density_Final/Grid_10km_RM_Density_Final.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

        if DKU_DST_ap_data_sector == "SOY_NA_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_Soy_NA_Summer/BaseMap_Grid_10km_NA_Soy_2024.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_Soy_Brazil_Summer/BaseMap_Grid_10km_Brazil_Soy_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f',
                         'st_code', 'mu_code', 'value_ratio', 'Market_Volume'])

            grid_10km_df = grid_10km_df.rename(columns={'Syngenta_Cycle': 'Market', "Market_Value": 'Market_Pct'})

            grid_10km_df['Market_Days'] = grid_10km_df['Market'].str[0].astype(int)  # * 5 + 80

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_Corn_Brazil_Summer/BaseMap_Grid_10km_Brazil_Corn_Summer_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f', 'market_seg',
                         'Grid_area', 'Market_Volume', 'value_ratio'])

            grid_10km_df = grid_10km_df.rename(columns={'RM_BAND': 'Market', "Market_Value": 'Market_Pct'})

            grid_10km_df['Market_Days'] = grid_10km_df['Market']

        if DKU_DST_ap_data_sector == "CORNGRAIN_EAME_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
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
            grid_10km_df['Market_Area'] = grid_10km_df['ha']

            grid_10km_df = grid_10km_df.rename(columns={'MaturityGroup': 'Market', "ha": 'Market_Pct',
                                                        'country_c3': 'country'})

            grid_10km_df['Market_Days'] = grid_10km_df['Market']

        if DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_Corngrain_EAME_Summer/eame_market_based_on_cropland_5.csv'))
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
            grid_10km_df['Market_Area'] = grid_10km_df['ha']

            grid_10km_df = grid_10km_df.rename(columns={'MaturityGroup': 'Market', "ha": 'Market_Pct',
                                                        'country_c3': 'country'})

            grid_10km_df['Market_Days'] = grid_10km_df['Market']

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SAFRINHA":
            grid_10km_df = pd.read_csv(os.path.join(args.s3_input_grid_10km_folder,
                                                    'Grid_10km_Corn_Brazil_Safrinha/BaseMap_Grid_10km_Brazil_Corn_Safrinha_2024_v2.csv'))
            print('grid_10km_df shape: ', grid_10km_df.shape)

            # grid_10km_df = grid_10km_df[grid_10km_df.country_n == 'Brazil']

            grid_10km_df = grid_10km_df.drop(
                columns=['gid', 'row_id', 'region_c', 'country_n', 'country_c2', 'country_c3',
                         'area_ha', 'land_f', 'cropland_f', 'market_seg',
                         'Grid_area', 'Market_Volume', 'value_ratio'])

            grid_10km_df = grid_10km_df.rename(columns={'RM_BAND': 'Market', "Market_Value": 'Market_Pct'})

            grid_10km_df['Market_Days'] = grid_10km_df['Market']

        # trial_data_by_year = dataiku.Dataset("trial_data_by_year")
        trial_data_df = pd.read_parquet(os.path.join(args.s3_input_trial_data_all_years_folder, DKU_DST_ap_data_sector,
                                                     'trial_data_all_years.parquet'))
        # trial_data_df = pd.read_parquet("/opt/ml/processing/input/data/trial_data_all_years")
        trial_data_df["year"] = trial_data_df["year"].astype('int')
        trial_data_df = trial_data_df.loc[trial_data_df["year"] <= int(DKU_DST_analysis_year),]
        print('trial_data_df["year"]: ', trial_data_df["year"].unique().tolist())
        trial_df = trial_data_df
        print('trial_df shape: ', trial_df.shape)
        print('trial_df.ap_data_sector: ', trial_df.ap_data_sector.unique().tolist())

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            with DenodoConnection() as dc:
                asec_df = dc.get_data("""SELECT
                CAST(analysis_year AS integer) AS analysis_year,
                ap_data_sector_name AS ap_data_sector,
                experiment_id,
                decision_group_rm,
                MAX(stage) as stage
              FROM managed.rv_ap_sector_experiment_config
            WHERE CAST(analysis_year AS integer) = 2024
            and experiment_id LIKE '23%'
            GROUP BY
                analysis_year,
                ap_data_sector_name,
                experiment_id,
                decision_group_rm
            """)
            asec_df['analysis_year'] = DKU_DST_analysis_year
            print('asec_df shape: ', asec_df.shape)

        elif DKU_DST_ap_data_sector == "CORN_BRAZIL_SAFRINHA":
            with DenodoConnection() as dc:
                asec_df = dc.get_data("""SELECT
                CAST(analysis_year AS integer) AS analysis_year,
                ap_data_sector_name AS ap_data_sector,
                experiment_id,
                decision_group_rm,
                MAX(stage) as stage
              FROM managed.rv_ap_sector_experiment_config
            WHERE CAST(analysis_year AS integer) = 2024
            and experiment_id LIKE '24%'
            GROUP BY
                analysis_year,
                ap_data_sector_name,
                experiment_id,
                decision_group_rm
            """)
            asec_df['analysis_year'] = DKU_DST_analysis_year
            print('asec_df shape: ', asec_df.shape)

        else:
            with DenodoConnection() as dc:
                asec_df = dc.get_data("""SELECT
                CAST(analysis_year AS integer) AS analysis_year,
                ap_data_sector_name AS ap_data_sector,
                experiment_id,
                decision_group_rm,
                MAX(stage) as stage
              FROM managed.rv_ap_sector_experiment_config
            WHERE CAST(analysis_year AS integer) = {0}
            and experiment_id LIKE '23%'
            GROUP BY
                analysis_year,
                ap_data_sector_name,
                experiment_id,
                decision_group_rm
            """.format(DKU_DST_analysis_year))
            print('asec_df shape: ', asec_df.shape)

        asec_full = asec_df
        asec_df = asec_df[asec_df['ap_data_sector'] == DKU_DST_ap_data_sector]

        grid_classification_dir_path = os.path.join('/opt/ml/processing/data/grid_classification',
                                                    DKU_DST_ap_data_sector, DKU_DST_analysis_year)

        isExist = os.path.exists(grid_classification_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(grid_classification_dir_path)

        asec_full_data_path = os.path.join(grid_classification_dir_path, 'asec_full.parquet')
        asec_full.to_parquet(asec_full_data_path)

        asec_df_data_path = os.path.join(grid_classification_dir_path, 'asec_df.parquet')
        asec_df.to_parquet(asec_df_data_path)

        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            highname_df = get_highname_soy()
            highname_df_data_path = os.path.join(grid_classification_dir_path, 'highname_df.parquet')
            highname_df.to_parquet(highname_df_data_path)

        if DKU_DST_ap_data_sector == "CORN_NA_SUMMER":
            asec_df_subset = asec_df.loc[asec_df.stage > 4, :]

        if DKU_DST_ap_data_sector == "CORNGRAIN_EAME_SUMMER" or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            asec_df_subset = asec_df.loc[asec_df.stage > 4, :]

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER":
            asec_df['decision_group_rm'] = asec_df['decision_group_rm'].replace(133, 129)
            asec_df['decision_group_rm'] = asec_df['decision_group_rm'].replace(136, 133)
            asec_df_subset = asec_df[(asec_df['stage'] > 4) & ~(asec_df.decision_group_rm.isna())]

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SAFRINHA":
            asec_df['decision_group_rm'] = asec_df['decision_group_rm'].replace(133, 129)
            asec_df['decision_group_rm'] = asec_df['decision_group_rm'].replace(137, 133)
            asec_df_subset = asec_df[(asec_df['stage'] >= 5) & ~(asec_df.decision_group_rm.isna())]

        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            asec_df_subset = asec_df[(asec_df['stage'] >= 5) & ~(asec_df.decision_group_rm.isna())]

            dg_bebid_df = trial_df[
                ['ap_data_sector', 'experiment_id', 'be_bid', 'sample_id', 'cpifl', 'cperf']] \
                .drop_duplicates() \
                .merge(asec_df_subset, on=['ap_data_sector', 'experiment_id']) \
                .groupby(["analysis_year", "ap_data_sector", "decision_group_rm", "stage", "be_bid"]) \
                .agg(par_hp1_be_bid=pd.NamedAgg(column="be_bid", aggfunc="first"),
                     par_hp1_sample=pd.NamedAgg(column="sample_id", aggfunc="first"),
                     cpifl=pd.NamedAgg(column="cpifl", aggfunc="max"),
                     cperf=pd.NamedAgg(column="cperf", aggfunc="max")).reset_index()

        else:
            dg_bebid_df = trial_df[
                ['ap_data_sector', 'experiment_id', 'be_bid', 'par_hp1_be_bid', 'par_hp1_sample', 'par_hp2_be_bid',
                 'par_hp2_sample', 'cpifl', 'cperf']] \
                .drop_duplicates() \
                .merge(asec_df_subset, on=['ap_data_sector', 'experiment_id']) \
                .groupby(["analysis_year", "ap_data_sector", "decision_group_rm", "stage", "be_bid"]) \
                .agg(par_hp1_be_bid=pd.NamedAgg(column="par_hp1_be_bid", aggfunc="first"),
                     par_hp1_sample=pd.NamedAgg(column="par_hp1_sample", aggfunc="first"),
                     par_hp2_be_bid=pd.NamedAgg(column="par_hp2_be_bid", aggfunc="first"),
                     par_hp2_sample=pd.NamedAgg(column="par_hp2_sample", aggfunc="first"),
                     cpifl=pd.NamedAgg(column="cpifl", aggfunc="max"),
                     cperf=pd.NamedAgg(column="cperf", aggfunc="max")).reset_index()

        print('dg_bebid_df shape after merge with asec_df: ', dg_bebid_df.shape)

        asec_df_subset_data_path = os.path.join(grid_classification_dir_path, 'asec_df_subset.parquet')
        asec_df_subset.to_parquet(asec_df_subset_data_path)

        dg_bebid_df.loc[
            dg_bebid_df.groupby(["analysis_year", "ap_data_sector", "decision_group_rm", "stage"])["cpifl"].transform(
                'max') == 0, "cpifl"] = 1

        dg_bebid_df.loc[
            dg_bebid_df.groupby(["analysis_year", "ap_data_sector", "decision_group_rm", "stage"])["cperf"].transform(
                'max') == 0, "cperf"] = dg_bebid_df.loc[
            dg_bebid_df.groupby(["analysis_year", "ap_data_sector", "decision_group_rm", "stage"])["cperf"].transform(
                'max') == 0, 'cpifl']

        dg_bebid_df['cpifl'] = (dg_bebid_df['cpifl'] == 1)
        dg_bebid_df['cperf'] = (dg_bebid_df['cperf'] == 1)
        print('dg_bebid_df shape before writing out: ', dg_bebid_df.shape)

        if DKU_DST_ap_data_sector == "CORN_NA_SUMMER":
            grid_10km_df.columns = grid_10km_df.columns.str.lower()
            grid_10km_df["ap_data_sector"] = DKU_DST_ap_data_sector  # 'NA_CORN_SUMMER'
            grid_10km_df["analysis_year"] = int(DKU_DST_analysis_year)

            grid_10km_df = grid_10km_df.loc[grid_10km_df["market_pct"] > 0, :] \
                .rename(columns={'subccode': 'sub_c_code', 'spatialunitcode': 'spatial_unit_code',
                                 'market_days': 'maturity_days', 'market': 'maturity'})

            grid_classification_df = grid_10km_df[
                ['place_id', 'latitude', 'longitude', 'rm_band', 'wce', 'tpp', 'market_segment', 'sub_c_code',
                 'spatial_unit_code', 'ap_data_sector', 'analysis_year']].drop_duplicates(
                ignore_index=True).reset_index().rename(columns={'index': 'id'})

            grid_classification_df["id"] = grid_classification_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

            # grid_market_maturity_df = grid_10km_df[['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct', 'market_pct']].drop_duplicates()\
            #                                      .merge(dg_bebid_df[['analysis_year', 'ap_data_sector', 'decision_group', 'decision_group_rm', 'be_bid', 'cperf']], left_on="maturity_days", right_on="decision_group_rm").drop(columns = "decision_group_rm")\
            #                                      .drop_duplicates().reset_index().rename(columns = {'index':'id'})

            grid_market_maturity_df = grid_10km_df[
                ['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct',
                 'market_pct', 'ap_data_sector', 'analysis_year']].drop_duplicates() \
                .merge(dg_bebid_df[['decision_group_rm', 'stage']].drop_duplicates(), left_on="maturity_days",
                       right_on="decision_group_rm").drop(columns="decision_group_rm") \
                .drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id', 'market_acres': 'maturity_acres', 'market_pct': 'maturity_pct'})

            grid_market_maturity_df = grid_market_maturity_df.astype({'stage': 'str', 'maturity_days': 'int16'})
            grid_market_maturity_df["id"] = grid_market_maturity_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

        if DKU_DST_ap_data_sector == "SOY_NA_SUMMER":
            grid_10km_df.columns = grid_10km_df.columns.str.lower()
            grid_10km_df["ap_data_sector"] = DKU_DST_ap_data_sector  # 'NA_CORN_SUMMER'
            grid_10km_df["analysis_year"] = int(DKU_DST_analysis_year)

            grid_10km_df = grid_10km_df.loc[grid_10km_df["market_pct"] > 0, :] \
                .rename(columns={'subccode': 'sub_c_code', 'spatialunitcode': 'spatial_unit_code',
                                 'rm_num': 'maturity_days'})
            grid_10km_df['maturity'] = grid_10km_df['maturity_days']

            grid_classification_df = grid_10km_df[
                ['place_id', 'latitude', 'longitude', 'rm_band', 'ecw', 'acs', 'maturity_days', 'maturity',
                 'sub_c_code',
                 'spatial_unit_code', 'ap_data_sector', 'analysis_year']].drop_duplicates(
                ignore_index=True).reset_index().rename(columns={'index': 'id'})

            grid_classification_df["id"] = grid_classification_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

            # grid_market_maturity_df = grid_10km_df[['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct', 'market_pct']].drop_duplicates()\
            #                                      .merge(dg_bebid_df[['analysis_year', 'ap_data_sector', 'decision_group', 'decision_group_rm', 'be_bid', 'cperf']], left_on="maturity_days", right_on="decision_group_rm").drop(columns = "decision_group_rm")\
            #                                      .drop_duplicates().reset_index().rename(columns = {'index':'id'})

            grid_market_maturity_df = grid_10km_df[
                ['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct',
                 'market_pct', 'ap_data_sector', 'analysis_year']].drop_duplicates() \
                .merge(dg_bebid_df[['decision_group_rm', 'stage']].drop_duplicates(), left_on="maturity_days",
                       right_on="decision_group_rm").drop(columns="decision_group_rm") \
                .drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id', 'market_acres': 'maturity_acres', 'market_pct': 'maturity_pct'})

            grid_market_maturity_df = grid_market_maturity_df.astype({'stage': 'str', 'maturity_days': 'int16'})
            grid_market_maturity_df["id"] = grid_market_maturity_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

        if DKU_DST_ap_data_sector == "CORN_BRAZIL_SUMMER" or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            grid_10km_df.columns = grid_10km_df.columns.str.lower()
            grid_10km_df["ap_data_sector"] = DKU_DST_ap_data_sector
            grid_10km_df["analysis_year"] = int(DKU_DST_analysis_year)

            grid_10km_df = grid_10km_df.loc[grid_10km_df["market_pct"] > 0, :] \
                .rename(columns={'market_days': 'maturity_days', 'market': 'maturity'})

            grid_classification_df = grid_10km_df[
                ['place_id', 'latitude', 'longitude', 'tpp', 'meso', 'maturity_days', 'maturity',
                 'ap_data_sector', 'analysis_year']].drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id'})

            grid_classification_df["id"] = grid_classification_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

            # grid_market_maturity_df = grid_10km_df[['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct', 'market_pct']].drop_duplicates()\
            #                                      .merge(dg_bebid_df[['analysis_year', 'ap_data_sector', 'decision_group', 'decision_group_rm', 'be_bid', 'cperf']], left_on="maturity_days", right_on="decision_group_rm").drop(columns = "decision_group_rm")\
            #                                      .drop_duplicates().reset_index().rename(columns = {'index':'id'})

            grid_market_maturity_df = grid_10km_df[
                ['place_id', 'maturity', 'maturity_days', 'market_area',
                 'market_pct', 'ap_data_sector', 'analysis_year']].drop_duplicates() \
                .merge(dg_bebid_df[['decision_group_rm', 'stage']].drop_duplicates(), left_on="maturity_days",
                       right_on="decision_group_rm").drop(columns="decision_group_rm") \
                .drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id', 'market_area': 'maturity_acres', 'market_pct': 'maturity_pct'})

            grid_market_maturity_df = grid_market_maturity_df.astype({'stage': 'str', 'maturity_days': 'int16'})
            grid_market_maturity_df["id"] = grid_market_maturity_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            grid_10km_df.columns = grid_10km_df.columns.str.lower()
            grid_10km_df["ap_data_sector"] = DKU_DST_ap_data_sector
            grid_10km_df["analysis_year"] = int(DKU_DST_analysis_year)

            grid_10km_df = grid_10km_df.loc[grid_10km_df["market_pct"] > 0, :] \
                .rename(columns={'market_days': 'maturity_days', 'market': 'maturity'})

            grid_classification_df = grid_10km_df[
                ['place_id', 'latitude', 'longitude', 'country', 'mst', 'maturity_days', 'maturity',
                 'ap_data_sector', 'analysis_year']].drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id'})

            grid_classification_df["id"] = grid_classification_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

            # grid_market_maturity_df = grid_10km_df[['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct', 'market_pct']].drop_duplicates()\
            #                                      .merge(dg_bebid_df[['analysis_year', 'ap_data_sector', 'decision_group', 'decision_group_rm', 'be_bid', 'cperf']], left_on="maturity_days", right_on="decision_group_rm").drop(columns = "decision_group_rm")\
            #                                      .drop_duplicates().reset_index().rename(columns = {'index':'id'})

            grid_market_maturity_df = grid_10km_df[
                ['place_id', 'maturity', 'maturity_days', 'market_area',
                 'market_pct', 'ap_data_sector', 'analysis_year']].drop_duplicates() \
                .merge(dg_bebid_df[['decision_group_rm', 'stage']].drop_duplicates(), left_on="maturity_days",
                       right_on="decision_group_rm").drop(columns="decision_group_rm") \
                .drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id', 'market_area': 'maturity_acres', 'market_pct': 'maturity_pct'})

            grid_market_maturity_df = grid_market_maturity_df.astype({'stage': 'str', 'maturity_days': 'int16'})
            grid_market_maturity_df["id"] = grid_market_maturity_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

        if DKU_DST_ap_data_sector == "SOY_BRAZIL_SUMMER":
            grid_10km_df.columns = grid_10km_df.columns.str.lower()
            grid_10km_df["ap_data_sector"] = DKU_DST_ap_data_sector
            grid_10km_df["analysis_year"] = int(DKU_DST_analysis_year)

            grid_10km_df = grid_10km_df.loc[grid_10km_df["market_pct"] > 0, :] \
                .rename(columns={'market_days': 'maturity_days', 'market': 'maturity'})

            grid_classification_df = grid_10km_df[
                ['place_id', 'latitude', 'longitude', 'tpp', 'rec', 'maturity_days', 'maturity',
                 'ap_data_sector', 'analysis_year']].drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id'})

            grid_classification_df["id"] = grid_classification_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

            # grid_market_maturity_df = grid_10km_df[['place_id', 'maturity', 'maturity_days', 'core_north_south', 'market_acres', 'irrigated_pct', 'market_pct']].drop_duplicates()\
            #                                      .merge(dg_bebid_df[['analysis_year', 'ap_data_sector', 'decision_group', 'decision_group_rm', 'be_bid', 'cperf']], left_on="maturity_days", right_on="decision_group_rm").drop(columns = "decision_group_rm")\
            #                                      .drop_duplicates().reset_index().rename(columns = {'index':'id'})

            grid_market_maturity_df = grid_10km_df[
                ['place_id', 'maturity', 'maturity_days', 'market_area',
                 'market_pct', 'ap_data_sector', 'analysis_year']].drop_duplicates() \
                .merge(dg_bebid_df[['decision_group_rm', 'stage']].drop_duplicates(), left_on="maturity_days",
                       right_on="decision_group_rm").drop(columns="decision_group_rm") \
                .drop_duplicates(ignore_index=True).reset_index().rename(
                columns={'index': 'id', 'market_area': 'maturity_acres', 'market_pct': 'maturity_pct'})

            grid_market_maturity_df = grid_market_maturity_df.astype({'stage': 'str', 'maturity_days': 'int16'})
            grid_market_maturity_df["id"] = grid_market_maturity_df["id"] + 400000 * (int(DKU_DST_analysis_year) - 2021)

        dg_be_bids_dir_path = os.path.join('/opt/ml/processing/data/grid_classification', DKU_DST_ap_data_sector,
                                           DKU_DST_analysis_year)
        dg_be_bids_data_path = os.path.join(dg_be_bids_dir_path, 'dg_be_bids.parquet')
        print('dg_be_bids_data_path: ', dg_be_bids_data_path)

        grid_classification_data_path = os.path.join(grid_classification_dir_path, 'grid_classification.parquet')
        print('grid_classification_data_path: ', grid_classification_data_path)

        grid_market_maturity_dir_path = os.path.join('/opt/ml/processing/data/grid_classification',
                                                     DKU_DST_ap_data_sector, DKU_DST_analysis_year)
        grid_market_maturity_data_path = os.path.join(grid_market_maturity_dir_path, 'grid_market_maturity.parquet')
        print('grid_market_maturity_data_path: ', grid_market_maturity_data_path)

        # Write recipe outputs
        # grid_classification_data_path = os.path.join('/opt/ml/processing/data', 'grid_classification.parquet')
        grid_classification_df.to_parquet(grid_classification_data_path)

        # dg_be_bids_data_path = os.path.join('/opt/ml/processing/data', 'dg_be_bids.parquet')
        dg_bebid_df.to_parquet(dg_be_bids_data_path)

        # grid_market_maturity_data_path = os.path.join('/opt/ml/processing/data', 'grid_market_maturity.parquet')
        grid_market_maturity_df.to_parquet(grid_market_maturity_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement grid_classification error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_grid_10km_folder', type=str,
                        help='s3 input grid 10km folder', required=True)
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
        years = [str(DKU_DST_analysis_year)]

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_grid_classification/data/grid_classification',
                                     DKU_DST_ap_data_sector,
                                     input_year, 'grid_classification.parquet')

            check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:

            print('Creating file in the following location: ', file_path)
            grid_classification_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args, logger=logger)
            print('File created')
            print()

            # else:
            # print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
