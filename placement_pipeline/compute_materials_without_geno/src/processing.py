import argparse
import json
import os

import numpy as np
import pandas as pd

from libs.config.config_vars import ENVIRONMENT
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.lgbm_utils import find_latest_model, folder_files, load_object
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm folder', required=True)
    parser.add_argument('--s3_input_trial_data_train_folder', type=str,
                        help='s3 input trial data train folder', required=True)
    args = parser.parse_args()
    print('args collected')
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        try:
            model_fpath = os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector,
                                       DKU_DST_analysis_year,
                                       pipeline_runid)
            print('model_fpath: ', model_fpath)
            files_in_folder = folder_files(
                os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm',
                             DKU_DST_ap_data_sector,
                             DKU_DST_analysis_year, pipeline_runid, ''))
            print('files_in_folder: ', files_in_folder)
            latest_model, m_date, m_time = find_latest_model(files_in_folder)
            print('latest_model: ', latest_model)
            estimator = load_object(model_fpath, latest_model)
            print('estimator loaded')

            latest_model_allmodels, m_date, m_time = find_latest_model(files_in_folder, True)
            allmodels_estimator = load_object(model_fpath, latest_model_allmodels)
            print('allmodels_estimator loaded')

            allmodels_estimator_10 = allmodels_estimator['q 0.10']
            allmodels_estimator_50 = allmodels_estimator['q 0.50']
            allmodels_estimator_90 = allmodels_estimator['q 0.90']

            rfecv_mask_path = os.path.join(model_fpath, 'feature_mask.csv')
            print('rfecv_mask_path: ', rfecv_mask_path)
            rfecv_mask = np.loadtxt(rfecv_mask_path, dtype="float64", delimiter=",", usecols=0)
            print('rfecv_mask loaded')

            cropFact_df = pd.read_parquet(os.path.join(
                's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_trial_feature_keep_array/data/cropFact_api_output_all_years',
                DKU_DST_ap_data_sector, 'cropFact_api_output_all_years.parquet'))

            cropFact_df["year"] = cropFact_df["year"].astype('int')
            cropFact_df = cropFact_df.loc[cropFact_df["year"] <= int(DKU_DST_analysis_year),]
            print('cropFact_df["year"]: ', cropFact_df["year"].unique().tolist())
            print(cropFact_df.shape)

            trial_data_df = pd.read_parquet(os.path.join(
                's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_trial_data_all_years/data/trial_data_all_years',
                DKU_DST_ap_data_sector, 'trial_data_all_years.parquet'))
            trial_data_df["year"] = trial_data_df["year"].astype('int')
            trial_data_df = trial_data_df.loc[trial_data_df["year"] <= int(DKU_DST_analysis_year),]
            print('trial_data_df["year"]: ', trial_data_df["year"].unique().tolist())

            trial_data_keep_df = pd.read_parquet(os.path.join(
                's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_trial_feature_keep_array/data/trial_feature_keep_array',
                DKU_DST_ap_data_sector, 'trial_feature_keep_array.parquet'))
            keep_arr = np.ravel(trial_data_keep_df.to_numpy())
            print(keep_arr.shape)

            # Compute recipe outputs
            # Create separate df of all trial info that may be useful for viz/validation/explanation but isn't useful for modeling
            trial_data_df["analysis_year"] = np.full(trial_data_df.shape[0], int(DKU_DST_analysis_year))
            trial_data_df["irrflag"] = np.select([trial_data_df["irrigation"] == "NONE",
                                                  trial_data_df["irrigation"] == "TILE",
                                                  trial_data_df["irrigation"] == "LIRR",
                                                  trial_data_df["irrigation"] == "IRR"], [0, 0, 1, 2])
            trial_data_df["tileflag"] = (trial_data_df["irrigation"] == "TILE").astype(int)
            trial_info_cols = ["experiment_id", "trial_status", "irrigation", "plant_date", "harvest_date",
                               "x_longitude", "y_latitude", "state_province_code"]
            trial_info_df = trial_data_df.loc[:,
                            ["year", "ap_data_sector", "trial_id", "analysis_year"] + trial_info_cols].drop(
                columns=["plant_date", "harvest_date"]).drop_duplicates()
            # keep_arr = np.concatenate(([1, 1, 1,1], keep_arr))

            # keep_arr = np.concatenate(([1, 1, 1,1], keep_arr))
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                keep_arr = np.concatenate(([1, 1, 1], keep_arr))
                trial_data_df = trial_data_df.merge(cropFact_df.loc[:, keep_arr == 1],
                                                    on=['year', 'ap_data_sector', 'cropfact_id'],
                                                    how="inner") \
                    .drop(
                    columns=trial_info_cols + ["cropfact_id", "material_id", "match_be_bid", "GMSTP_cf", "GMSTP"])

                # read in appropriate year of hetpool1_pca data
                hetpool1_pca_df = pd.read_parquet(os.path.join(
                    's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_pca_output_hetpool1/data/pca_output_hetpool1',
                    DKU_DST_ap_data_sector, '2023/hetpool1_pca.parquet'))

                # Merge in geno info
                trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                    left_on="sample_id",
                                                    right_on="sample_id_par1",
                                                    how="left",
                                                    suffixes=("", "_par1")).drop(
                    columns=["sample_id", "sample_id_par1"])


            else:
                # Merge in cropFact info

                # keep_arr = np.concatenate(([1, 1, 1], keep_arr))

                if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                    col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "GMSTP_cf", "GMSTP"]
                    keep_arr = np.concatenate(([1, 1, 1], keep_arr))
                elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                    col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "fp_het_group", "mp_het_group"]
                    keep_arr = np.concatenate(([1, 1, 1, 0, 0, 0, 0, 0], keep_arr))
                else:
                    col_to_drop = ["cropfact_id", "material_id", "match_be_bid", "fp_het_group", "mp_het_group",
                                   "GMSTP_cf",
                                   "GMSTP"]
                    keep_arr = np.concatenate(([1, 1, 1], keep_arr))
                trial_data_df = trial_data_df.merge(cropFact_df.loc[:, keep_arr == 1],
                                                    on=['year', 'ap_data_sector', 'cropfact_id'],
                                                    how="inner") \
                    .drop(columns=trial_info_cols + col_to_drop)

                # read in appropriate year of hetpool1_pca data
                hetpool1_pca_df = pd.read_parquet(os.path.join(
                    's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_pca_output_hetpool1/data/pca_output_hetpool1',
                    DKU_DST_ap_data_sector, '2023/hetpool1_pca.parquet'))

                # read in appropriate year of hetpool2_pca data
                hetpool2_pca_df = hetpool2_pca_df = pd.read_parquet(os.path.join(
                    's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_pca_output_hetpool2/data/pca_output_hetpool2',
                    DKU_DST_ap_data_sector, '2023/hetpool2_pca.parquet'))
                # Merge in geno info

                if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                    trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                        left_on="par_hp1_be_bid",
                                                        right_on="be_bid_par1",
                                                        how="left",
                                                        suffixes=("", "_par1")) \
                        .merge(hetpool2_pca_df.add_suffix("_par2"),
                               left_on="par_hp2_be_bid",
                               right_on="be_bid_par2",
                               how="left",
                               suffixes=("", "_par2")).drop(
                        columns=["par_hp1_be_bid", "be_bid_par1", "par_hp2_be_bid", "be_bid_par2"])
                else:
                    trial_data_df = trial_data_df.merge(hetpool1_pca_df.add_suffix("_par1"),
                                                        left_on="par_hp1_sample",
                                                        right_on="sample_id_par1",
                                                        how="left",
                                                        suffixes=("", "_par1")) \
                        .merge(hetpool2_pca_df.add_suffix("_par2"),
                               left_on="par_hp2_sample",
                               right_on="sample_id_par2",
                               how="left",
                               suffixes=("", "_par2")).drop(
                        columns=["par_hp1_sample", "sample_id_par1", "par_hp2_sample", "sample_id_par2"])

            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER' or DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                comp = pd.read_csv(
                    's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_grid_classification/data/Grid_10km_Corn_Brazil_Safrinha/soy_rv_trial_pheno_analytic_dataset_202406111121.csv').drop_duplicates()
                print(comp.shape)
            else:
                comp = pd.read_csv(
                    's3://us.com.syngenta.ap.prod/prod/dme/placement/compute_grid_classification/data/Grid_10km_Corn_Brazil_Safrinha/corn_rv_trial_pheno_analytic_dataset_202406071459.csv')

            comp.company.value_counts(dropna=False)

            trial_data_df1 = trial_data_df.merge(comp, on='be_bid')

            pd.set_option('display.max_columns', None)
            col1 = ['company']
            counts_n = trial_data_df1.groupby(col1).size()
            missing_vals = trial_data_df1.groupby(col1).count().rsub(trial_data_df1.groupby(col1).size(),
                                                                     axis=0).reset_index()[
                ['company', 'comp_0_par1']]
            missing_vals['Count'] = pd.DataFrame(counts_n.astype(int)).reset_index()[0]
            ab = missing_vals[missing_vals.comp_0_par1 == missing_vals.Count]['company'].values.tolist()

            comp_data = trial_data_df1[trial_data_df1.company.isin(ab)]
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER':
                comp_data = comp_data[comp_data.tpp_region != 'B']
                comp_data["tpp_region"] = comp_data["tpp_region"].astype('int')
                info_cols = ['year', 'ap_data_sector', 'YGSMN', 'trial_id', 'trial_stage',
                             'loc_selector', 'plot_barcode', 'be_bid', 'par_hp1_be_bid',
                             'par_hp2_be_bid', 'cpifl', 'cperf', 'company', 'analysis_year']

            if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
                comp_data = comp_data.drop(columns=['et'])
                info_cols = ['year', 'ap_data_sector', 'YGSMN', 'trial_id', 'trial_stage',
                             'loc_selector', 'plot_barcode', 'be_bid', 'par_hp1_be_bid',
                             'par_hp2_be_bid', 'cpifl', 'cperf', 'company', 'analysis_year']

            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                info_cols = ["year", "ap_data_sector", 'tpp_region', "trial_id", "plot_barcode", "be_bid",
                             "loc_selector", 'company',
                             "cpifl", "cperf", "analysis_year", "YGSMN"]
            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                info_cols = ["year", "ap_data_sector", 'maturity_zone', "trial_id", 'trial_stage', "plot_barcode", "be_bid",
                             "loc_selector", 'company',
                             "cpifl", "cperf", "analysis_year", "YGSMN"]

            comp_data1 = comp_data.drop(columns=info_cols)

            print('comp data before predict: ', comp_data.shape)
            print(comp_data.columns.values)
            comp_data["comp_data_preds"] = estimator.predict(comp_data1)

            print('comp data after predict: ', comp_data.shape)

            # Write recipe outputs
            materials_without_geno_dir_path = os.path.join('/opt/ml/processing/data/materials_without_geno',
                                                           DKU_DST_ap_data_sector, DKU_DST_analysis_year)

            materials_without_geno_data_path = os.path.join(materials_without_geno_dir_path,
                                                            'materials_without_geno.parquet')
            print('materials_without_geno_data_path: ', materials_without_geno_data_path)
            isExist = os.path.exists(materials_without_geno_dir_path)
            if not isExist:
                # Create a new directory because it does not exist
                os.makedirs(materials_without_geno_dir_path)

            comp_data.to_parquet(materials_without_geno_data_path)

        except Exception as e:
            logger.error(e)
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            message = f'Placement materials_without_geno error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
            print('message: ', message)
            teams_notification(message, None, pipeline_runid)
            print('teams message sent')
            raise e


if __name__ == '__main__':
    main()
