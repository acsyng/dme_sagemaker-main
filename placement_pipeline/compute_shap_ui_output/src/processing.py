import os

from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.env_cluster import *
from libs.placement_lib.hybrid_ranking import *
from libs.placement_lib.lgbm_utils import create_model_input, find_latest_model, load_object, folder_files, \
    create_model_input_soy, create_model_input_cornsilage
from libs.placement_lib.shap_summary_model import *
from libs.placement_lib.teams_notification import teams_notification
from libs.placement_lib.ui_grid_utils import merge_cfgrid_entry
from libs.event_bridge.event import error_event
import boto3
import argparse
import json
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def shap_ui_output_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, args, logger):
    try:
        # Read recipe inputs

        model_fpath = os.path.join(args.s3_input_train_data_lgbm_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                                   pipeline_runid)
        print('model_fpath: ', model_fpath)
        files_in_folder = folder_files(
            os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_train_data_lgbm/data/train_data_lgbm',
                         DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, ''))
        print('files_in_folder: ', files_in_folder)
        latest_model, mdate, mtime = find_latest_model(files_in_folder)
        print('latest_model: ', latest_model)
        estimator = load_object(model_fpath, latest_model)
        print('estimator loaded')

        # cropFact_grid_for_model = dataiku.Dataset("cropFact_grid_for_model")
        # cropFact_grid_df = cropFact_grid_for_model.get_dataframe()
        s3 = boto3.resource('s3')
        # bucket_name = 'us.com.syngenta.ap.nonprod'
        bucket_name = S3_BUCKET
        bucket = s3.Bucket(bucket_name)
        cropFact_grid_df = pd.DataFrame()
        for obj in bucket.objects.filter(Prefix=os.path.join(get_s3_prefix(ENVIRONMENT),
                                                             'compute_cropFact_grid_for_model/data/cropFact_grid_for_model',
                                                             DKU_DST_ap_data_sector, '')):
            # print(obj.key)
            df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
            # print('df shape: ', df.shape)
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                df['Market_Days'] = df['Market']
                df['maturity'] = df['Market']
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                df['Market_Days'] = df['Market'].str[0].astype(int)
                df['maturity'] = df['Market']
            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                df['Market_Days'] = df['maturity']
                print('shape after read: ', df.shape)
                df = df[df.irrigation == 'NON-IRRIGATED']
                print('shape after filtering: ', df.shape)
            if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                df['Market_Days'] = df['Market']
                df['maturity'] = df['Market']

            cropFact_grid_df = pd.concat([cropFact_grid_df, df], ignore_index=True)

        print('cropFact_grid_df shape: ', cropFact_grid_df.shape)

        print('maturity(s)', cropFact_grid_df['maturity'].unique().tolist())

        # dg_be_bids = dataiku.Dataset("dg_be_bids")
        # dg_be_bids_df = dg_be_bids.get_dataframe()
        dg_be_bids_df = pd.read_parquet(
            os.path.join(args.s3_input_grid_classification_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'dg_be_bids.parquet'))
        print('dg_be_bids_df: ', dg_be_bids_df.shape)
        # hetpool1_pca = dataiku.Dataset("hetpool1_pca")
        # hetpool1_pca_df = hetpool1_pca.get_dataframe()
        hetpool1_pca_df = pd.read_parquet(
            os.path.join(args.s3_input_hetpool1_pca_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'hetpool1_pca.parquet'))
        print('hetpool1_pca_df: ', hetpool1_pca_df.shape)

        # hetpool2_pca = dataiku.Dataset("hetpool2_pca")
        # hetpool2_pca_df = hetpool2_pca.get_dataframe()
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            pass
        else:
            hetpool2_pca_df = pd.read_parquet(
                os.path.join(args.s3_input_hetpool2_pca_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                             'hetpool2_pca.parquet'))
            print('hetpool2_pca_df: ', hetpool2_pca_df.shape)

        # test_data = dataiku.Dataset("test_data")
        # test_data_df = test_data.get_dataframe()
        test_data_df = pd.read_parquet(
            os.path.join(args.s3_input_test_data_folder, DKU_DST_ap_data_sector, DKU_DST_analysis_year,
                         'test_data.parquet'))
        print('test_data_df: ', test_data_df.shape)

        rfecv_mask_path = os.path.join(model_fpath, 'feature_mask.csv')
        print('rfecv_mask_path: ', rfecv_mask_path)
        rfecv_mask = np.loadtxt(rfecv_mask_path, dtype="float64", delimiter=",", usecols=0)
        print('rfecv_mask loaded')

        # Create template of what is needed to run the model
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            X_template, _ = create_model_input_soy(test_data_df, DKU_DST_ap_data_sector, rfecv_mask)
        elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            X_template, _ = create_model_input_cornsilage(test_data_df, rfecv_mask, DKU_DST_ap_data_sector)
        else:
            X_template, _ = create_model_input(test_data_df, rfecv_mask, DKU_DST_ap_data_sector)
            print('X_template after create model input: ', X_template.shape)

        print('X_template: ', X_template.shape)
        # get analysis_year and ap_data_sector to write into UI output
        analysis_year = int(DKU_DST_analysis_year)
        ap_data_sector = dg_be_bids_df['ap_data_sector'].iloc[0]

        # merge and aggregate data

        # merge geno to get all geno information for shap calculation
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            dg_be_bids_df = merge_parent_geno_all_rms_soy(dg_be_bids_df, hetpool1_pca_df, drop_stage=False)
        else:
            dg_be_bids_df = merge_parent_geno_all_rms(dg_be_bids_df, hetpool1_pca_df, hetpool2_pca_df, drop_stage=False)

        dg_be_bids_df = dg_be_bids_df.drop_duplicates()
        print('dg_be_bids_df after merge geno: ', dg_be_bids_df.shape)
        if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
            region_types = ['wce', 'tpp']
            cropFact_grid_df = cropFact_grid_df.groupby(
                ['year', 'irrigation', 'Market_Days', 'maturity', 'wce', 'tpp']).mean(numeric_only=True).reset_index()
        if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
            region_types = ['tpp', 'meso', 'maturity']
            cropFact_grid_df = cropFact_grid_df.groupby(
                ['year', 'irrigation', 'Market_Days', 'maturity', 'tpp', 'meso']).mean(numeric_only=True).reset_index()
        if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
            region_types = ['tpp', 'rec', 'maturity']
            cropFact_grid_df = cropFact_grid_df.groupby(
                ['year', 'irrigation', 'Market_Days', 'maturity', 'tpp', 'rec']).mean(numeric_only=True).reset_index()
        if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
            region_types = ['ecw', 'acs', 'maturity', 'ap_data_sector']
            cropFact_grid_df = cropFact_grid_df.groupby(
                ['year', 'irrigation', 'Market_Days', 'maturity', 'ecw', 'acs']).mean(numeric_only=True).reset_index()
        if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            region_types = ['country', 'mst', 'maturity']
            cropFact_grid_df = cropFact_grid_df.groupby(
                ['year', 'irrigation', 'Market_Days', 'maturity', 'country', 'mst']).mean(
                numeric_only=True).reset_index()

        year_series = cropFact_grid_df[["year", "irrigation"]].drop_duplicates()
        df_outA = pd.DataFrame()

        for i in range(year_series.shape[0]):
            df_out0 = merge_cfgrid_entry(cropFact_grid_df, dg_be_bids_df, X_template, estimator,
                                         year_series.year.iloc[i],
                                         year_series.irrigation.iloc[i],
                                         None, None, None)
            for region_type in region_types:
                df_out = df_out0[:]
                df_out['region_type'] = region_type
                df_out['region_name'] = df_out[region_type].astype(str)
                # Perform transformations - in this case, select only columns of interest and write to output table.
                if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                    id_columns = ['ap_data_sector', 'region_type', 'region_name', 'maturity', 'year', 'be_bid',
                                  'irrigation', 'analysis_year', 'stage']
                if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                    id_columns = ['ap_data_sector', 'region_type', 'region_name', 'maturity', 'year', 'be_bid',
                                  'irrigation', 'analysis_year', 'stage']
                if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                    id_columns = ['ap_data_sector', 'region_type', 'region_name', 'maturity', 'year', 'be_bid',
                                  'irrigation', 'analysis_year', 'stage']
                if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                    id_columns = ['ap_data_sector', 'region_type', 'region_name', 'maturity', 'year', 'be_bid',
                                  'irrigation', 'analysis_year', 'stage']
                if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                    id_columns = ['ap_data_sector', 'region_type', 'region_name', 'maturity', 'year', 'be_bid',
                                  'irrigation', 'analysis_year', 'stage']

                df_out = df_out.groupby(id_columns).mean(numeric_only=True).reset_index()
                print(df_out.shape)
                df_outA = pd.concat([df_outA, df_out], axis=0)
                df_outA.info()
                print(df_outA.columns.values)
                print(df_outA.shape)

        df_outA.index = range(df_outA.shape[0])

        # Write recipe outputs
        shap_ui_output_dir_path = os.path.join('/opt/ml/processing/data/shap_ui_output', DKU_DST_ap_data_sector,
                                               DKU_DST_analysis_year)
        df_outA_output_data_path = os.path.join(shap_ui_output_dir_path, 'df_outA.parquet')
        print('df_outA_output_data_path: ', df_outA_output_data_path)
        isExist = os.path.exists(shap_ui_output_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(shap_ui_output_dir_path)

        # Write recipe outputs
        df_outA.to_parquet(df_outA_output_data_path)
        # calculation 1:  shap output
        # define ids_columns to identify grouping ids

        shap_outA = pd.DataFrame()
        for region_type in region_types:
            df_type = df_outA[df_outA['region_type'] == region_type]
            df_type.index = range(df_type.shape[0])
            if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                id_columns = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'maturity', 'irrigation',
                              'be_bid']
                shap_out = shap_ui_flow(df_type, estimator, id_columns, analysis_year, ap_data_sector, n_top=10,
                                        n_job=8,
                                        aggregation_levels=[['maturity', 'region_name', 'region_type'],
                                                            ['maturity', 'region_name', 'region_type', 'be_bid']])
            if DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER' or DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                id_columns = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'maturity', 'irrigation',
                              'be_bid']
                df_type['tpp_region'] = df_type['Market']
                shap_out = shap_ui_flow(df_type, estimator, id_columns, analysis_year, ap_data_sector, n_top=10,
                                        n_job=8,
                                        aggregation_levels=[['tpp_region', 'region_name', 'region_type'],
                                                            ['tpp_region', 'region_name', 'region_type', 'be_bid']])
            if DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                id_columns = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'maturity', 'irrigation',
                              'be_bid']
                # df_type['tpp_region'] = df_type['Market']
                shap_out = shap_ui_flow(df_type, estimator, id_columns, analysis_year, ap_data_sector, n_top=10,
                                        n_job=8,
                                        aggregation_levels=[['maturity', 'region_name', 'region_type'],
                                                            ['maturity', 'region_name', 'region_type', 'be_bid']])

            if DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                id_columns_init = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'irrigation',
                                   'be_bid']
                shap_out_init = shap_ui_flow(df_type, estimator, id_columns_init, analysis_year, ap_data_sector,
                                             n_top=10,
                                             n_job=8, aggregation_levels=[['region_name', 'region_type']],
                                             column_orders=['region_type', 'region_name', 'be_bid', 'analysis_year',
                                                            'ap_data_sector',
                                                            'irrigation',
                                                            'feature_name', 'feature_rank', 'feature_direction',
                                                            'feature_value_category', 'feature_shap_value',
                                                            'feature_shap_value_5',
                                                            'feature_shap_value_25', 'feature_shap_value_50',
                                                            'feature_shap_value_75', 'feature_shap_value_95',
                                                            'aggregate_level'])
                #    'maturity','region_name'])  # ['maturity', 'region_name', 'region_type'],['maturity', 'region_name', 'region_type', 'be_bid']])

                id_columns_sec = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'maturity',
                                  'irrigation', 'be_bid']
                shap_out_sec = shap_ui_flow(df_type, estimator, id_columns_sec, analysis_year, ap_data_sector,
                                            n_top=10,
                                            n_job=8,
                                            aggregation_levels=[['maturity', 'region_name', 'region_type', 'be_bid']])
                shap_out = pd.concat([shap_out_init, shap_out_sec], ignore_index=True)

            if DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER' or DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                id_columns = ['ap_data_sector', 'analysis_year', 'region_type', 'region_name', 'maturity', 'irrigation',
                              'be_bid']
                shap_out = shap_ui_flow(df_type, estimator, id_columns, analysis_year, ap_data_sector, n_top=10,
                                        n_job=8,
                                        aggregation_levels=[['maturity', 'region_name', 'region_type'],
                                                            ['maturity', 'region_name', 'region_type', 'be_bid']])

            shap_outA = pd.concat([shap_out, shap_outA], axis=0)

        shap_outA['id'] = shap_outA.reset_index().index

        shap_outA['id'] = shap_outA['id'] + 10000000 * (int(DKU_DST_analysis_year) - 2021)
        # shift column 'C' to first position
        first_column = shap_outA.pop('id')

        # insert column using insert(position,column_name,first_column) function
        shap_outA.insert(0, 'id', first_column)

        shap_ui_output_data_path = os.path.join(shap_ui_output_dir_path, 'shap_ui_output.parquet')
        print('shap_ui_output_data_path: ', shap_ui_output_data_path)
        # Write recipe outputs
        shap_outA.to_parquet(shap_ui_output_data_path)

        # calculation 2:  entry ranking
        ranking_A = pd.DataFrame()
        for region_type in region_types:
            df_type = df_outA[df_outA['region_type'] == region_type]
            print('df_type shape: ', df_type.shape)
            df_type.index = range(df_type.shape[0])

            # remove genomic variables
            df_type = remove_genomic_variables(df_type)
            print('df_type shape after remove geno vars: ', df_type.shape)
            # environment clustering
            Env_Clusters_df = cluster(df_type)

            # Rename column
            Env_Clusters_df.rename(columns={'irrigation': 'watermgmt'}, inplace=True)
            # Define the list of regions of interest
            Region_of_Interest_new = ['region_name']
            # Use the rank_hybrids function to rank the hybrids in the new DataFrame
            Hybrid_Ranking_3_df = rank_hybrids(Env_Clusters_df, Region_of_Interest_new)
            Hybrid_Ranking = ranking_post_process(Hybrid_Ranking_3_df, ap_data_sector, analysis_year)
            Hybrid_Ranking['region_type'] = region_type
            ranking_A = pd.concat([ranking_A, Hybrid_Ranking], axis=0)

        # Write recipe outputs
        # OutPut2 = dataiku.Dataset("Be_bid_Ranking")
        # OutPut2.write_with_schema(ranking_A)

        Be_bid_Ranking_dir_path = os.path.join('/opt/ml/processing/data/shap_ui_output', DKU_DST_ap_data_sector,
                                               DKU_DST_analysis_year)
        Be_bid_Ranking_data_path = os.path.join(Be_bid_Ranking_dir_path, 'Be_bid_Ranking.parquet')
        print('Be_bid_Ranking_data_path: ', Be_bid_Ranking_data_path)
        isExist = os.path.exists(Be_bid_Ranking_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(Be_bid_Ranking_dir_path)

        # Write recipe outputs
        ranking_A.to_parquet(Be_bid_Ranking_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement shap_ui_output error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_train_data_lgbm_folder', type=str,
                        help='s3 input train data lgbm folder', required=True)
    parser.add_argument('--s3_input_grid_classification_folder', type=str,
                        help='s3 input grid classification folder', required=True)
    parser.add_argument('--s3_input_hetpool1_pca_folder', type=str,
                        help='s3 input hetpool1 pca folder', required=True)
    parser.add_argument('--s3_input_hetpool2_pca_folder', type=str,
                        help='s3 input hetpool1 pca folder', required=True)
    parser.add_argument('--s3_input_test_data_folder', type=str,
                        help='s3 input test data folder', required=True)
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
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_shap_ui_output/data/shap_ui_output',
                                     DKU_DST_ap_data_sector,
                                     input_year, 'shap_ui_output.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            shap_ui_output_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, args=args, logger=logger)
            print('File created')
            #    print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
