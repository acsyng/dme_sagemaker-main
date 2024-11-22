# import packages
import os
import sys
import json
import pandas as pd
import numpy as np
import argparse

from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.performance_lib import performance_helper
from libs.config.config_vars import ENVIRONMENT, CONFIG, S3_BUCKET, S3_DATA_PREFIX
from libs.postgres.postgres_connection import PostgresConnection


def get_materials_per_decision_group(ap_data_sector, analysis_year):
    # analysis year in postgres table corresponds to harvest year... our year is planting year.
    # for some data sectors (LATAM), harvest year is different from planting year
    year_offset = 0
    if performance_helper.check_for_year_offset(ap_data_sector):
        year_offset = 1
    query_str = """
                select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", dg.stage as "dg_stage"
                from advancement.decision_group dg 
                inner join advancement.decision_group_entry dge 
                    on dge.decision_group_id = dg.decision_group_id 
                where dg.ap_data_sector_name = {0}
                    and dg.analysis_year = {1}
                    and dg.decision_group_type != 'MULTI_YEAR'
            """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset)

    dc = PostgresConnection()
    material_list_df = dc.get_data(query_str)
    material_list_df = material_list_df.drop_duplicates()

    return material_list_df


def get_materials_and_experiments_per_decision_group(ap_data_sector, analysis_year, material_type='entry'):
    # analysis year in postgres table corresponds to harvest year... our year is planting year.
    # for some data sectors (LATAM), harvest year is different from planting year
    dc = PostgresConnection()
    year_offset = 0
    if performance_helper.check_for_year_offset(ap_data_sector) == True:
        year_offset = 1

    if material_type == 'entry':
        postgres_table = 'decision_group_entry'
    elif material_type == 'parent':
        postgres_table = 'decision_group_hybrid_parent'

    query_str = """
        select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", 
            dg.stage as "dg_stage", dge.is_check , dge2.experiment_id as "source_id"
        from advancement.decision_group dg 
        inner join advancement.{2} dge 
            on dge.decision_group_id = dg.decision_group_id 
        inner join advancement.decision_group_experiment dge2 
            on dge2.decision_group_id = dg.decision_group_id 
        where dg.ap_data_sector_name = {0}
            and dg.analysis_year = {1}
            and dg.decision_group_type != 'MULTI_YEAR'
            and cast(dg.stage as float) < 10
    """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    df = dc.get_data(query_str)

    # get decision_group_name as source_id in case of single-exp decision groups
    query_str = """
        select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", 
            dg.stage as "dg_stage", dge.is_check , dg.decision_group_name as "source_id"
        from advancement.decision_group dg 
        inner join advancement.{2} dge 
            on dge.decision_group_id = dg.decision_group_id 
        where dg.ap_data_sector_name = {0}
            and dg.analysis_year = {1}
            and dg.decision_group_type != 'MULTI_YEAR'
            and cast(dg.stage as float) < 10
    """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    df_dg = dc.get_data(query_str)
    df = pd.concat((df, df_dg), axis=0).drop_duplicates()

    # all 1's and 0's for is_check
    replace_dict = {True: 1, 'True': 1, False: 0, 'False': 0}
    df = df.replace(to_replace=replace_dict)

    return df


def preprocess_infer(ap_data_sector,
                    output_year,
                    pipeline_runid,
                    material_type,
                    args,
                    write_outputs=1,
                    filter_materials=1):

    bucket = CONFIG['bucket']

    # preprocess checks per decision group, but entries across all decision groups.
    # this is weird, really need to find a way around this that's appropriate for model building
    # filter down to specific list of materials
    # also gets decision groups for each material.
    if filter_materials == 1:
        #material_decision_group_list = get_materials_per_decision_group(
        #    ap_data_sector=ap_data_sector,
        #    analysis_year=output_year
        #)
        material_experiment_decision_group_list = get_materials_and_experiments_per_decision_group(
            ap_data_sector=ap_data_sector,
            analysis_year=output_year,
            material_type=material_type
        )
    else:
        material_experiment_decision_group_list = None

    # load in files from s3, pivot, aggregate, and merge
    df_input_piv = predictive_advancement_lib.load_and_preprocess_all_inputs_ml(
        args=args,
        prefix_all='data',
        years_to_load=output_year,
        material_type=material_type,
        material_experiment_decision_group_list=material_experiment_decision_group_list,
        read_from_s3=0,
        is_infer=1,
        bucket=bucket
    )


    print('after agg', df_input_piv.shape)

    # get decision group info for entries, duplicate rows for each entry per dg
    if material_experiment_decision_group_list.shape[0] > 0:
        # split checks and non-checks
        df_input_piv_checks = df_input_piv[df_input_piv['is_check'] == 1]
        df_input_piv_entries = df_input_piv[df_input_piv['is_check'] != 1].drop(
            columns=['decision_group_name','is_check','dg_stage']
        )

        print('entry size', df_input_piv_entries.shape)
        # extract only non-checks and only the columns we wantonly merge non-checks?
        material_entry_dg_list = material_experiment_decision_group_list[material_experiment_decision_group_list['is_check'] == 0]
        material_entry_dg_list = material_entry_dg_list[['decision_group_name', "entry_identifier","dg_stage"]].drop_duplicates()
        df_input_piv_entries = df_input_piv_entries.merge(
            material_entry_dg_list,
            how='inner',
            on='entry_identifier')
        print('entry size after merge', df_input_piv_entries.shape)
        df_input_piv_entries['is_check'] = 0

        df_input_piv = pd.concat((df_input_piv_checks, df_input_piv_entries), axis=0)
        df_input_piv = df_input_piv.rename(columns={'decision_group_name':'decision_group'})
    else:
        df_input_piv['decision_group'] = 'na'
        df_input_piv['dg_stage'] = df_input_piv['current_stage']

    # check to see if file path exists
    out_dir = '/opt/ml/processing/data/preprocessing_infer/{}'.format(pipeline_runid)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    df_input_piv.to_csv(os.path.join(out_dir, 'testing_input_piv_{}.csv'.format(material_type)), index=False)
    if material_experiment_decision_group_list is not None:
        material_experiment_decision_group_list.to_csv(
            os.path.join(out_dir, 'testing_material_list_{}.csv'.format(material_type)), index=False)

    df_input_piv = df_input_piv.replace([np.inf, -np.inf], np.nan)

    # preprocess harvt
    if 'SOY' in ap_data_sector and 'harvt' in df_input_piv.columns:
        # also drop based on harvt
        df_input_piv['harvt_drop'] = df_input_piv['harvt'].apply(predictive_advancement_lib.process_harvt)
    elif 'SOY' in ap_data_sector:
        df_input_piv['harvt_drop'] = 0

    # For each stage in df_input_piv, get a preprocessor (if it exists)
    # preprocess, output evaluation data. Keep all columns to avoid having to load in model here.
    df_input_piv = df_input_piv[df_input_piv['dg_stage'].notna()]
    for stg in pd.unique(df_input_piv['dg_stage']): # do for stages based on decision groups
        if len(stg) == 1:
            stg_as_num = int(stg)
        else:
            stg_as_num = float(stg)

        if stg == '4.1': # use stage 4
            # load in most recent preprocessor with a corresponding model
            mdl_fname, preproc_fname, meta_fname = predictive_advancement_lib.get_fname_local(
                fpath=args.s3_input_model_folder,
                stage=str(4),
                material_type=material_type
            )
        else:
            # load in most recent preprocessor with a corresponding model
            mdl_fname, preproc_fname, meta_fname = predictive_advancement_lib.get_fname_local(
                fpath=args.s3_input_model_folder,
                stage=str(stg_as_num),
                material_type=material_type
            )

        print(str(stg_as_num), mdl_fname, preproc_fname)

        if material_type == 'entry':
            df_use = df_input_piv[
                (df_input_piv['material_type_simple'] == 'entry') &
                (df_input_piv['dg_stage'] == stg)
            ]
        else:
            df_use = df_input_piv[
                (df_input_piv['material_type_simple'] != 'entry') &
                (df_input_piv['dg_stage'] == stg)
                ]

        if mdl_fname != '' and preproc_fname != '' and df_use.shape[0] > 0: # otherwise do nothing
            # load in preprocessor
            preproc_class = performance_helper.load_object(
                args.s3_input_preprocessor_folder,
                preproc_fname
            )

            df_use_proc = preproc_class.run_preprocessing(df_use)

            # make sure all necessary columns are in df_use_proc
            meta_info = performance_helper.load_object(
                args.s3_input_model_folder,
                meta_fname
            )
            for col in meta_info['mdl_in_cols']:
                if col not in df_use_proc.columns:
                    df_use_proc[col] = np.nan

            # compute statistics on input data and note any outliers
            # remove .pkl from preproc fname, add .csv.
            preproc_stats_fname = 'preprocessed_training_statistics_' + '-'.join(preproc_fname.split('-')[1:])[:-4] + '.csv'
            df_stats = pd.read_csv(
                os.path.join(
                    args.s3_input_preprocessor_folder,
                    preproc_stats_fname
                )
            )

            for col in pd.unique(df_stats['column']):
                if col in df_use_proc.columns:
                    is_numeric = np.any(df_stats[df_stats['column'] == col]['metric'].values == 'mean')
                    is_rating = False
                    if is_numeric and not is_rating:
                        below_min = df_use_proc[col].values.reshape((-1,1)) < \
                                  df_stats[(df_stats['column'] == col) & (df_stats['metric'] == '2perc')]['value'].values
                        above_max = df_use_proc[col].values.reshape((-1,1)) > \
                                  df_stats[(df_stats['column'] == col) & (df_stats['metric'] == '98perc')]['value'].values
                    if is_numeric and is_numeric:
                        below_min = df_use_proc[col].values.reshape((-1, 1)) < \
                                    df_stats[(df_stats['column'] == col) & (df_stats['metric'] == 'min')]['value'].values
                        above_max = df_use_proc[col].values.reshape((-1, 1)) > \
                                    df_stats[(df_stats['column'] == col) & (df_stats['metric'] == 'max')]['value'].values
                    else: # don't do outlier detection for strings for now
                        below_min = np.zeros((df_use_proc.shape[0],1),dtype=bool)
                        above_max = np.zeros((df_use_proc.shape[0],1),dtype=bool)

                    df_use_proc[col + '_outlier'] = np.any(np.concatenate((above_max,below_min),axis=1), axis=1)

            # save evaluation set
            if write_outputs == 1:
                # check to see if file path exists
                out_dir = '/opt/ml/processing/data/preprocessing_infer/{}'.format(pipeline_runid)

                out_postfix = 'stg'+str(stg_as_num)+'-'+material_type
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

                print(str(stg_as_num), df_use_proc.shape)
                if df_use_proc.shape[0] > 0:
                    df_use_proc.to_csv(os.path.join(out_dir, 'infer_data_predictions_'+out_postfix+'.csv'), index=False)
        else:
            print("nothing done for stage", str(stg_as_num))


def make_inference_results_file(ap_data_sector, output_year):
    fpath = os.path.join(
        S3_DATA_PREFIX,
        ap_data_sector,
        'infer',
        str(output_year),
        'inference_results',
        'inference_results.csv'
    )
    file_exists = performance_helper.check_if_file_exists_s3(
        fname=fpath,
        bucket=S3_BUCKET
    )
    print(file_exists, fpath)

    if not file_exists or 1==1:
        print("making inference results file")
        out_dir = '/opt/ml/processing/data/preprocessing_infer/inference_results'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        pd.DataFrame().to_csv(os.path.join(out_dir, 'inference_results.csv'), index=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input inference data folder', required=True)
    parser.add_argument('--s3_input_preprocessor_folder', type=str,
                        help='s3 input preprocessor folder', required=True)
    parser.add_argument('--s3_input_model_folder', type=str,
                        help='s3 model folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        output_year = data['forward_model_year']
        pipeline_runid = data['target_pipeline_runid']

        try:
            # check for both entry and parent models regardless of data sector
            mat_types = ['entry', 'parent']

            for mat_type in mat_types:
                preprocess_infer(
                    ap_data_sector=ap_data_sector,
                    output_year=output_year,
                    material_type=mat_type,
                    pipeline_runid=pipeline_runid,
                    args=args
                )

            make_inference_results_file(
                ap_data_sector,
                output_year
            )

        except Exception as e:
            error_event(ap_data_sector, output_year, pipeline_runid, str(e))
            raise e




if __name__ == '__main__':
    main()