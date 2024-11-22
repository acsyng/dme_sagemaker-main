# import packages
import os
import sys
import json
import pandas as pd
import numpy as np
#import fasttreeshap
import shap
from scipy.spatial.distance import jensenshannon
from sklearn.calibration import CalibratedClassifierCV

import argparse

from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.performance_lib import performance_helper
from libs.postgres.postgres_connection import PostgresConnection
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
import multiprocessing as mp


def compute_shap_values(df_in, mdl_list, meta_info, batch_size=1000):

    x = df_in[meta_info['mdl_in_cols']].values
    samp_x = df_in[meta_info['mdl_in_cols']].sample(
        n=np.minimum(df_in.shape[0], 250), replace=False
    ).values
    shap_values = np.zeros((x.shape[0], x.shape[1], len(mdl_list)))

    ctx = mp.get_context("spawn")
    for i in range(len(mdl_list)):
        # get tree estimator if we calibrated the classifier
        if isinstance(mdl_list[i], CalibratedClassifierCV):
            mdl = mdl_list[i].estimator
        else:
            mdl = mdl_list[i]

        explainer = shap.TreeExplainer(
            model=mdl,
            data=samp_x,
            model_output='raw',
            feature_perturbation='interventional'
        )

        # parallelization
        with ctx.Pool(processes=mp.cpu_count()) as pool:
            # inputs are : x, y, tree limit, approximate, check_addivity, from_call
            args = [
                (x[start_idx:end_idx], None, None, False, False, False) # give all inputs to shap_values ...
                for start_idx, end_idx in zip(
                    range(0, len(x), batch_size),
                    range(batch_size, len(x) + batch_size, batch_size),
                )
            ]

            shap_values_list = pool.starmap(explainer.shap_values, args)
        shap_values_temp = np.concatenate([np.array(sv) for sv in shap_values_list])

        shap_values[:, :, i] = shap_values_temp

    mean_shap_values = np.round(np.mean(shap_values, axis=-1), 4)*100 # convert to same scale as output score

    # append mean shap vals to output file
    for i in range(len(meta_info['mdl_in_cols'])):
        df_in[meta_info['mdl_in_cols'][i] + '_shap'] = mean_shap_values[:, i]

    return df_in


def get_feature_importance(mdl_list, meta_info, imp_type='gain'):
    score_dict = {col: 0 for col in meta_info['mdl_in_cols']}
    for i in range(len(mdl_list)):
        # check if we have an xgboost model or if we calibrated the output
        # if calibrated, get estimator. if not, mdl_list[i] contains estimator
        if isinstance(mdl_list[i], CalibratedClassifierCV):
            temp_score = mdl_list[i].estimator.get_booster().get_score(importance_type=imp_type)
        else:
            temp_score = mdl_list[i].get_booster().get_score(importance_type=imp_type)

        for j in range(len(meta_info['mdl_in_cols'])):
            key = meta_info['mdl_in_cols'][j]
            if 'f'+str(j) in temp_score.keys():
                score_dict[key] = score_dict[key] + temp_score['f' + str(j)]

    total_score = np.sum(list(score_dict.values()))

    for key in score_dict:
        score_dict[key] = score_dict[key] / total_score

    return score_dict


def get_expected_n_entries(df_material, stage):
    df_material = df_material[df_material['stage'].astype(str) == stage] # stage is a string, df_material['stage'] might not be

    return df_material.shape[0], \
        np.sum((df_material['was_excluded'] == False) & (df_material['was_trialed'] == True))


def get_materials_per_decision_group(ap_data_sector, analysis_year, material_type):
    # analysis year in postgres table corresponds to harvest year... our year is planting year.
    # for some data sectors (LATAM), harvest year is different from planting year
    year_offset = 0
    if performance_helper.check_for_year_offset(ap_data_sector):
        year_offset = 1

    if material_type == 'entry':
        postgres_table = 'decision_group_entry'
    elif material_type == 'parent':
        postgres_table = 'decision_group_hybrid_parent'

    query_str = """
                    select distinct dg.decision_group_name, dge.be_bid as "entry_identifier", 
                        dg.stage as "stage"
                    from advancement.decision_group dg 
                    inner join advancement.{2} dge 
                        on dge.decision_group_id = dg.decision_group_id 
                    where dg.ap_data_sector_name = {0}
                        and dg.analysis_year = {1}
                        and dg.decision_group_type != 'MULTI_YEAR'
                """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    # get decision_group_name as source_id in case of single-exp decision groups
    query_str = """
                    select distinct dg.decision_group_name, 
                        dge.be_bid as "entry_identifier", 
                        dg.stage as "stage"
                    from advancement.decision_group dg 
                    inner join advancement.{2} dge 
                        on dge.decision_group_id = dg.decision_group_id 
                    where dg.ap_data_sector_name = {0}
                        and dg.analysis_year = {1}
                        and dg.decision_group_type != 'MULTI_YEAR'
                """.format("'" + ap_data_sector + "'", int(analysis_year) + year_offset, postgres_table)

    dc = PostgresConnection()
    material_list_df = dc.get_data(query_str)
    material_list_df = material_list_df.drop_duplicates()

    return material_list_df


def check_trial_pheno_missing_entries(entry_list, ap_data_sector, analysis_year, query_size=250):
    # entry list is a dataframe.
    output_df_list = []
    uniq_entry_list = pd.unique(entry_list['entry_identifier'])

    with SnowflakeConnection() as dc:
        for i in range(0,uniq_entry_list.shape[0],query_size):
            if i+query_size >= uniq_entry_list.shape[0]:
                entry_list_query = performance_helper.make_search_str(uniq_entry_list[i:])
            else:
                entry_list_query = performance_helper.make_search_str(uniq_entry_list[i:i+query_size])

            # look for year, and traits that we care about. Not an exhaustive list, but without those we can't really make a prediction
            query_str = """
                select entry_id as "entry_identifier", 
                    CAST(MIN(pr_exclude) as INTEGER) as "was_excluded"
                from rv_trial_pheno_analytic_dataset 
                where rv_trial_pheno_analytic_dataset.year={0}
                    and entry_id in {1}
                    and ap_data_sector = {2}
                    and trait_measure_code in ('YGSMN','MRTYN','GMSTP','TWSMN')
                group by entry_id 
            """.format(int(analysis_year), entry_list_query, "'" + ap_data_sector + "'")

            # get was excluded.
            # if entry not in list, then entry was not trialed this year
            output_df_list.append(dc.get_data(query_str))

    if len(output_df_list) > 0:
        output_df = pd.concat(output_df_list,axis=0)
        output_df['was_trialed'] = 1
        # get entries not in trial pheno as well
        output_df = output_df.merge(entry_list['entry_identifier'],on=['entry_identifier'],how='outer').fillna(0)
    else:
        output_df = pd.DataFrame(columns=['entry_identifier','was_trialed'])

    return output_df



def infer(ap_data_sector,
          eval_year,
          args,
          material_types,
          pipeline_runid,
          do_shap=1,
          write_outputs=1,
          write_output_message=1):
    ################ TO DO ########################
    # save to a single file with time stamps
    # if output is duplicated, do not append.
    # then write new step to get all unique outputs and push to postgres table
    # compute shap values and append to output
    out_msg = "Inference run report for {}, run {}: \n\n".format(ap_data_sector, pipeline_runid)

    if ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
        yield_stem = 'YSDMN'
    else:
        yield_stem = 'YGSMN'

    # load in all previous inference results. Use this to determine whether we need to make new inferences
    # and compute shap values
    inference_results_path = os.path.join(
        args.s3_input_inference_results_folder,
        'inference_results.csv'
    )
    inference_metrics_path = os.path.join(
        args.s3_input_inference_results_folder,
        'inference_metrics.csv'
    )

    if os.path.exists(inference_results_path) and os.path.getsize(inference_results_path) > 1: # check if non-empty
        print("loaded previous results")
        df_infer_results_all = pd.read_csv(
            inference_results_path
        )
    else:
        df_infer_results_all = pd.DataFrame()

    if os.path.exists(inference_metrics_path) and os.path.getsize(inference_metrics_path) > 1: # check if non-empty
        print("loaded previous results")
        df_metrics = pd.read_csv(
            inference_metrics_path
        )
    else:
        df_metrics = pd.DataFrame()

    # load in new inference data for each stage, save outputs per stage
    # do this per material_type
    df_mats_list = []

    for material_type in material_types:
        fpath = args.s3_input_inference_data_folder

        fnames = os.listdir(fpath)
        fnames.sort()
        infer_data_fnames = []
        stages = []
        for fname in fnames:
            fname_no_ext, ext = os.path.splitext(fname)
            if 'infer_data_predictions_' in fname and material_type in fname and 'stg' in fname:
                infer_data_fnames.append(fname)
                stg_idx = fname_no_ext.find('stg')
                end_stg_idx = fname_no_ext[stg_idx:].find('-') + stg_idx
                stg = fname_no_ext[stg_idx+3:end_stg_idx]
                stages.append(stg)

        print(list(zip(infer_data_fnames, stages)))

        # make inferences per stage
        for fname,stg in zip(infer_data_fnames, stages):
            # set stg as num correctly
            if len(stg) == 1:
                stg_as_num = int(stg)
            else:
                stg_as_num = float(stg)

            # load inference data
            df_infer_all = pd.read_csv(os.path.join(
                fpath,
                fname
            ))
            # store pipeline runid as time stamp of output
            df_infer_all['timestamp'] = pipeline_runid

            # get latest model for this stg and data sector.
            # load in model, preprocessing has already happened
            if stg == '4.1':
                mdl_fname, _, meta_fname = predictive_advancement_lib.get_fname_local(
                    fpath=args.s3_input_model_folder,
                    stage='4',
                    material_type=material_type
                )
            else:
                mdl_fname, _, meta_fname = predictive_advancement_lib.get_fname_local(
                    fpath=args.s3_input_model_folder,
                    stage=stg,
                    material_type=material_type
                )

            mdl_list = performance_helper.load_object(
                args.s3_input_model_folder,
                mdl_fname
            )
            meta_info = performance_helper.load_object(
                args.s3_input_model_folder,
                meta_fname
            )

            cols_to_check_same = meta_info['mdl_in_cols'].copy()
            cols_to_check_same.extend(['entry_identifier','ap_data_sector','decision_group'])

            # append new data to bottom of file, drop duplicates and keep first entry,
            # then make inferences on only the data remaining with the current timestamp
            df_infer_concat = pd.concat((df_infer_results_all, df_infer_all), axis=0).round(6).drop_duplicates(
                subset=cols_to_check_same,
                keep='first'
            )
            df_infer_all = df_infer_concat[(df_infer_concat['timestamp'] == pipeline_runid) &
                                           (df_infer_concat['dg_stage'] == stg_as_num)]

            if df_infer_all.shape[0] > 0:
                # do not need to run preprocessing, this is done in 'preprocessing_infer'
                y_proba = predictive_advancement_lib.predict_proba_list(
                    mdl_list,
                    df_infer_all[meta_info['mdl_in_cols']].values
                )

                df_infer_all['recommendation_score'] = y_proba
                # use thresholds set during training to get recommendation text
                df_infer_all['recommendation'] = ''
                if 'review_thresh' in meta_info.keys() and 'adv_thresh' in meta_info.keys():
                    df_infer_all['recommendation'] = 'drop'
                    df_infer_all['recommendation'][df_infer_all['recommendation_score'] > meta_info['review_thresh']] = 'review'
                    df_infer_all['recommendation'][df_infer_all['recommendation_score'] > meta_info['adv_thresh']] = 'advance'
                df_infer_all['recommendation'][df_infer_all['is_check'] == 1] = 'is_check'

                # compute shap values
                if do_shap == 1:
                    df_infer_all = compute_shap_values(df_infer_all, mdl_list, meta_info)

                # compute metrics related to predictions:
                # too many missing values?
                feature_importance = get_feature_importance(mdl_list, meta_info)
                weighted_missing = np.matmul(
                    np.isnan(df_infer_all[meta_info['mdl_in_cols']].values),
                    np.array(list(feature_importance.values())).reshape(-1,1)
                )
                df_infer_all['weighted_missing'] = weighted_missing

                # yield col missing:
                yield_col = [col for col in meta_info['mdl_in_cols'] if yield_stem in col][0]
                df_infer_all['yield_missing'] = np.isnan(df_infer_all[yield_col].values)

                # append new inferences to inference file
                df_infer_results_all = pd.concat((df_infer_results_all, df_infer_all), axis=0)

        # get metrics and flag any missing materials per stage (and per DG)
        # this is the same for loop as above. Needs to be done after in case a material is in
        # multiple stages. Then, we need all predictions for all stages done, then check if material exists.
        df_material_list = get_materials_per_decision_group(
            ap_data_sector=ap_data_sector,
            analysis_year=eval_year,
            material_type=material_type
        )

        for fname, stg in zip(infer_data_fnames, stages):
            # get latest model for this stg and data sector.
            # load in meta info to get yield col and to drop duplicates.

            # set stg as num,handle stage 4.1
            if len(stg) == 1:
                stg_as_num = int(stg)
            else:
                stg_as_num = float(stg)

            if stg == '4.1':
                _, _, meta_fname = predictive_advancement_lib.get_fname_local(
                    fpath=args.s3_input_model_folder,
                    stage='4',
                    material_type=material_type
                )
            else:
                _, _, meta_fname = predictive_advancement_lib.get_fname_local(
                    fpath=args.s3_input_model_folder,
                    stage=stg,
                    material_type=material_type
                )
            meta_info = performance_helper.load_object(
                args.s3_input_model_folder,
                meta_fname
            )

            cols_to_check_same = meta_info['mdl_in_cols'].copy()
            cols_to_check_same.extend(['entry_identifier', 'ap_data_sector','decision_group'])
            if 'material_type' in df_infer_results_all.columns:
                cols_to_check_same.append('material_type')

            # compute metrics on inferred data
            # size, prediction and trait distributions
            # use latest predictions for data from this stage

            df_infer_all = df_infer_results_all[
                (df_infer_results_all['dg_stage'] == stg_as_num) &
                (df_infer_results_all['material_type_simple'] == material_type)
            ].drop_duplicates(
                subset=cols_to_check_same,
                keep='last'
            )

            yield_col = [col for col in meta_info['mdl_in_cols'] if yield_stem in col][0]

            # compute useful metrics and whether materials are missing
            metrics = {}  # do not put each metric in a list, will be done when making dataframe
            metrics['pipeline_runid'] = pipeline_runid
            metrics['stage'] = stg
            metrics['yield_col'] = yield_col
            metrics['n_miss'] = 0
            # do we have all expected materials?
            # output list of missing
            if df_material_list.shape[0] > 0:
                decision_group_list = df_material_list[df_material_list['stage'].astype(str) == str(stg)]['decision_group_name'].drop_duplicates()
                df_mats_temp = df_material_list[df_material_list['stage'].astype(str) == str(stg)][['entry_identifier']].drop_duplicates()
                # get whether an expected material was in rv_trial_pheno_analytic_dataset
                df_mats_temp = check_trial_pheno_missing_entries(
                    entry_list=df_mats_temp,
                    ap_data_sector=ap_data_sector,
                    analysis_year=eval_year
                )

                # get inferred mats within all decision groups
                df_infer_mats_list = []
                for decision_group in decision_group_list:
                    df_infer_mats_list.append(
                        df_infer_all[df_infer_all['decision_group'] == decision_group][['entry_identifier']].drop_duplicates()
                    )

                df_infer_mats = pd.concat(df_infer_mats_list,axis=0).drop_duplicates()
                df_infer_mats['was_inferred'] = 1
                df_mats_temp = df_mats_temp.merge(df_infer_mats,how='left',on=['entry_identifier']).fillna(0)

                if 'was_inferred' not in df_mats_temp.columns:
                    df_mats_temp['was_inferred'] = 0

                # count how many materials are missing
                metrics['n_miss'] = np.sum((df_mats_temp['was_inferred'] == 0) & (df_mats_temp['was_trialed'] == 1) & (df_mats_temp['was_excluded'] == 0))
                # store all materials and flags
                df_mats_list.append(df_mats_temp)
            else:
                metrics['n_miss'] = np.nan

            if 'train_metrics' in meta_info.keys():
                train_metrics = meta_info['train_metrics']
                yield_bins = train_metrics['yield_bins']
                pred_bins = train_metrics['pred_bins']
                missing_bins = train_metrics['missing_bins']

                if 'yield_thresh' in train_metrics.keys():
                    yield_thresh = train_metrics['yield_thresh']
                else:
                    yield_thresh = 0.3
                if 'pred_thresh' in train_metrics.keys():
                    pred_thresh = train_metrics['pred_thresh']
                else:
                    pred_thresh = 0.3
                if 'missing_thresh' in train_metrics.keys():
                    missing_thresh = train_metrics['missing_thresh']
                else:
                    missing_thresh = 0.3

            else: # compute metrics regardless, use default bins, store nan for stat tests
                yield_bins = 'auto'
                pred_bins = 'auto'
                missing_bins = 'auto'

            pred_count, pred_bins = np.histogram(
                df_infer_all['recommendation_score'],
                bins=pred_bins,
                range=(np.nanmin(df_infer_all['recommendation_score']), np.nanmax(df_infer_all['recommendation_score']))
            )
            yield_count, yield_bins = np.histogram(
                df_infer_all[yield_col],
                bins=yield_bins,
                range=(np.nanmin(df_infer_all[yield_col]), np.nanmax(df_infer_all[yield_col]))
            )
            # fraction missing inputs per row (again bin counts and edges)
            n_missing = np.sum(np.isnan(df_infer_all[meta_info['mdl_in_cols']].values), axis=1)
            missing_count, missing_bins = np.histogram(
                n_missing,
                bins=missing_bins,
                density=False,
                range=(0, len(meta_info['mdl_in_cols']))
            )

            if 'train_metrics' in meta_info.keys():
                pred_stat = jensenshannon(pred_count, train_metrics['pred_count'])
                pred_flag = pred_stat > pred_thresh
                yield_stat = jensenshannon(yield_count, train_metrics['yield_count'])
                yield_flag = yield_stat > yield_thresh
                missing_stat = jensenshannon(missing_count, train_metrics['missing_count'])
                missing_flag = missing_stat > missing_thresh

                print(yield_stat, pred_stat, missing_stat)
            else:
                pred_stat = np.nan
                yield_stat = np.nan
                missing_stat = np.nan

                pred_flag, yield_flag, missing_flag = np.nan, np.nan, np.nan

            metrics['pred_stat'] = pred_stat
            metrics['pred_flag'] = pred_flag
            metrics['yield_stat'] = yield_stat
            metrics['yield_flag'] = yield_flag
            metrics['missing_stat'] = missing_stat
            metrics['missing_flag'] = missing_flag

            # how to save count and bins....?
            metrics['pred_count'] = pred_count
            metrics['pred_bins'] = pred_bins
            metrics['yield_count'] = yield_count
            metrics['yield_bins'] = yield_bins
            metrics['missing_count'] = missing_count
            metrics['missing_bins'] = missing_bins

            df_metrics = pd.concat(
                (
                    df_metrics,
                    pd.DataFrame(data={key : [metrics[key]] for key in metrics.keys()})
                 ),
                axis=0
            )

            # output message:
            # Stage, number of materials, number missing?, R2 between yield and rec score.
            # put R2 between yield and rec score in output
            df_r2 = df_infer_all[[yield_col, 'recommendation_score']].dropna()
            n_total = df_infer_all.shape[0]
            n_yield = np.sum(df_infer_all[yield_col].notna())
            n_rec_score = np.sum(df_infer_all['recommendation_score'].notna())
            rec_range = np.round(np.nanpercentile(df_infer_all['recommendation_score'], [2.5, 97.5]), 3)
            out_msg = out_msg + "Stage {} {}: Yield col = {} \n".format(
                stg,
                material_type,
                yield_col
            )

            out_msg = out_msg + "# total = {}, # with yield = {}, # with rec score = {} \n".format(
                str(n_total),
                str(n_yield),
                str(n_rec_score)
            )

            out_msg = out_msg + "2.5th and 97.5th percentile score: {}, {} \n".format(
                str(rec_range[0]),
                str(rec_range[1])
            )

            out_msg = out_msg + "Pearson r between yield and rec score = {} \n".format(
                str(np.round(df_r2.corr(method='pearson').iloc[0,1], 2))
            )

            out_msg = out_msg + "Spearman r between yield and rec score = {} \n\n".format(
                str(np.round(df_r2.corr(method='spearman').iloc[0, 1], 2))
            )

    # concat all materials
    if len(df_mats_list) > 0:
        df_mats = pd.concat(df_mats_list,axis=0)
    else:
        df_mats = pd.DataFrame()

    # write all inferences
    if write_outputs == 1:
        # check to see if file path exists
        out_dir = '/opt/ml/processing/data/infer/inference_results'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        # setup filenames
        infer_data_fname = 'inference_results.csv'
        metrics_fname = 'inference_metrics.csv'
        # save all data
        df_infer_results_all.to_csv(os.path.join(out_dir, infer_data_fname), index=False)
        df_metrics.to_csv(os.path.join(out_dir, metrics_fname), index=False)

        # save run specific material list
        out_dir = '/opt/ml/processing/data/infer/' + pipeline_runid
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        # setup filenames
        material_list_fname = 'adv_model_infer_expected_entries.csv'
        df_mats.to_csv(os.path.join(out_dir, material_list_fname), index=False)

    if write_output_message:
        performance_helper.performance_create_event(
            ap_data_sector=ap_data_sector,
            target_pipeline_runid=pipeline_runid,
            status="RUN_REPORT",
            message=out_msg
        )


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_model_folder', type=str,
                        help='s3 input training data folder', required=True)
    parser.add_argument('--s3_input_inference_data_folder', type=str,
                        help='s3 input inference data folder', required=True)
    parser.add_argument('--s3_input_inference_results_folder', type=str,
                        help='s3 input inference results folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        eval_year = data['forward_model_year']
        pipeline_runid = data['target_pipeline_runid']

        try:
            # check for both parent and hybrid models automatically
            mat_types = ['entry','parent']

            infer(
                ap_data_sector,
                eval_year,
                args=args,
                material_types=mat_types,
                pipeline_runid=pipeline_runid
            )

        except Exception as e:
            error_event(ap_data_sector, eval_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()