# import packages
import os
import json
import pandas as pd
import numpy as np
import argparse

from libs.config.config_vars import CONFIG
from libs.postgres.postgres_connection import PostgresConnection
from libs.event_bridge.event import error_event, create_event
from libs.performance_lib import performance_helper

ADV_REC_OUTPUT_UPSERT = '''INSERT INTO dme.advancement_recommender_output
(
    ap_data_sector,
    analysis_year,
    decision_group,
    current_stage,
    pipeline_runid,
    entry_id,
    material_type,
    adv_rec_score,
    recommendation,
    adv_rec_warning,
    shap_explanation
)
SELECT ap_data_sector,
    analysis_year,
    decision_group,
    current_stage,
    pipeline_runid,
    entry_id,
    material_type,
    adv_rec_score,
    recommendation,
    adv_rec_warning,
    shap_explanation
FROM ( 
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY
            ap_data_sector, 
            analysis_year, 
            decision_group, 
            current_stage, 
            entry_id, 
            material_type
            ORDER BY pipeline_runid DESC) as row_number
    FROM
        public.{}
) as "temp"
where row_number=1
ON CONFLICT
(
    ap_data_sector,
    analysis_year,
    decision_group,
    entry_id,
    material_type,
    current_stage
)
DO UPDATE
SET
    pipeline_runid = EXCLUDED.pipeline_runid,
    adv_rec_score = EXCLUDED.adv_rec_score,
    recommendation = EXCLUDED.recommendation,
    adv_rec_warning = EXCLUDED.adv_rec_warning,
    shap_explanation = EXCLUDED.shap_explanation
'''

ADV_REC_OUTPUT_DELETE = """
    DELETE FROM dme.advancement_recommender_output
    WHERE ap_data_sector = '{0}'
    AND analysis_year = {1}
"""


def generate_shap_explanation(df, n_top=4):
    ### provide a dataframe with data from a single stage
    # generate string explaining decisions. do this per stage
    # explanation contains N most impactful vars

    # useful variables
    pot_shap_cols = df.columns[['shap' in col for col in df.columns]]
    pot_shap_cols = pot_shap_cols[[('result' in col) or ('prediction' in col) for col in pot_shap_cols]]
    shap_cols = list(pot_shap_cols[[df[col].count() > 0 for col in pot_shap_cols]])
    print(shap_cols)
    if 'decision_group_rm_shap' in shap_cols:
        shap_cols.remove('decision_group_rm_shap')

    trait_cols = [col.replace('_shap', '') for col in shap_cols]
    trait_names = [col.replace('prediction_','').replace('result_', '').replace('diff_', '').replace('perc_', '') for col in trait_cols]
    shap_values = df[shap_cols].fillna(0).values

    # don't try to output more columns than we have
    n_top = min(n_top, shap_values.shape[1] - 1)
    print(n_top)

    # get normalized trait columns
    trait_values_norm = df[trait_cols].values
    col_means = np.nanmean(trait_values_norm, axis=0)
    col_stds = np.nanstd(trait_values_norm, axis=0)
    for i in range(trait_values_norm.shape[1]):
        if col_stds[i] != 0:
            trait_values_norm[:, i] = (trait_values_norm[:, i] - col_means[i]) / col_stds[i]
        else:
            trait_values_norm[:, i] = 0

    # sort features by magnitude of shapley values
    most_impactful_names = []
    most_impactful_vals = np.zeros((shap_values.shape[0], shap_values.shape[1]))
    most_impactful_shap_vals = np.zeros((shap_values.shape[0], shap_values.shape[1]))

    for i_te in range(shap_values.shape[0]):
        # get n_top most important features. argsort sorts in ascending order, so flip
        sort_impact_idx = np.flip(np.argsort(np.abs(shap_values[i_te, :])))
        most_impactful_names.append([trait_names[sort_idx] for sort_idx in sort_impact_idx])

        # get shapley values to put into context
        most_impactful_shap_vals[i_te,:] = shap_values[i_te,sort_impact_idx].reshape((-1,))
        most_impactful_vals[i_te, :] = trait_values_norm[i_te, sort_impact_idx].reshape((-1,))
    most_impactful_names = np.array(most_impactful_names)

    # build string explaining model output
    explain_out = []

    high_dict = {0: 'very low', 1: 'low', 2: 'moderate', 3: 'high', 4: 'very high'}
    wet_dict = {0: 'very dry', 1: 'dry', 2: 'moderate', 3: 'wet', 4: 'very wet'}
    tall_dict = {0: 'very short', 1: 'short', 2: 'moderate', 3: 'tall', 4: 'very tall'}

    trait_to_term_dict = {'YGSMN-PLHTN-ERHTN-GMSTP': high_dict, 'YGSMN': high_dict, 'PLHTN': tall_dict,
                          'ERHTN': tall_dict, \
                          'GMSTP': wet_dict, 'TWSMN': high_dict, 'GRSNP': high_dict, 'reliability': high_dict, \
                          'STKLP': high_dict, 'ERTLP': high_dict, 'LRTLP': high_dict, 'LP': high_dict}

    for i_te in range(most_impactful_vals.shape[0]):
        temp_out = ""
        if not np.all(np.isnan(shap_values[i_te, :])):
            for i_feat in range(n_top):
                if most_impactful_names[i_te, i_feat] == 'YGSMN_LRsub' or most_impactful_names[i_te,i_feat] == 'YGSMN_residual':
                    temp_out += 'YMH resid'
                else:
                    temp_out += most_impactful_names[i_te, i_feat]

                if np.isnan(most_impactful_vals[i_te,i_feat]):
                    temp_out += ': missing, '
                else:
                    # generate 5 categories. Trait values are normalized, so do by standard dev
                    category_val = 0  # lowest
                    if most_impactful_vals[i_te, i_feat] > -1.25:
                        category_val = 1
                    if most_impactful_vals[i_te, i_feat] > -0.5:
                        category_val = 2
                    if most_impactful_vals[i_te, i_feat] > 0.5:
                        category_val = 3
                    if most_impactful_vals[i_te, i_feat] > 1.25:
                        category_val = 4

                    # get description dict from trait name
                    if most_impactful_names[i_te, i_feat] in trait_to_term_dict:
                        dict_to_use = trait_to_term_dict[most_impactful_names[i_te, i_feat]]
                    else:
                        dict_to_use = high_dict
                    temp_out += ': ' + dict_to_use[category_val] + ', '
            temp_out = temp_out[:-2]  # remove final , and space
        explain_out.append(temp_out)

    return explain_out, trait_cols


def compute_output_metrics(df_in):
    # compute # entries, prediction percentiles, input trait distributions

    return pd.DataFrame()


def push_to_postgres(args, analysis_year, force_refresh=False):
    # load in inference results, drop duplicates, keep oldest record
    inference_results_path = os.path.join(
        args.s3_input_inference_results_folder,
        'inference_results.csv'
    )

    if os.path.exists(inference_results_path) and os.path.getsize(inference_results_path) > 1: # check if non-empty
        df_infer = pd.read_csv(inference_results_path)
        if 'decision_group' not in df_infer.columns:
            df_infer['decision_group'] = 'na'
        if 'material_type' not in df_infer.columns:
            df_infer['material_type'] = df_infer['material_type_simple']

        meta_cols = ['ap_data_sector','analysis_year','decision_group','dg_stage',
                     'entry_identifier','material_type','timestamp']
        # only keep 1 record per entry
        df_infer = df_infer.drop_duplicates(
            subset=meta_cols,
            keep='last'
        )

        # rename columns before upsert
        df_infer = df_infer.rename(columns={
            'timestamp': 'pipeline_runid',
            'entry_identifier': 'entry_id',
            'recommendation_score': 'adv_rec_score'
        })

        # update analysis year if ap data sector is LAS, LAN, BRAZIL
        # Performance pipeline uses planting year throughout, output needs to be harvest year.
        ap_data_sector = df_infer['ap_data_sector'].iloc[0]
        if performance_helper.check_for_year_offset(ap_data_sector) == True:
            df_infer['analysis_year'] = df_infer['analysis_year'] + 1

        # get shap explanations
        df_shap_list = []
        for stg in pd.unique(df_infer['dg_stage']):
            df_stg = df_infer[df_infer['dg_stage'] == stg]
            explain_out, trait_cols = generate_shap_explanation(df_stg)

            df_stg['shap_explanation'] = explain_out
            df_stg['mdl_cols'] = ''
            for i_df in range(df_stg.shape[0]):
                df_stg['mdl_cols'].iloc[i_df] = trait_cols
            df_shap_list.append(df_stg)

        df_out = pd.concat(df_shap_list,axis=0)

        # get warnings
        # too much missing data: if weighted missing > thresh, write warning
        # yield missing: if yield_col is nan, write warning
        # outlier flag: if col not nan and is outlier: warning
        warning_list = []
        for i_row in range(df_infer.shape[0]):
            temp_warning = ''
            if df_out.iloc[i_row]['weighted_missing'] > 0.75:
                temp_warning += 'many missing inputs, '

            if temp_warning != '':  # we have a problem, drop score, recommendation, and explanation
                df_out['adv_rec_score'].iloc[i_row] = np.nan
                df_out['recommendation'].iloc[i_row] = ''
                df_out['shap_explanation'].iloc[i_row] = ''

            warning_list.append(temp_warning[:-2])  # remove ", "

        df_out['adv_rec_warning'] = warning_list

        # order df_out columns to make output more accessible
        df_out = performance_helper.order_output_columns(df_out)

        # format rec score
        df_out['adv_rec_score'] = np.round(df_out['adv_rec_score'] * 100, 1)

        # check output columns
        for col in ['adv_rec_warning','recommendation','shap_explanation']:
            if col not in df_out.columns:
                df_out[col] = ''

        # check to see if file path exists
        out_dir = '/opt/ml/processing/data/push_to_postgres/'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        # setup filenames
        fname = 'adv_rec_out.csv'
        # save all data
        df_out.to_csv(os.path.join(out_dir, fname), index=False)

        # use dg_stage instead of current_stage
        df_out['current_stage'] = df_out['dg_stage']

        pc = PostgresConnection()
        if force_refresh: # delete table, then upsert.
            pc.run_sql(
                ADV_REC_OUTPUT_DELETE.format(ap_data_sector, analysis_year)
            )
        # upsert new data
        pc.upsert(ADV_REC_OUTPUT_UPSERT, 'advancement_recommender_output', df_out, is_spark=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_inference_results_folder', type=str,
                        help='s3 input inference results folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        eval_year = data['forward_model_year']
        pipeline_runid = data['target_pipeline_runid']
        force_refresh = data['force_refresh']
        try:
            push_to_postgres(args,analysis_year=eval_year,force_refresh=force_refresh)

            create_event(CONFIG.get('event_bus'), ap_data_sector, eval_year, pipeline_runid,
                         'performance', 'null', 'END',
                         f'End Sagemaker DME Performance pipeline: {pipeline_runid}')
        except Exception as e:
            error_event(ap_data_sector, eval_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
