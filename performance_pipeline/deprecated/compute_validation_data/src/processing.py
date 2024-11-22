import pandas as pd
import numpy as np
import json
import argparse
import shap

import os
from libs.performance_lib import predictive_advancement_lib
from libs.event_bridge.event import error_event

def compute_validation_data(DKU_DST_ap_data_sector,
                                output_year,
                                DKU_DST_analysis_type,
                                pipeline_runid,
                                args,
                                write_outputs=1):
    try:
        # Read recipe inputs
        df_rec_summary = pd.read_csv(os.path.join(args.s3_input_model_predictions_folder,
                                                 DKU_DST_ap_data_sector,
                                                 DKU_DST_analysis_type,
                                                 output_year,
                                                 'model_predictions.csv'))

        # only look at materials in stage 3, remove any checks
        df_rec_summary = df_rec_summary[(df_rec_summary['current_stage'] < 7)]
        # we have decision groups for 2022 and beyond for Soy. Not for historical data
        if DKU_DST_ap_data_sector=="SOY_NA_SUMMER" and output_year>=2022:
            df_rec_summary = df_rec_summary[df_rec_summary['decision_group'] != 'na']

        # get recommendations for each method based on number of actually advanced materials
        # group by grouping vars, then compute metrics for each group

        grouping_vars = ['ap_data_sector', 'analysis_year', 'current_stage', 'decision_group', \
                         'material_type', 'decision_group_rm']
        # trait group contains nan's, which screws this up. Replace
        df_rec_summary['trait_group'] = df_rec_summary['trait_group'].fillna(value='na')

        # group by grouping vars, then compute metrics for each group
        adv_col = 'was_adv'
        adv_next_col = 'was_adv_next'

        df_groups = df_rec_summary[grouping_vars].drop_duplicates()
        df_rec_norm = []
        for index, row in df_groups.iterrows():
            # make sure the number of recommended materials is the same across methods
            # split by a group of columns when doing this
            # also compute percentile-rank within each decision group rm

            df_rec_group = df_rec_summary[np.all(df_rec_summary[grouping_vars] == row, axis=1)]

            n_rec = np.sum(df_rec_group[adv_col])
            n_adv_rec = np.minimum(np.sum((df_rec_group[adv_col]) & (df_rec_group[adv_next_col])),
                                   n_rec)  # can't recommend more than what was advanced next year

            df_rec_group = df_rec_group.sort_values(by='recommendation_score', ascending=False)
            df_rec_group['recommendation_normRec'] = False
            df_rec_group['recommendation_percentile'] = 0

            if output_year != 2022 or row['decision_group'] != 'na':
                df_rec_group['recommendation_normRec'].iloc[0:n_rec] = True
                df_rec_group['recommendation_percentile'] = 100 * (
                            1 - np.arange(0, df_rec_group.shape[0]) / df_rec_group.shape[0])

            #### get normalized recommendations for next year
            # only recommend materials that were advanced next year
            df_rec_group['recommendation_nextNormRec'] = False
            if output_year != 2022 or row['decision_group'] != '':
                df_rec_group_adv = df_rec_group[df_rec_group['was_adv'] == True]
                df_rec_group_notadv = df_rec_group[df_rec_group['was_adv'] == False]
                # update the recommendation for materials that were advanced. Sorting should be preserved after split.
                df_rec_group_adv['recommendation_nextNormRec'].iloc[0:n_adv_rec] = True

            # merge together
            df_rec_group = pd.concat([df_rec_group_adv, df_rec_group_notadv], axis=0)

            # store data with new recommendations as an output
            df_rec_norm.append(df_rec_group)

        # concat data with new recommendations
        df_out = pd.concat(df_rec_norm, axis=0)

        if write_outputs == 1:
            # check to see if file path exists
            out_dir = '/opt/ml/processing/data/validation_data/{}/{}/{}'.format(
                    DKU_DST_ap_data_sector, DKU_DST_analysis_type, output_year)
            out_fname = 'validation_data.csv'

            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            df_out.to_csv(os.path.join(out_dir,out_fname), index=False)

        return df_out

    except Exception as e:
        error_event(DKU_DST_ap_data_sector, output_year, pipeline_runid, str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_model_predictions_folder', type=str,
                        help='s3 input model_predictions folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_output_year = data['output_year']
        DKU_DST_analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        test_flag = data['test_flag']

        if test_flag == '1':
            print('generating validation dataset for output year')
            df_out = compute_validation_data(DKU_DST_ap_data_sector=DKU_DST_ap_data_sector,
                                        output_year=DKU_DST_output_year,
                                        DKU_DST_analysis_type=DKU_DST_analysis_type,
                                        pipeline_runid=pipeline_runid,
                                        args=args)
        else:
            print('skipping validation step')


if __name__ == "__main__":
    main()