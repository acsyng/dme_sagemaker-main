# import packages
import os
import sys
import json
import pandas as pd, numpy as np
import argparse
import warnings
from sklearn.calibration import CalibratedClassifierCV
import shap

import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.backends.backend_pdf import PdfPages

from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.performance_lib import performance_helper
from libs.postgres.postgres_connection import PostgresConnection
from libs.denodo.denodo_connection import DenodoConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
import multiprocessing as mp

ADV_METRICS_DROP = '''
    DELETE from dme.advancement_recommender_metrics
    WHERE ap_data_sector = {0}
        and analysis_year = {1}
        and stage = {2}
'''

ADV_METRICS_UPSERT = '''INSERT INTO dme.advancement_recommender_metrics
(
    ap_data_sector,
    analysis_year,
    metric_group,
    stage,
    model_id,
    metric,
    value,
    alpha_value,
    model_type,
    material_type,
    trait
)
SELECT
    ap_data_sector,
    analysis_year,
    metric_group,
    stage,
    MAX(model_id),
    metric,
    MAX(value),
    MAX(alpha_value),
    model_type,
    material_type,
    trait
FROM
    public.{}
GROUP BY 
    ap_data_sector,
    analysis_year,
    metric_group,
    stage,
    metric,
    model_type,
    material_type,
    trait
ON CONFLICT
(
    ap_data_sector,
    analysis_year,
    metric_group,
    stage,
    metric,
    model_type,
    material_type,
    trait
)
DO UPDATE
SET
    model_id = EXCLUDED.model_id,
    value = EXCLUDED.value,
    alpha_value = EXCLUDED.alpha_value
'''


def get_dme_metric_output(ap_data_sector, input_years, material_type):
    # input years is a list of strings...we want (2023,2024), so cast to int
    input_years = [int(yr) for yr in input_years]
    if performance_helper.check_for_year_offset(ap_data_sector) == True:
        # add 1 to analysis year, we use a different standard than everyone else....
        input_years = [yr+1 for yr in input_years]

    input_years_str = performance_helper.make_search_str(input_years)

    # we want the 'all' breakout, which doesn't seem to exist everywhere historically. When it's not there,
    # average over all other breakouts...
    for include_breakouts in [False, True]:
        query_str = """
            select ap_data_sector ,
                analysis_year ,
                analysis_type ,
                stage as "current_stage",
                entry_id as "entry_identifier",
                avg(abs_mean_pctchk) as "abs_mean_pctchk",
                avg(abs_mean_prob) as "abs_mean_prob"
            from (
                select ap_data_sector ,
                    analysis_year ,
                    analysis_type ,
                    first(pipeline_runid) as "pipeline_runid",
                    source_id ,
                    stage ,
                    decision_group_rm ,
                    entry_id ,
                    first (abs_mean_pctchk) as "abs_mean_pctchk" ,
                    first (abs_mean_prob) as "abs_mean_prob"
                from advancement.dme_output 
                where ap_data_sector = {0}
                    and analysis_year in {1}
                    and analysis_type in ('MultiExp','SingleExp','GenoPred','PhenoGCA')
        """
        # handle material type search...
        if material_type == 'parent':
            query_str = query_str + """ and material_type in ('pool1','pool2','male','female')
            """
        else:
            query_str = query_str + """ and material_type = 'entry'
            """
        query_str = query_str + """
                    and check_entry_id is null 
                    and metric = 'advancement'
                    and trait = 'aggregate'
                    and metric_type = 'summary'
        """
        if include_breakouts:
            query_str = query_str + """
                and breakout_level_1 = 'na'
                and breakout_level_2 = 'na'
                and breakout_level_3 = 'na'
                and breakout_level_4 = 'na'
            """
        else:
            query_str = query_str + """
                and (breakout_level_1 != 'na' or breakout_level_2 != 'na')
                and breakout_level_3 = 'na'
                and breakout_level_4 = 'na'
            """

        query_str = query_str + """
            group by ap_data_sector , 
                analysis_year ,
                analysis_type , 
                source_id , 
                stage, 
                decision_group_rm , 
                entry_id 
            order by pipeline_runid desc
        ) "first_dme"
        group by ap_data_sector , 
            analysis_year ,
            analysis_type , 
            stage, 
            entry_id 
        """

        query_str = query_str.format(
            "'" + ap_data_sector + "'",
            input_years_str,
            "'" + material_type+ "'"
        )

        with SnowflakeConnection() as dc:
            df = dc.get_data(query_str)

        # process df
        df_multi = df[df['analysis_type'] == 'MultiExp']
        df_single = df[df['analysis_type'] == 'SingleExp']
        df_geno = df[df['analysis_type'] == 'GenoPred']

        # take multi first, then single. Drop any duplicates
        df_temp = pd.concat((df_multi, df_single, df_geno)).drop_duplicates(subset=['ap_data_sector','current_stage','entry_identifier'])

        # average, concat, do drop duplicates to keep first (prioritizing 'all' breakout)
        if include_breakouts:
            df_temp = df_temp.groupby(by=['ap_data_sector','analysis_year','analysis_type','current_stage','entry_identifier']).mean().reset_index()
            df_out = pd.concat((df_out,df_temp),axis=0)
            df_out = df_out.drop_duplicates(subset=['ap_data_sector', 'current_stage', 'entry_identifier'])
        else:
            df_out = df_temp

    # adjust year for certain data sectors
    if performance_helper.check_for_year_offset(ap_data_sector):
        df_out['analysis_year'] = df_out['analysis_year'] - 1

    # if abs mean prob is mostly empty, use abs mean pctchk instead?
    if np.sum(df_out['abs_mean_prob'].isna()) > 0.75*df_out.shape[0]:
        df_out['abs_mean_prob'] = df_out['abs_mean_pctchk']

    df_out = df_out.drop(columns='abs_mean_pctchk')

    return df_out


def create_subtitle(fig, grid, title: str):
    "Sign sets of subplots with title"
    row = fig.add_subplot(grid)
    # the '\n' is important
    row.set_title(f'{title}\n', fontweight='semibold',y=0.9,pad=0)
    # hide subplot
    row.set_frame_on(False)
    row.axis('off')


def get_confusion_matrix_as_text(df_in):
    cell_text = [
        [
            str(int(df_in['value'][df_in['metric'] == 'TP'].values)),
            str(int(df_in['value'][df_in['metric'] == 'FP'].values))
        ],
        [
            str(int(df_in['value'][df_in['metric'] == 'FN'].values)),
            str(int(df_in['value'][df_in['metric'] == 'TN'].values))
        ]
    ]
    return cell_text


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

    return mean_shap_values, x


def make_model_performance_card(
                out_path,
                mdl_list,
                meta_info,
                mdl_id,
                df_data,
                df_adv,
                ap_data_sector,
                material_type,
                stage, # comes in as a string
                max_traits = 9
):
    out_fname = 'model_card_'+'stage'+stage+'_'+material_type+'_'+mdl_id+'.pdf'

    # useful constants
    title_fontsize = 10
    bar_width = 0.9
    conf_bbox = [0.05, 0.15, 0.95, 0.8]
    n_rows_per_page = 5
    figsize=(7.5,10)
    max_points = 8000

    shap_vals, shap_data = compute_shap_values(
        df_in=df_data,
        mdl_list=mdl_list,
        meta_info=meta_info
    )

    # Create the PdfPages object to which we will save the pages:
    # The with statement makes sure that the PdfPages object is closed properly at
    # the end of the block, even if an Exception occurs.
    with PdfPages(os.path.join(out_path, out_fname)) as pdf:
        # make giant matplotlib figure, save as pdf
        fig = plt.figure(layout="constrained", figsize=(7.5, 6))
        fig.suptitle('Advancement recommender engine performance \n ' + ap_data_sector + ', stage ' +
                     stage + ', ID ' + mdl_id + ' \n', fontsize=12)

        gs = GridSpec(3, 2, figure=fig)
        ax_mdl_conf = fig.add_subplot(gs[0, 0])
        ax_yield_conf = fig.add_subplot(gs[0, 1])
        ax_mdl_hist = fig.add_subplot(gs[1, 0])
        ax_acc_year = fig.add_subplot(gs[1, 1])
        ax_acc_mg = fig.add_subplot(gs[2, 0])
        ax_fn_vs_thresh = fig.add_subplot(gs[2, 1])

        ##### make confusion matrices
        row_labels = ['Adv', 'Drop']
        col_labels = ['Rec adv', 'Rec drop']

        dg = 'all'
        mdl = 'ML'
        cell_text = get_confusion_matrix_as_text(df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == dg)])
        # hide axes
        ax_mdl_conf.axis('off')
        ax_mdl_conf.axis('tight')

        mdl_conf_table = ax_mdl_conf.table(
            cellText=cell_text,
            cellLoc='center',
            rowLabels=row_labels,
            colLabels=col_labels,
            loc='center',
            bbox=conf_bbox  # seemingly a proportion of axes size?
        )
        ax_mdl_conf.set_title('Rec Engine', fontsize=title_fontsize, pad=0)

        # make yield confusion matrix
        dg = 'all'
        mdl = 'yield'
        cell_text = get_confusion_matrix_as_text(df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == dg)])
        # hide axes
        ax_yield_conf.axis('off')
        ax_yield_conf.axis('tight')

        yield_conf_table = ax_yield_conf.table(
            cellText=cell_text,
            cellLoc='center',
            rowLabels=row_labels,
            colLabels=col_labels,
            loc='center',
            bbox=conf_bbox  # seemingly a proportion of axes size?
        )
        ax_yield_conf.set_title('Yield-only', fontsize=title_fontsize, pad=0)

        ### beeswarm
        feat_names_plot = []
        for col in meta_info['mdl_in_cols']:
            if 'perc_' in col:
                col = col.replace('perc_', '') + '%'
            if 'dummy' in col:
                col = 'sector_flag'

            feat_names_plot.append(col.replace('prediction_', ''))

        ### histogram of rec score for advanced materials vs not advanced materials
        score_col = 'adv_rec_score'
        # plot the advancement data twice for the legend. The second one shows on top of the first.
        ax_mdl_hist.hist(df_data[df_data['was_adv'] == True][score_col].dropna(),
                         bins=np.arange(0, 1 + 0.01, 0.05), histtype='step', density=False, log=False)
        ax_mdl_hist.hist(df_data[df_data['was_adv'] == True][score_col].dropna(),
                         bins=np.arange(0, 1 + 0.01, 0.05), histtype='step', density=False, log=False)

        ax_mdl_hist2 = ax_mdl_hist.twinx()
        ax_mdl_hist2.hist(df_data[df_data['was_adv'] == False][score_col].dropna(),
                          bins=np.arange(0, 1 + 0.01, 0.05), histtype='step', density=False, log=False)

        ax_mdl_hist.set_xlabel('recommendation score')
        ax_mdl_hist.set_ylabel('adv count')
        ax_mdl_hist.legend(['not adv', 'adv'], loc='best', fontsize=7)
        ax_mdl_hist.set_ylim([0, ax_mdl_hist.get_ylim()[1]])

        ax_mdl_hist2.set_ylabel('not adv count')
        ax_mdl_hist2.set_ylim([0, ax_mdl_hist2.get_ylim()[1]])

        # accuracy vs year
        # do both mdl and yield
        potential_years = [str(i) for i in range(2010, 3000)]

        yrs = [int(grp) for grp in pd.unique(df_adv['decision_group']) if grp in potential_years]
        mdls = ['ML', 'yield']
        metric = 'balanced_acc'
        accs = []
        years = []
        mdl_names = []
        colors = []
        bar_label = []

        offsets = [-0.15, 0, 0.15]
        width = 0.15
        c = ['r', 'g', 'b']
        i = 0
        for yr in yrs:
            for mdl in mdls:
                if mdl == 'ML':
                    mdl_names.append('Adv Rec')
                else:
                    mdl_names.append(mdl)
                if mdl == 'yield':
                    bar_label.append(
                        'n=' + str(df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == str(yr)) & (
                                    df_adv['metric'] == 'n')]['value'].values[0])
                    )
                else:
                    bar_label.append('')
                years.append(yr + offsets[i])
                accs.append(
                    df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == str(yr)) & (
                                df_adv['metric'] == metric)]['value'].values[0] * 100
                )
                colors.append(c[i])

                i = i + 1
            # add chance
            mdl_names.append('chance')
            years.append(yr + offsets[i])
            accs.append(50)
            colors.append(c[i])
            bar_label.append('')

            i = 0

        b = ax_acc_year.bar(years, accs, width=width, color=colors)
        # ax_acc.bar_label(b, labels=bar_label,
        #             padding=0, color='k', fontsize=10)
        ax_acc_year.set_ylim([0, 100])
        ax_acc_year.set_xticks(labels=[str(yr) for yr in yrs], ticks=yrs)
        ax_acc_year.set_yticks(ticks=np.arange(0, 110, 10))
        ax_acc_year.legend([b[0], b[1], b[2]], mdl_names, fontsize=7)
        ax_acc_year.set_xlabel('year')
        ax_acc_year.set_ylabel('balanced accuracy')

        # accuracy per MG
        mgs = [grp for grp in pd.unique(df_adv['decision_group']) if grp not in potential_years]
        if len(mgs) > 1:
            if 'all' in mgs:
                mgs.remove('all')

            mdls = ['ML', 'yield']
            metric = 'balanced_acc'
            accs = []
            mgs_plot = []
            mdl_names = []
            colors = []
            bar_label = []

            offsets = [-0.15, 0, 0.15]
            width = 0.15
            c = ['r', 'g', 'b']
            i = 0
            for i_mg, mg in enumerate(mgs):
                for mdl in mdls:
                    if mdl == 'ML':
                        mdl_names.append('Adv Rec')
                    else:
                        mdl_names.append(mdl)
                    if mdl == 'yield':
                        bar_label.append(
                            'n=' + str(df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == mg) & (
                                        df_adv['metric'] == 'n')]['value'].values[0])
                        )
                    else:
                        bar_label.append('')
                    mgs_plot.append(i_mg + offsets[i])
                    accs.append(
                        df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == mg) & (
                                    df_adv['metric'] == 'balanced_acc')]['value'].values[0] * 100
                    )
                    colors.append(c[i])

                    i = i + 1
                # add chance
                mdl_names.append('chance')
                mgs_plot.append(i_mg + offsets[i])
                accs.append(50)
                colors.append(c[i])
                bar_label.append('')

                i = 0

            b = ax_acc_mg.bar(mgs_plot, accs, width=width, color=colors)
            # ax_acc.bar_label(b, labels=bar_label,
            #             padding=0, color='k', fontsize=10)
            ax_acc_mg.set_ylim([0, 100])
            ax_acc_mg.set_xticks(labels=mgs, ticks=np.arange(0, len(mgs)), rotation=-45, ha='left')
            ax_acc_mg.set_yticks(ticks=np.arange(0, 110, 10))
            ax_acc_mg.legend([b[0], b[1], b[2]], mdl_names, fontsize=7)
            ax_acc_mg.set_xlabel('Maturity group')
            ax_acc_mg.set_ylabel('balanced accuracy')
        else:
            ax_acc_mg.set_title('no maturity groups?')

        # FN rate vs threshold: perc advanced dropped per percentage dropped
        metric_pref = 'perc_adv_in_bottom_'
        metric_suf = 'perc'
        metrics = [met for met in pd.unique(df_adv['metric']) if metric_pref in met]

        for mdl, c in zip(['ML', 'yield'], ['r', 'g']):
            percs = []
            vals = []
            for met in metrics:
                percs.append(int(met.split('_')[-1].replace(metric_suf, '')))
                vals.append(
                    df_adv[(df_adv['mdl'] == mdl) & (df_adv['decision_group'] == 'all') & (df_adv['metric'] == met)][
                        'value'].values[0] * 100)

            ax_fn_vs_thresh.plot(percs, vals, color=c, marker='.')

        ax_fn_vs_thresh.legend(['Adv Rec', 'yield'])
        ax_fn_vs_thresh.set_xlabel('% of materials dropped')
        ax_fn_vs_thresh.set_ylabel('% of advanced\nmaterials dropped')
        ax_fn_vs_thresh.set_ylim([0, 100])
        ax_fn_vs_thresh.set_xlim([0, 100])
        ax_fn_vs_thresh.set_xticks(ticks=np.arange(0, 105, 10))
        ax_fn_vs_thresh.set_yticks(ticks=np.arange(0, 105, 10))
        ax_fn_vs_thresh.grid(visible=True)

        pdf.savefig()  # saves the current figure into a pdf page
        plt.close()

        # page 2, beeswarm plot
        fig = plt.figure(layout="constrained")
        fig.suptitle('Advancement recommender engine performance \n ' + ap_data_sector + ', stage ' +
                     stage + ', ID ' + mdl_id + ' \n', fontsize=12)

        if shap_vals.shape[0] > max_points:
            shap_idx = np.random.choice(np.arange(0, shap_vals.shape[0]), size=(max_points,), replace=False)
            shap_vals = shap_vals[shap_idx, :]
            shap_data = shap_data[shap_idx, :]

        shap.plots.beeswarm(
            shap.Explanation(
                values=shap_vals,
                base_values=np.zeros((shap_vals.shape[0],)),
                data=shap_data,
                feature_names=feat_names_plot
            ),
            max_display=20,
            show=False,
            plot_size=None,
        )

        pdf.savefig()  # saves the current figure into a pdf page
        plt.close()

        # page 3+, trait comparison histograms
        # first col = rec vs not rec, second col = adv vs not adv
        ### make giant matplotlib figure, save as pdf
        trait_list = [tr for tr in meta_info['mdl_in_cols'] if 'prediction_' in tr]
        n_pages = np.ceil(len(trait_list) / n_rows_per_page).astype(int)

        for i_page in range(n_pages):
            fig = plt.figure(layout="constrained", figsize=figsize)
            fig.suptitle('Advancement recommender engine performance \n ' + ap_data_sector + ', stage ' +
                         stage + ', ID ' + mdl_id + ' \n', fontsize=12)

            if i_page == n_pages - 1:  # last page
                traits_plot = trait_list[i_page * n_rows_per_page:]
            else:
                traits_plot = trait_list[i_page * n_rows_per_page:(i_page + 1) * n_rows_per_page]

            gs = GridSpec(n_rows_per_page, 2, figure=fig)

            for i_tr, tr_plot in enumerate(traits_plot):
                ax_rec = fig.add_subplot(gs[i_tr, 0])
                ax_adv = fig.add_subplot(gs[i_tr, 1])

                if np.sum(df_data[tr_plot].notna()) > 10:
                    # use the same bins for all histograms

                    bin_min = np.nanpercentile(df_data[tr_plot],5)
                    bin_max = np.nanpercentile(df_data[tr_plot],95)
                    tr_range = bin_max - bin_min
                    bins = np.linspace(bin_min - tr_range/4, bin_max + tr_range/4, 11) # setting bin edges, so need 1 more than the number of bins

                    # rec vs not rec
                    was_rec = df_data['adv_rec_score'] > np.nanpercentile(df_data['adv_rec_score'], 80)
                    ax_rec.hist(
                        df_data.loc[was_rec == True, tr_plot],
                        histtype='step',
                        density=True,
                        bins=bins
                    )
                    ax_rec.hist(
                        df_data.loc[was_rec == False, tr_plot],
                        histtype='step',
                        density=True,
                        bins=bins
                    )
                    ax_rec.set_title(tr_plot)
                    ax_rec.legend(['rec', 'not rec'])
                    if 'perc_' in tr_plot:
                        ax_rec.set_xlabel('BLUP % check')
                    else:
                        ax_rec.set_xlabel('BLUP')
                    ax_rec.set_ylabel('Density')

                    # adv vs not adv
                    ax_adv.hist(
                        df_data.loc[df_data['was_adv'] == True, tr_plot],
                        histtype='step',
                        density=True,
                        bins=bins
                    )
                    ax_adv.hist(
                        df_data.loc[df_data['was_adv'] == False, tr_plot],
                        histtype='step',
                        density=True,
                        bins=bins
                    )
                    ax_adv.set_title(tr_plot)
                    ax_adv.legend(['adv', 'not adv'])
                    if 'perc_' in tr_plot:
                        ax_adv.set_xlabel('BLUP % check')
                    else:
                        ax_adv.set_xlabel('BLUP')
                    ax_adv.set_ylabel('Density')

            pdf.savefig()  # saves the current figure into a pdf page
            plt.close()

    return os.path.join(out_path, out_fname)


def get_feature_importance(mdl_list, meta_info, imp_type='gain'):
    score_dict = {}
    for i in range(len(mdl_list)):
        # check if we have an xgboost model or if we calibrated the output
        # if calibrated, get estimator. if not, mdl_list[i] contains estimator
        if isinstance(mdl_list[i], CalibratedClassifierCV):
            temp_score = mdl_list[i].estimator.get_booster().get_score(importance_type=imp_type)
        else:
            temp_score = mdl_list[i].get_booster().get_score(importance_type=imp_type)


        for j in range(len(meta_info['mdl_in_cols'])):
            key = meta_info['mdl_in_cols'][j]
            to_remove = ['prediction_', 'result_', 'perc_', 'diff_']
            for to_r in to_remove:
                key = key.replace(to_r, '')
            if key not in score_dict.keys():
                score_dict[key] = 0

            if 'f' + str(j) in temp_score.keys():
                score_dict[key] = score_dict[key] + temp_score['f' + str(j)]

    total_score = np.sum(list(score_dict.values()))

    for key in score_dict:
        score_dict[key] = score_dict[key] / total_score

    return score_dict


def get_decision_group_rm(
    ap_data_sector,
    min_year=2020,
    material_type='entry'
):
    """
    Get most frequency decision group rm for each material and year from a data sector

    inputs:
        ap_data_sector : which data sector
        min_year : earliest year (inclusive) to grab data
        material_type : entry (hybrids) or parent (inbreds)
    outputs:
        df_rm : dataframe containing entry identifier, analysis year, and most frequently tested RM

    """

    if material_type == 'entry':
        query_str = """
            select 
                be_bid as "entry_identifier",
                "year" as "analysis_year",
                MAX(decision_group_rm) as "decision_group_rm"
            from (
                select 
                    rv_trial.be_bid,
                    rv_trial."year",
                    rv_trial.ap_data_sector,
                    rv_exp.decision_group_rm,
                    COUNT(*) as "n"
                from rv_ap_sector_experiment_config rv_exp
                inner join (
                    select distinct be_bid, experiment_id , rv_trial_pheno_analytic_dataset.YEAR AS "year", ap_data_sector
                    from rv_trial_pheno_analytic_dataset 
                    where ap_data_sector = {0}
                        and rv_trial_pheno_analytic_dataset.year >= {1}
                ) rv_trial
                on rv_trial.experiment_id = rv_exp.experiment_id
                    and rv_trial."year" = rv_exp.experiment_year
                group by rv_trial.be_bid,
                    rv_trial."year",
                    rv_trial.ap_data_sector,
                    rv_exp.decision_group_rm
                order by "n" desc 
            )
            group by be_bid, "year", ap_data_sector
        """.format("'" + ap_data_sector + "'", int(min_year))
    else: # use managed.rv_bb_ancestory_laas to connect hybrid to parents. Get parents.
        query_str = """
            select 
                be_bid as "entry_identifier",
                "year" as "analysis_year",
                MAX(decision_group_rm) as "decision_group_rm"
            from (
                select 
                    rv_anc.be_bid,
                    rv_trial."year",
                    rv_trial.ap_data_sector,
                    rv_exp.decision_group_rm,
                    COUNT(*) as "n"
                from rv_ap_sector_experiment_config rv_exp
                inner join (
                    select distinct be_bid, experiment_id , rv_trial_pheno_analytic_dataset."YEAR" AS "year", ap_data_sector
                    from rv_trial_pheno_analytic_dataset 
                    where ap_data_sector = {0}
                    and "year" >= {1}
                ) rv_trial
                on rv_trial.experiment_id = rv_exp.experiment_id
                    and rv_trial."year" = rv_exp.experiment_year
                inner join (
                    select distinct *
                        from (
                            select distinct donor_p as be_bid, be_bid as child_be_bid
                            from RV_BE_BID_ANCESTRY  
                            where donor_p is not null
                            union
                            select distinct receiver_p as be_bid, be_bid as child_be_bid
                            from RV_BE_BID_ANCESTRY  
                            where receiver_p is not null
                        )
                ) rv_anc
                    on rv_trial.be_bid = rv_anc.child_be_bid
                group by 
                    rv_anc.be_bid,
                    rv_trial."year",
                    rv_trial.ap_data_sector,
                    rv_exp.decision_group_rm
                order by "n" desc 
            )
            group by be_bid, "year", ap_data_sector
        """.format("'" + ap_data_sector + "'", int(min_year))

    with SnowflakeConnection() as dc:
        df_query = dc.get_data(query_str)

    return df_query


def evaluate(args,
             ap_data_sector,
             output_year,
             input_years,
             material_type,
             pipeline_runid,
             data_fname_stem='adv_model_validation_data',
             output_fname_add='validation',
             write_outputs=1,
             push_to_postgres=1):

    # load in validation file for each stage, validate model per stage, save metrics
    fpath = args.s3_input_validation_data_folder
    df_val_all_list = [] # storing prediction output

    fnames = os.listdir(fpath)
    validation_data_fnames = []
    stages = []
    for fname in fnames:
        fname_no_ext, ext = os.path.splitext(fname)
        if data_fname_stem in fname and material_type in fname and 'stg' in fname:
            validation_data_fnames.append(fname)
            stg_idx = fname_no_ext.find('stg')
            end_stg_idx = fname_no_ext[stg_idx:].find('-') + stg_idx
            stg = fname_no_ext[stg_idx+3:end_stg_idx]
            stages.append(stg)

    print(list(zip(validation_data_fnames, stages)))

    # load in all dme_metric_output at once
    if 1 == 0 and ap_data_sector != 'SUNFLOWER_EAME_SUMMER': # this call hangs for some reason
        df_dme_output = get_dme_metric_output(
            ap_data_sector=ap_data_sector, input_years=input_years, material_type=material_type
        )
    else:
        df_dme_output = pd.DataFrame()

    # load in decision group rms
    df_rm = get_decision_group_rm(
        ap_data_sector=ap_data_sector,
        min_year=np.min([int(yr) for yr in input_years]),
        material_type=material_type
    )
    print(df_rm.shape)

    for fname, stg in zip(validation_data_fnames, stages):
        # load in validation file
        df_val_all = pd.read_csv(os.path.join(
            fpath,
            fname
        ))
        # only keep data from current data sector (if multiple are used)
        df_val_all = df_val_all[df_val_all['ap_data_sector'] == ap_data_sector]

        if 'decision_group' not in df_val_all.columns:
            df_val_all['decision_group'] = 'na'

        # load in model, preprocessing has already happened
        mdl_fname, preproc_fname, meta_fname = predictive_advancement_lib.get_fname_local(
            fpath=args.s3_input_model_folder,
            material_type=material_type,
            stage=stg
        )

        print(mdl_fname, preproc_fname, meta_fname)
        mdl_list = performance_helper.load_object(
            args.s3_input_model_folder,
            mdl_fname
        )
        meta_info = performance_helper.load_object(
            args.s3_input_model_folder,
            meta_fname)

        # do not need to runpreprocessing, this is done in 'preprocessing_train' or 'preprocessing_infer'
        #df_use_proc = preproc_class.run_preprocessing(df_use)

        kfold_vals = None
        if 'kfold_label' in df_val_all.columns:
            kfold_vals = df_val_all['kfold_label'].values

        y_proba = predictive_advancement_lib.predict_proba_list(
            mdl_list,
            df_val_all[meta_info['mdl_in_cols']].values,
            kfold_vals=kfold_vals
        )

        acc, conf_mat, roc_auc, f1 = predictive_advancement_lib.get_evaluation_metrics(
            y_true=df_val_all[meta_info['mdl_out_cols']],
            y_pred=y_proba > 0.5,
            y_proba=y_proba
        )

        print(
            "Validation:",
            stg,
            df_val_all.shape,
            (acc, conf_mat, roc_auc, f1)
        )

        # make sure advancement columns are boolean
        df_val_all['adv_rec_score'] = y_proba

        if 'was_adv' in df_val_all.columns:
            df_val_all['was_adv'] = df_val_all['was_adv'].astype(bool)
        if 'was_adv_next' in df_val_all.columns:
            df_val_all['was_adv_next'] = df_val_all['was_adv_next'].astype(bool)

        # set trait column and yield column based on data sector
        if ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            yield_col = [col for col in meta_info['mdl_in_cols'] if 'YSDMN' in col and '_prev' not in col][0]
        else:
            yield_col = [col for col in meta_info['mdl_in_cols'] if 'YGSMN' in col and '_prev' not in col][0]

        # merge in dme_output_score as a second comparison
        if df_dme_output.shape[0] > 0:
            df_val_all = df_val_all.merge(
                df_dme_output.drop(columns=['analysis_type']),
                on=['ap_data_sector','analysis_year','current_stage','entry_identifier'],
                how='left'
            )

        # assign each entry/year to a decision group rm. Compute metrics per RM.
        # use denodo tables to do this, only select most frequently tested RM per material
        if 'decision_group_rm' not in df_val_all.columns:
            df_val_all = df_val_all.merge(
                df_rm,
                on=['entry_identifier','analysis_year'],
                how='left'
            )

        # compute advancement metrics and trait metrics using trial pheno data
        adv_metrics, trait_metrics1 = predictive_advancement_lib.compute_model_metrics(
            df_val_all,
            compute_advancement_metrics=True,
            yield_col=yield_col,
            trait_prefix='result_'
        )

        # compute trait metrics using GSE/AAE output (trait prefix is different from above)
        _, trait_metrics2 = predictive_advancement_lib.compute_model_metrics(
            df_val_all,
            compute_advancement_metrics=False,
            yield_col=yield_col,
            trait_prefix='prediction_'
        )

        trait_metrics = pd.concat((trait_metrics1, trait_metrics2), axis=0)

        if 'kfold' in data_fname_stem and df_val_all.shape[0] > 0:
            # generate model performance card
            make_model_performance_card(
                out_path='/opt/ml/processing/data/evaluate/',
                mdl_list=mdl_list,
                meta_info=meta_info,
                mdl_id=mdl_fname.split('-')[-1][:-4],
                df_data=df_val_all,
                df_adv=adv_metrics,
                material_type=material_type,
                ap_data_sector=df_val_all['ap_data_sector'].iloc[0],
                stage=str(stg)
            )

        df_val_all_list.append(df_val_all) # concat validation files across stages (done below)

        # save validation data, validation metrics for each stage/group
        if write_outputs == 1:
            # check to see if file path exists
            out_dir = '/opt/ml/processing/data/evaluate/'

            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            # save advancement and trait metrics
            postfix = 'stg' + str(stg) + '-' + material_type

            adv_metrics.to_csv(os.path.join(out_dir, 'advancement_metrics_'+output_fname_add+'-'+postfix+'.csv'), index=False)
            trait_metrics.to_csv(os.path.join(out_dir, 'trait_metrics_'+output_fname_add+'-'+postfix+'.csv'), index=False)
            df_val_all.to_csv(os.path.join(out_dir, fname), index=False)

        # push metrics for this stage
        if push_to_postgres == 1 and 'kfold' in data_fname_stem:
            """
            ap_data_sector,
            analysis_year,
            metric_group,
            stage,
            model_id,
            metric,
            value,
            alpha_value,
            model_type
            """

            # convert adv_metrics to postgres format
            df_adv_postgres = adv_metrics.copy()
            df_adv_postgres['ap_data_sector'] = ap_data_sector
            df_adv_postgres['analysis_year'] = int(output_year)
            df_adv_postgres['material_type'] = material_type
            df_adv_postgres['stage'] = str(stg)
            df_adv_postgres['model_id'] = pipeline_runid
            df_adv_postgres['alpha_value'] = None
            df_adv_postgres['trait'] = 'advancement'

            df_adv_postgres = df_adv_postgres.rename(columns={
                'decision_group':'metric_group',
                'mdl':'model_type'
            })

            # convert trait_metrics to postgres format
            df_trait_postgres = trait_metrics.copy()
            df_trait_postgres['ap_data_sector'] = ap_data_sector
            df_trait_postgres['analysis_year'] = int(output_year)
            df_trait_postgres['metric_group'] = 'all'
            df_trait_postgres['material_type'] = material_type
            df_trait_postgres['stage'] = str(stg)
            df_trait_postgres['model_id'] = pipeline_runid
            df_trait_postgres['alpha_value'] = None

            df_trait_postgres = df_trait_postgres.rename(columns={
                'mdl': 'model_type'
            })

            # feature importances
            feature_imp = get_feature_importance(mdl_list, meta_info, imp_type='gain')
            df_imp_postgres = pd.DataFrame()

            for key in feature_imp:
                temp_data = {
                    'ap_data_sector': ap_data_sector,
                    'analysis_year': int(output_year),
                    'metric_group': 'all',
                    'stage': str(stg),
                    'model_id': pipeline_runid,
                    'alpha_value': None,
                    'model_type': 'ML',
                    'metric': 'feature_imp',
                    'trait': key,
                    'value': feature_imp[key],
                    'material_type': material_type
                }
                df_imp_postgres = pd.concat((df_imp_postgres, pd.DataFrame(data=[temp_data])), axis=0)

            # stack trait and adv metrics, then clean up
            df_postgres = pd.concat((df_adv_postgres, df_trait_postgres, df_imp_postgres), axis=0)
            df_postgres = df_postgres.dropna(subset=['value','alpha_value'],how='all')

            # convert analysis year if
            if performance_helper.check_for_year_offset(ap_data_sector):
                df_postgres['analysis_year'] = df_postgres['analysis_year'] + 1

            # drop rows, then upsert
            pc = PostgresConnection()
            pc.run_sql(ADV_METRICS_DROP.format(
                "'" + ap_data_sector + "'",
                int(output_year),
                "'"+str(stg)+"'"
            ))
            pc.upsert(ADV_METRICS_UPSERT, 'advancement_recommender_output', df_postgres, is_spark=False)

    # output validation file with all stages
    if write_outputs == 1:
        df_val_all_out = pd.concat(df_val_all_list,axis=0)
        df_val_all_out = performance_helper.order_output_columns(df_val_all_out) # function makes copy, then reorders columns.)

        # check to see if file path exists
        out_dir = '/opt/ml/processing/data/evaluate/'

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # save advancement and trait metrics
        df_val_all_out.to_csv(
            os.path.join(out_dir, 'predictions_validation_' + output_fname_add + '.csv'), index=False)
        #df_dme_output.to_csv(os.path.join(out_dir, 'dme_out_' + output_fname_add + '.csv'), index=False)


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_validation_data_folder', type=str,
                        help='s3 input validation data folder', required=True)
    parser.add_argument('--s3_input_model_folder', type=str,
                        required=True)
    args = parser.parse_args()

    # ignore future warnings from pandas
    warnings.simplefilter(action='ignore', category=FutureWarning)

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['train_years'] # list of strings
        output_year = data['forward_model_year']
        material_type = data['material_type']
        pipeline_runid = data['target_pipeline_runid']

        try:
            # run for both the kfold validation data and the completely held-out set.
            #for data_fname_stem, output_fname_add in zip(['adv_model_validation_data_','adv_model_kfold_validation_data_'],
            #                                             ['validation','kfold']):
            for data_fname_stem, output_fname_add in zip(['adv_model_kfold_validation_data_'],
                                                         ['kfold']):
                evaluate(
                    ap_data_sector=ap_data_sector,
                    input_years=input_years,
                    output_year=output_year,
                    material_type=material_type,
                    pipeline_runid=pipeline_runid,
                    data_fname_stem=data_fname_stem,
                    output_fname_add=output_fname_add,
                    args=args
                )

        except Exception as e:
            error_event(ap_data_sector, output_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()