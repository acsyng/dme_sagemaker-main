# import packages
import os
import sys
import json
import pandas as pd, numpy as np
import argparse
import boto3

from sklearn.feature_selection import f_classif

from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.performance_lib import performance_helper
from libs.config.config_vars import CONFIG, S3_DATA_PREFIX
from libs.performance_lib import preprocessor_class


# provide df[col].values, df['was_adv'].values to function
def f_classif_wrapper(x, y):
    if isinstance(x, pd.Series):
        x = x.values
    if isinstance(y, pd.Series):
        y = y.values
    nan_mask = (np.isnan(x)) | (np.isinf(x))
    x = x[~nan_mask]
    y = y[~nan_mask]

    if x.shape[0] > 0 and y.shape[0] > 0:
        f_val, p_val = f_classif(x.reshape((-1, 1)), y)
    else:
        f_val = -100
        p_val = 1

    return f_val, p_val


def make_preprocessor(df_in, potential_vars=None, marker_traits=[],ap_data_sector=''):
    if potential_vars is None:
        potential_vars = ['result_', 'prediction_']

    # get phenotypic traits by searching for result_diff_
    # get corr subtract option based on input columns
    corr_cols = []
    for var in ['result_','prediction_']:
        corr_cols_temp = []
        if var in potential_vars:
            #if var+'GMSTP' in df_in.columns and np.sum(df_in[var+'GMSTP'].notna()) > 25:
            #    corr_cols_temp.append([[var+'GMSTP'],[var+'YGSMN']])
            #if var+'MRTYN' in df_in.columns and np.sum(df_in[var+'MRTYN'].notna()) > 25:
            #    corr_cols_temp.append([[var + 'MRTYN'], [var + 'YGSMN']])
            # make yield: gmstp + plhtn+ erhtn if we have those variables
            in_vars =[]
            for in_var in ['GMSTP','PLHTN','ERHTN','MRTYN']:
                if var+in_var in df_in.columns and np.sum(df_in[var+in_var].notna()) > 25:
                    in_vars.append(var+in_var)
            if len(in_vars) > 1:
                corr_cols_temp.append([in_vars, [var+'YGSMN']])

        if len(corr_cols_temp) > 0:
            corr_cols.extend(corr_cols_temp)

    corr_cols = []
    # corr_cols = [
    #    [['PLHTN','ERHTN','GMSTP'],['YGSMN']],
    #    [['ERHTN'],['PLHTN']],
    #    [['GMSTP'],['YGSMN']]
    #    ]
    # x = PLHTN, ERHTN, GMSTP, y= YGSMN
    # x = ERHTN, y = PLHTN
    # x = GMSTP, y = YGSMN

    # find text traits, make sure to preprocess
    text_traits=[]
    text_suff = {'bsr_t':'Rbs1', 'fels_t':'Rcs3'}
    text_pref = {col:'' for col in ['fels_t','bp_t','cls_t','dic_t','dpm_t',
                                      'e1_t','e3_t','fl_ct','ll55_t','met_t',
                                      'mi__t','pb_ct','pd_ct','rr2_t','stmtt','sts_t']}
    clean_text = {'cls_t':'_'}

    preproc_steps = []
    if len(corr_cols) > 0:
        preproc_steps.append('subtract corr')

    for trait in marker_traits:
        if trait in df_in.columns and np.sum(df_in[trait].notna()) / df_in.shape[0] > 0.05:
            text_traits.append(trait)
            if 'process text traits' not in preproc_steps:
                preproc_steps.append('process text traits')

    # preprocessor
    # corn brazil LRTLR needs to be filled if missing. In training data, if missing increases advancement rate
    cols_to_fill = []
    if ap_data_sector == 'CORN_BRAZIL_SAFRINHA' or ap_data_sector == 'CORN_BRAZIL_SUMMER':
        preproc_steps.append('fill missing')
        cols_to_fill = ['prediction_LRTLR','prediction_perc_LRTLR']

    preproc_steps.append('make dummies')
    preproc_class = preprocessor_class.PredAdvPreprocessor(
        preprocess_steps=preproc_steps,
        corr_traits=corr_cols,
        text_traits=text_traits,
        text_suff=text_suff,
        text_pref=text_pref,
        clean_text=clean_text,
        make_dummies=['ap_data_sector'],
        cols_to_fill=cols_to_fill
    )

    return preproc_class


def compute_model_columns(
        df_piv,
        mdl_out_col = 'was_adv',
        yield_stem='YGSMN',
        potential_vars=None,
        ap_data_sector=None,
        stage=None
):
    """
    function to automatically generate model columns given a dataframe containing phenotypic traits and advancement decision
    determines which traits to use based on their predictive power for the output column
    determines how to normalize each trait (difference or percent check, raw value) based on predict power
    uses only a single variable for each trait (only 1 yield column, only 1 erhtn column)

    inputs :
        df_piv : dataframe containing potential input traits and output column. Each row is a full observation
        mdl_out_col : output column
        yield_stem : yield trait to use (YGSMN or YSDMN or etc)
        potential_vars : types of traits and normalizations to consider.
            ['prediction_','prediction_perc_'] means only use outputs from AAE/GSE and only use raw and percent check
        ap_data_sector : data sector as string
        stage : stage for model as numeric
            Can implement specific logic per stage and data sector

    outputs : list of columns from the dataframe to use as predictors in a model to predict the output column



    """
    if potential_vars is None:
        potential_vars = ['result_', 'result_diff_', 'result_perc_']

    # get phenotypic traits by searching for result_diff_
    pheno_traits = list(set(
        ['_'.join(col.split('_')[1:]) for col in
                df_piv.columns[['prediction_' in col and
                                'perc' not in col and
                                'diff' not in col and
                                yield_stem not in col for col in df_piv.columns]]] # get yield next
    ))

    pheno_mdl_in_cols = []

    # many yield columns, including potentially YGSMN-GMSTP-ERHTN-PLHTN like columns
    # only use prefixes in potential_vars
    yield_cols = []
    for var in potential_vars:
        yield_cols.extend([col for col in df_piv.columns if (yield_stem in col or 'ymh_residual' in col) and var in col and 'prev' not in col])
    yield_cols = list(set(yield_cols))

    # force percentage if it is present and has data...
    if 'prediction_'+yield_stem in yield_cols and \
            'prediction_perc_'+yield_stem in yield_cols and \
            np.sum(df_piv['prediction_perc_'+yield_stem].notna())/df_piv.shape[0] > 0.1:
        yield_cols.remove('prediction_'+yield_stem)
    if 'result_'+yield_stem in yield_cols:
        yield_cols.remove('result_'+yield_stem) # never use this for yield, it's not normalized

    best_f_val = -1
    is_pred_col = 0 # switch to 0 and uncomment if block to force prediction if available. to -1 if allow result_
    if len(yield_cols) > 0:
        best_yield_col = yield_cols[0]
        # only include 1 yield col
        for yield_col in yield_cols:
            # if we have a pred col, don't consider result_cols
            if not (is_pred_col == 1 and 'result_' in yield_col):
                f_val, p_val = f_classif_wrapper(df_piv[yield_col].values, df_piv[mdl_out_col].values)
                if ((f_val > best_f_val) or (is_pred_col==0 and 'prediction' in yield_col)) \
                        and p_val < 0.4 and np.sum(df_piv[yield_col].notna()) / df_piv.shape[0] > 0.05:
                    best_f_val = f_val
                    best_yield_col = yield_col
                    if 'prediction' in yield_col:
                        is_pred_col = 1
        # force YGSMN_residual for corn na summer
        if 'prediction_'+yield_stem+'_residual' in df_piv.columns and ap_data_sector == 'CORN_NA_SUMMER' and stage < 4:
            pheno_mdl_in_cols.append('prediction_'+yield_stem+'_residual')
        else:
            pheno_mdl_in_cols.append(best_yield_col)

    # no yield in pheno_traits
    pheno_trait_thresh = 0.4
    for trait in pheno_traits:
        # get columns to check, keep if >1% of data has value
        best_f_val = -1
        best_trait = ''
        is_pred_col = -0 # prioritize prediction columns
        for var in potential_vars:
            if var + trait in df_piv.columns and not (is_pred_col==1 and 'result' in var) and \
                    np.sum(df_piv[var + trait].notna()) / df_piv.shape[0] > 0.025:
                f_val, p_val = f_classif_wrapper(df_piv[var + trait].values, df_piv[mdl_out_col].values)
                # only keep somewhat important traits
                # do not use result_YGSMN....
                if (f_val > best_f_val and p_val < pheno_trait_thresh) or (is_pred_col==0 and 'prediction' in var):
                    best_f_val = f_val
                    best_trait = var + trait
                    if 'prediction' in var:
                        is_pred_col = 1

        if best_trait != '':
            pheno_mdl_in_cols.append(best_trait)

    # check for prev columns specifically -- data from the "previous" year as input for the "current" year
    for trait in pheno_traits + [yield_stem]:
        if 'prediction_' + trait + '_prev' in df_piv.columns:
            f_val, p_val = f_classif_wrapper(df_piv['prediction_' + trait + '_prev'].values,
                                             df_piv[mdl_out_col].values)
        else:
            p_val = 1
        if 'prediction_perc_' + trait + '_prev' in df_piv.columns:
            f_val_perc, p_val_perc = f_classif_wrapper(df_piv['prediction_perc_' + trait + '_prev'].values,
                                             df_piv[mdl_out_col].values)
        else:
            p_val_perc = 1

        if p_val_perc < p_val and p_val_perc < 0.4:
            pheno_mdl_in_cols.append('prediction_perc_' + trait + '_prev')
        elif p_val < 0.4:
            pheno_mdl_in_cols.append('prediction_'+trait+'_prev')

    pheno_mdl_in_cols = list(set(pheno_mdl_in_cols)) # remove duplicates

    # remove STD_P and STD_N traits if present
    pheno_mdl_in_cols = [col for col in pheno_mdl_in_cols if 'STD_P' not in col and 'STD_N' not in col]

    # remove correlated traits (keep most prevalent)
    traits_to_remove = []
    corr_mat = df_piv[pheno_mdl_in_cols].corr().values
    corr_idxs = np.argwhere(corr_mat > 0.9)

    for corr_idx in corr_idxs:
        col1 = pheno_mdl_in_cols[corr_idx[0]]
        col2 = pheno_mdl_in_cols[corr_idx[1]]

        if col1 not in traits_to_remove and col2 not in traits_to_remove:
            col1_perc = np.sum(df_piv[col1].notna())/df_piv.shape[0]
            col2_perc = np.sum(df_piv[col2].notna())/df_piv.shape[0]
            if col1_perc > col2_perc:
                traits_to_remove.append(col2)
            if col2_perc > col1_perc:
                traits_to_remove.append(col1)

    for trait in traits_to_remove:
        pheno_mdl_in_cols.remove(trait)

    # find text traits, make sure to preprocess
    text_traits=[]
    text_suff = {'bsr_t':'Rbs1', 'fels_t':'Rcs3'}
    text_pref = {col:'' for col in ['fels_t','bp_t','cls_t','dic_t','dpm_t',
                                      'e1_t','e3_t','fl_ct','ll55_t','met_t',
                                      'mi__t','pb_ct','pd_ct','rr2_t','stmtt','sts_t']}
    clean_text = {'cls_t':'_'}

    # decision_group_rm
    if 'decision_group_rm' in df_piv.columns:
        pheno_mdl_in_cols.append('decision_group_rm')

    # dummy vars
    for col in df_piv.columns:
        if 'dummy_' in col:
            pheno_mdl_in_cols.append(col)

    # for corn brazil, force perc_GMSTP
    if 'prediction_GMSTP' in pheno_mdl_in_cols and 'prediction_perc_GMSTP' in df_piv.columns:
        pheno_mdl_in_cols.remove('prediction_GMSTP')
        pheno_mdl_in_cols.append('prediction_perc_GMSTP')
    elif 'result_GMSTP' in pheno_mdl_in_cols and 'result_perc_GMSTP' in df_piv.columns:
        pheno_mdl_in_cols.remove('result_GMSTP')
        pheno_mdl_in_cols.append('prediction_perc_GMSTP')

    return pheno_mdl_in_cols, mdl_out_col


def download_folder_s3(bucket, fpath):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix = fpath):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key) # save to same path


def preprocess_train(ap_data_sector,
                    input_years,
                    pipeline_runid,
                    stages,
                    material_type,
                    args,
                    marker_traits=[],
                    write_outputs=1):

    # get bucket
    bucket = CONFIG['bucket']

    # set a numpy seed so kfolds is the same across runs
    np.random.seed(12394)

    yield_stem = 'YGSMN'
    if ap_data_sector in ['CORNSILAGE_EAME_SUMMER']:
        yield_stem = 'YSDMN'

    # get marker traits
    if len(marker_traits) == 0 and 1==0:
        for yr in input_years:
            plot_result_fpath = os.path.join(
                args.s3_input_data_ingestion_folder,
                'data_'+str(yr)+ '_plot_result_tall.csv'
            )
            if os.path.exists(plot_result_fpath):
                df_plot_result = pd.read_csv(os.path.join(
                    args.s3_input_data_ingestion_folder,
                    'data_'+str(yr)+ '_plot_result_tall.csv'
                ))
                if df_plot_result.shape[0] > 0:
                    marker_traits.extend(list(pd.unique(df_plot_result['var'])))

        # drop duplicates
        marker_traits = list(set(marker_traits))
        print(marker_traits)
        # remove df_plot_result to free up memory
        df_plot_result = []

    df_input_piv = predictive_advancement_lib.load_and_preprocess_all_inputs_ml(
        args=args,
        prefix_all='data',
        material_type=material_type,
        years_to_load=input_years,
        read_from_s3=0,
        yield_stem=yield_stem,
        bucket=bucket
    )

    # do not use YGSMN for corn silage eame
    if ap_data_sector in ['CORNSILAGE_EAME_SUMMER']:
        for trait_remove in ['prediction_YGSMN','prediction_perc_YGSMN']:
            if trait_remove in df_input_piv.columns:
                df_input_piv = df_input_piv.drop(columns=trait_remove)

    # for some data sectors, it may be worth training on data from a different season.
    # this code pulls in from a different ingestion folder that is provided in args.
    # if adding data sector, need to add argument for that data sector as well.
    temp_input_args = []
    if ap_data_sector == 'CORN_BRAZIL_SAFRINHA': # get corn brazil summer as well
        # inputs years is a list of strings, offset year by one because brazil summer is planted the year before.
        new_years = [str(int(input_yr) - 1) for input_yr in input_years]

        # download files from s3 to local folder.
        # local folder path = CONFIG['output_source']/data_ingestion_{data_sector}/
        s3_path = S3_DATA_PREFIX + '/{}/data_ingestion/'.format('CORN_BRAZIL_SUMMER')
        local_path = os.path.join(CONFIG['output_source'], 'data_ingestion_cbs'+os.path.sep) # end with '/'
        print(bucket, s3_path, local_path)

        performance_helper.download_folder_s3(
            bucket=CONFIG['bucket'],
            s3_path=s3_path,
            local_path=local_path
        )
        # set temp_input_args to point to local folder, which ends with '/'
        temp_input_args = [
            '--s3_input_data_ingestion_folder', local_path
        ]

    if len(temp_input_args) > 0:
        parser = argparse.ArgumentParser(description='app inputs and outputs')
        parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                            help='s3 input data ingestion folder', required=True)
        new_args = parser.parse_args(temp_input_args)

        df_input_piv_temp = predictive_advancement_lib.load_and_preprocess_all_inputs_ml(
            args=new_args,
            prefix_all='data',
            material_type=material_type,
            years_to_load=new_years,
            read_from_s3=0,
            bucket=bucket
        )

        df_input_piv = pd.concat((df_input_piv, df_input_piv_temp), axis=0)

    df_input_piv = df_input_piv.replace([np.inf, -np.inf], np.nan)

    # some sectors have decision groups, others don't in historic data.
    if 'decision_group' not in df_input_piv.columns:
        df_input_piv['decision_group'] = 'na'

    # get desired stages for each sector
    # default values
    potential_vars = None
    stratify_cols = ['ap_data_sector','analysis_year']

    """
    # include data from the previous year in current-year predictions (prediction_YGSMN and prediction_YGSMN_prev (from last year))
    # ends up not really helping...
    cols_to_merge = [col for col in df_input_piv.columns if 'prediction' in col]
    df_to_merge = df_input_piv[['analysis_year', 'entry_identifier'] + cols_to_merge]
    df_to_merge['analysis_year'] = df_to_merge['analysis_year'] + 1  # grab data from the previous year
    df_to_merge = df_to_merge.rename(columns={col: col + '_prev' for col in cols_to_merge})

    df_input_piv = df_input_piv.merge(df_to_merge, on=['analysis_year', 'entry_identifier'], how='left')
    """

    for stg_str in stages:
        # stg_str is a string. Convert to int if a single length. How to deal with 4.1....? -> deal with this later...
        if len(stg_str) == 1:
            stg = int(stg_str)
        else:
            stg = float(stg_str)

        if ap_data_sector in ['CORN_BANGLADESH_DRY','CORN_BANGLADESH_WET']: # some apac corn data sectors are tiny and don't have AAE outputs?
            potential_vars = ['result_', 'result_perc_']
        else:
            potential_vars = ['prediction_','prediction_perc_']

        if material_type == 'entry':
            df_piv = df_input_piv[
                (df_input_piv['material_type_simple'] == 'entry') &
                (df_input_piv['current_stage'] == stg)
                ]
        else:
            df_piv = df_input_piv[
                (df_input_piv['material_type_simple'] != 'entry') & # get parents across pools
                (df_input_piv['current_stage'] == stg)
            ]

        # don't train on materials where harvest text contains 'drop'
        if 'harvt_drop' in df_piv.columns:
            df_piv = df_piv[df_piv['harvt_drop'] == False]

        # compute was_adv for each row based on current_stage columns:
        # get advancement decisions by comparing stage information across years
        adv_same_stage = 0
        if ap_data_sector in ('SOY_BRAZIL_SUMMER', 'SOY_LAS_SUMMER', 'CORN_LAS_SUMMER', 'CORN_BRAZIL_SAFRINHA') \
                or 'CORN_THAILAND_' in ap_data_sector or 'CORN_INDONESIA_' in ap_data_sector or \
                'CORN_PHILIPPINES_' in ap_data_sector or 'CORN_BANGLADESH_' in ap_data_sector or \
                'CORN_INDIA_' in ap_data_sector:  # not for Pakistan, not for vietnam
            adv_same_stage = 1

        if ap_data_sector == 'CORN_NA_SUMMER' and stg_str == '4':
            adv_same_stage = 1

        print(adv_same_stage)
        df_piv = df_piv[(df_piv['current_stage'].notna()) & (df_piv['next_stage'].notna())]
        df_piv = performance_helper.compute_advancements(df_piv, adv_same_stage=adv_same_stage)
        print(df_piv['was_adv'].sum())

        # make sure there are advancements before making model...
        if df_piv.shape[0] > 50 and np.sum(df_piv['was_adv']) > 10:
            # get train/validation split (will do k-folds validation on train set)
            # split within each group based on stratify cols (defined above)
            # split into training and validation sets. Use stratified sampling.
            df_tr_all, df_val_all = predictive_advancement_lib.stratified_train_test_split(df_piv, stratify_cols)

            # provide label for k-fold training in training set. Do here so this is only done once
            df_tr_all = predictive_advancement_lib.stratified_kfolds(df_tr_all, stratify_cols, n_folds=5)

            # make and train preprocessor, then set model columns
            # do in this order to enable creation and use of residual traits
            preproc_class = make_preprocessor(
                df_tr_all,
                potential_vars=['result_', 'prediction_'],
                marker_traits=marker_traits,
                ap_data_sector=ap_data_sector
            )

            # train and run preprocessor. get kfold dataframe
            df_tr_all = preproc_class.train_preprocessing(df_tr_all)
            df_kfold_val = df_tr_all.copy()
            if df_val_all.shape[0] > 0:
                df_val_all = preproc_class.run_preprocessing(df_val_all)

            mdl_in_cols, mdl_out_col = compute_model_columns(
                df_tr_all,
                yield_stem=yield_stem,
                potential_vars=potential_vars,
                marker_traits=marker_traits,
                ap_data_sector=ap_data_sector,
                stage=stg
            )

            # remove rows with no input data in evaluation set and training set
            # also, remove rows with no input data in the prediction or result traits. These are most important
            pred_in_cols = [col for col in mdl_in_cols if ('prediction' in col or 'result' in col) and '_prev' not in col]
            for subset_cols in [mdl_in_cols, pred_in_cols]:
                df_tr_all = df_tr_all.dropna(subset=subset_cols, how='all')
                if df_val_all.shape[0] > 0:
                    df_val_all = df_val_all.dropna(subset=subset_cols, how='all')
                if df_kfold_val.shape[0] > 0:
                    df_kfold_val = df_kfold_val.dropna(subset=subset_cols, how='all')

            # check for text traits
            if hasattr(preproc_class, 'text_trait_cols'):
                for key in preproc_class.text_trait_cols:
                    if isinstance(preproc_class.text_trait_cols[key], list):
                        for col in preproc_class.text_trait_cols[key]:
                            if col not in mdl_in_cols:
                                mdl_in_cols.append(col)
                    else:
                        if preproc_class.text_trait_cols[key] not in mdl_in_cols:
                            mdl_in_cols.append(preproc_class.text_trait_cols[key])

            # remove duplicates from mdl_in_cols
            mdl_in_cols = list(dict.fromkeys(mdl_in_cols))

            #if ap_data_sector=='CORN_BRAZIL_SUMMER' and stg_str=='4':
            #    yield_col = [col for col in mdl_in_cols if 'YGSMN' in col][0]
            #    mdl_in_cols.remove(yield_col)
            #    mdl_in_cols.append('result_YGSMN-GMSTP-PLHTN-ERHTN')

            print(mdl_in_cols)
            # compute statistics on input data to later detect training-serving skew or drift
            if df_tr_all.shape[0] == 0:
                df_tr_stats = pd.DataFrame()
            else:
                df_tr_stats = predictive_advancement_lib.compute_input_statistics(df_in=df_tr_all, in_cols=mdl_in_cols)

            mdl_cols = mdl_in_cols.copy()
            if isinstance(mdl_out_col,list):
                mdl_cols.extend(mdl_out_col)
            else:
                mdl_cols.append(mdl_out_col)

            mdl_cols.append('analysis_year') # needed to weigh samples by year
            mdl_cols.append('kfold_label')  # needed to do k-fold validation
            mdl_cols.append('entry_identifier') # generally useful
            mdl_cols.append('ap_data_sector') # now that we can have multiple data sectors, keep this around

            # save training and validation sets if there is enough data
            if write_outputs == 1 and df_tr_all.shape[0] > 50 and np.sum(df_tr_all['was_adv']) > 10:
                # check to see if file path exists
                data_out_dir = '/opt/ml/processing/data/preprocessing_train/{}'.format(pipeline_runid)
                preproc_out_dir = '/opt/ml/processing/data/preprocessing_train/preprocessors'

                data_out_postfix = 'stg'+str(stg) + '-' + material_type
                preproc_out_postfix = data_out_postfix + '-' + pipeline_runid

                for out_dir in [data_out_dir, preproc_out_dir]:
                    if not os.path.exists(out_dir):
                        os.makedirs(out_dir)

                # save data files
                if 'prediction_YGSMN' in df_piv.columns:
                    print('yield_count', df_piv['prediction_YGSMN'].count())
                else:
                    print('not here')

                df_tr_all[mdl_cols].to_csv(os.path.join(data_out_dir, 'adv_model_training_data_'+data_out_postfix+'.csv'), index=False)
                df_kfold_val.to_csv(os.path.join(data_out_dir, 'adv_model_kfold_validation_data_'+data_out_postfix+'.csv'), index=False)
                #df_val_all.to_csv(os.path.join(data_out_dir, 'adv_model_validation_data_'+data_out_postfix+'.csv'), index=False)

                # save preprocessor in its own folder
                performance_helper.save_object(
                    preproc_class,
                    preproc_out_dir,
                    'preprocessor-' + preproc_out_postfix + '.pkl'
                )
                # save statistics in preprocessor folder
                df_tr_stats.to_csv(
                    os.path.join(
                        preproc_out_dir,
                        'preprocessed_training_statistics_' + preproc_out_postfix + '.csv'
                    ),
                    index=False
                )
        else:
            print("not making model for this stage, low data (n=)" + str(df_piv.shape[0]), " and/or lack of advancements (n=" + str(np.sum(df_piv['was_adv'])), str(stg))


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input data ingestion folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['train_years'] # list of strings
        material_type = data['material_type']
        pipeline_runid = data['target_pipeline_runid']
        stages = data['stages']

        try:
            preprocess_train(
                ap_data_sector=ap_data_sector,
                input_years=input_years,
                pipeline_runid=pipeline_runid,
                stages=stages,
                material_type=material_type,
                args=args
            )

        except Exception as e:
            error_event(ap_data_sector, '', pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
