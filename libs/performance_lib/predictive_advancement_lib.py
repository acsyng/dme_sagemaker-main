# imports 
import pandas as pd
import numpy as np
import os

import xgboost

from sklearn.metrics import roc_auc_score
from sklearn.metrics import f1_score
from sklearn.metrics import balanced_accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import auc
from sklearn.model_selection import train_test_split
from sklearn.calibration import CalibratedClassifierCV
from scipy.stats import ttest_ind

import re  # for text trait processing
import pickle  # for saving
import json
import datetime  # for generating model names
import boto3
import warnings
import xgboost

from libs.performance_lib import performance_helper
from libs.performance_lib import preprocessor_class

# functions specific to the advancement project are below

# extract whether a material was dropped based on harvest text
def process_harvt(row_in):
    if isinstance(row_in,str):
        return 'drop' in row_in.lower()
    else:
        return False

    
# aggregate tables to decision group level (if not already), pivot tables, then merge together
# for each table, check for meta columns. Pivot all other columns
def get_matching_columns(df_cols, potential_cols):
    return list(df_cols[[col in potential_cols for col in df_cols]])

# for aggregation...
# when training, aggregate to [ap_data_sector, analysis_year, decision group, entry_identifier, pipeline_runid] <- might want this to be an input
# if a column is missing, ignore it
# when testing, use breakout config to define aggregates. Include ap_data_sector, analysis_year, entry_identifier, pipeline_runid
def aggregate_and_pivot_inputs(df_input_list,
                               material_type,
                               how_to_join_list,
                               potential_var_cols,
                               potential_value_cols,
                               training_or_testing_agg_cols,
                               potential_agg_dict):
    df_piv_list = []
    for df_input in df_input_list:
        if "entry_identifier" not in df_input.columns and "be_bid" in df_input.columns:
            df_input = df_input.rename(columns={"be_bid":"entry_identifier"})

        # convert material type to material type simple if material_type_simple not present:
        if 'material_type' in df_input.columns and 'material_type_simple' not in df_input.columns:
            df_input['material_type_simple'] = 'entry'
            par_types = ['female', 'male', 'pool2', 'pool1']
            df_input.loc[df_input['material_type'].isin(par_types), 'material_type_simple'] = 'parent'

        # only get materials with the correct parent/entry type
        for mat_type_col in ['material_type','material_type_simple']:
            if mat_type_col in df_input.columns:
                if material_type == 'entry':
                    df_input = df_input[df_input[mat_type_col] == 'entry']
                else:
                    df_input = df_input[df_input[mat_type_col] != 'entry']

        # get var cols
        var_cols = get_matching_columns(df_input.columns, potential_var_cols)
        # get value cols
        value_cols = get_matching_columns(df_input.columns, potential_value_cols)
        # perform aggregate
        agg_cols = get_matching_columns(df_input.columns, training_or_testing_agg_cols)
        agg_cols.extend(var_cols)

        agg_dict = {val_col:potential_agg_dict[val_col] for val_col in value_cols}

        # nan's screw up groupby, RM is only numeric col
        for col in agg_cols:
            if col == 'decision_group_rm':
                df_input[col] = df_input[col].fillna(-10)
            else:
                df_input[col]= df_input[col].fillna('na')

        # replace any True's and False's with 1's and 0's
        replace_dict = {True:1,'True':1,False:0,'False':0}
        df_input = df_input.replace(to_replace=replace_dict)
        # make sure 'value' is a float
        if 'value' in df_input.columns:
            df_input['value'] = df_input['value'].astype(float)

        # aggregate checks and entries at different levels:
        if 'is_check' in df_input.columns:
            df_input_checks = df_input[df_input['is_check'] == 1]
            df_input_entries = df_input[df_input['is_check'] == 0].drop(
                columns=['decision_group_name','is_check','dg_stage']
            )
            # group entries as normal
            df_input_entries_grouped = df_input_entries.groupby(by=agg_cols).agg(agg_dict).reset_index()

            # group checks with addition of is_check, decision_group_name, dg_stage
            df_input_checks_grouped = df_input_checks.groupby(
                by=agg_cols + ['is_check','decision_group_name','dg_stage']
            ).agg(agg_dict).reset_index()

            print('after group checks', df_input_checks_grouped.shape)
            print('after group entries', df_input_entries_grouped.shape)
            df_input_grouped = pd.concat((df_input_entries_grouped, df_input_checks_grouped), axis=0)
        else:
            df_input_grouped = df_input.groupby(by=agg_cols).agg(agg_dict).reset_index()

        # get all meta_cols after aggregating
        meta_cols = get_matching_columns(df_input_grouped.columns, training_or_testing_agg_cols)
        if 'is_check' in df_input.columns:
            meta_cols.extend(['is_check','decision_group_name','dg_stage'])

        # nan's screw up pivot, RM is only numeric col
        for col in meta_cols:
            if col == 'decision_group_rm':
                df_input_grouped[col] = df_input_grouped[col].fillna(-10)
            else:
                df_input_grouped[col] = df_input_grouped[col].fillna('na')

        # perform pivot
        df_piv = df_input_grouped.pivot_table(
            values=value_cols,
            index=meta_cols,
            columns=var_cols,
            aggfunc=agg_dict
        ).reset_index()
        print(df_piv.shape)

        # rename columns and reset index
        df_piv_cols = []
        for col_tuple in df_piv.columns:
            if col_tuple[0] == 'value' or col_tuple[0] == 'alpha_value':
                df_piv_cols.append('_'.join(filter(None, col_tuple[1:])).strip('_'))
            else:
                df_piv_cols.append('_'.join(filter(None, col_tuple)).strip('_'))

        df_piv.columns = df_piv_cols
        df_piv = df_piv.rename_axis(None, axis=1)

        # store
        df_piv_list.append(df_piv)

    # perform joins across pivoted tables.
    # join on meta cols
    # use trial_pheno (or pvs data) as the base, join other tables to those
    if len(df_piv_list) > 0:
        df_input_piv_all = df_piv_list[0] # grab first one automatically, merge rest.
    else:
        df_input_piv_all = pd.DataFrame()

    if len(df_piv_list) > 1:
        for (df_piv,how_to_join) in zip(df_piv_list[1:],how_to_join_list[1:]):
            print('merging',df_piv.shape)
            # get common columns to join on
            meta_cols_joined = get_matching_columns(df_input_piv_all.columns, training_or_testing_agg_cols)
            meta_cols_new = get_matching_columns(df_piv.columns, training_or_testing_agg_cols)

            if 'is_check' in df_input_piv_all.columns and 'is_check' in df_piv.columns:
                meta_cols_new.extend(['is_check','decision_group_name','dg_stage'])
                meta_cols_joined.extend(['is_check','decision_group_name','dg_stage'])

            common_meta_cols = list(set(meta_cols_joined).intersection(set(meta_cols_new)))

            # join
            df_input_piv_all = df_input_piv_all.merge(df_piv, on=common_meta_cols, how=how_to_join)

    return df_input_piv_all


def load_and_preprocess_all_inputs_ml(args,
                                      prefix_all,
                                      years_to_load,
                                      material_type='entry',
                                      material_experiment_decision_group_list=None,
                                      read_from_s3=0,
                                      is_infer=0,
                                      yield_stem='YGSMN',
                                      bucket=None):
    """
    Loads data in tall format, aggregates and merges together
    Data contains meta information that can be used to merge tables together.
    When training a model, we aggregate up to the decision group level because we have decisions at that level
    when validating a model, we agg up to decision groups because we have decisions at that level
    when predicting on new data, we use breakout_config to agg as breeders may want to look at
        materials at the decision group level, or in a specific environment type, irrigation type, etc.
    """

    # years to load is a list of strings

    # cast as list so for loop works regardless of how many years are requested
    if not isinstance(years_to_load, list):
        years_to_load = [years_to_load]

    df_input_piv_list = []
    for yr in years_to_load:
        prefix_year = prefix_all + '_' + str(yr) + '_'

        # for each year, load all inputs,
        # aggregate, pivot, then store
        # concat years after loading each one

        # load all data, aggregate, pivot, merge together
        # concatenate across years
        df_inputs_loaded = []
        how_to_join_list = []

        # load in each file, merge with decision groups
        for suffix in get_potential_data_suffixes(is_infer=is_infer):
            full_fname = os.path.join(
                args.s3_input_data_ingestion_folder,
                prefix_year + suffix
            )

            # check locally for the file.
            if performance_helper.check_if_file_exists(full_fname, read_from_s3=read_from_s3, bucket=bucket):
                if read_from_s3 == 1:
                    df_input_temp = pd.read_csv('s3://' + bucket + '/' + full_fname)
                else:
                    df_input_temp = pd.read_csv(full_fname)

                # merge in decision group information if relevant. Do this per source id to keep separate
                if material_experiment_decision_group_list is not None:
                    if 'experiment_id' in df_input_temp.columns and 'source_id' not in df_input_temp.columns:
                        df_input_temp = df_input_temp.rename(columns={'experiment_id':'source_id'})
                    if 'source_id' in df_input_temp.columns:
                        df_input_temp = df_input_temp.merge(
                            material_experiment_decision_group_list,
                            how='left',
                            on=['entry_identifier','source_id']
                        )

                if df_input_temp.shape[0] > 0:
                    df_inputs_loaded.append(df_input_temp)
                    if 'pvs_data' in suffix:
                        how_to_join_list.append('outer')
                    else:
                        how_to_join_list.append('left')

        # if not dealing with breakouts, aggregate all at once
        # aggregate, pivot, and merge all data together
        df_input_piv_temp = aggregate_and_pivot_inputs(
            df_input_list=df_inputs_loaded,
            material_type=material_type,
            how_to_join_list=how_to_join_list,
            potential_var_cols=get_potential_var_cols(),
            potential_value_cols=get_potential_value_cols(),
            training_or_testing_agg_cols=get_training_or_testing_cols(is_infer=is_infer),
            potential_agg_dict=get_potential_agg_dict()
        )

        df_input_piv_list.append(df_input_piv_temp)

    df_input_piv_all = pandas_concat_wrapper(df_input_piv_list, axis=0)

    # rename ymh_residual from pvs_data
    if 'prediction_ymh_residual' in df_input_piv_all.columns:
        df_input_piv_all = df_input_piv_all.rename(columns={'prediction_ymh_residual': 'prediction_'+yield_stem+'_residual'})

    return df_input_piv_all


######################################################################
######################### training functions #########################

def stratified_train_test_split(df_in, strat_cols, test_size=0):
    df_tr_list = []
    df_te_list = []

    if test_size > 0:
        strat_groups = df_in[strat_cols].drop_duplicates().values
        for out_vals in strat_groups:
            df_in_val = df_in[np.all(df_in[strat_cols].values == out_vals,axis=1)]
            if df_in_val.shape[0] > 1/test_size:
                df_tr_val, df_te_val = train_test_split(df_in_val, test_size=test_size)
                df_tr_list.append(df_tr_val)
                df_te_list.append(df_te_val)
            else:
                df_tr_list.append(df_in_val)

        df_tr = pandas_concat_wrapper(df_tr_list,axis=0)
        df_te = pandas_concat_wrapper(df_te_list,axis=0)
    else:
        df_tr = df_in.copy()
        df_te = pd.DataFrame()
    
    return df_tr, df_te
    
def stratified_kfolds(df_in, strat_cols, n_folds=5):
    df_tr_list = []
    
    strat_groups = df_in[strat_cols].drop_duplicates().values    
    for out_vals in strat_groups:
        df_in_val = df_in[np.all(df_in[strat_cols].values == out_vals,axis=1)]

        # split into n_folds groups. Make array with appropriate number of 0's, 1's, 2's, ... n_folds-1's
        # then shuffle array
        step = np.floor(df_in_val.shape[0]/n_folds).astype(int)
        kfold_label = np.zeros((df_in_val.shape[0],))
        for i_fold in range(n_folds):
            if i_fold == n_folds-1:
                kfold_label[i_fold*step:] = i_fold
            else:
                kfold_label[i_fold*step:(i_fold+1)*step] = i_fold
        
        np.random.shuffle(kfold_label)
        df_in_val['kfold_label'] = kfold_label
        
        df_tr_list.append(df_in_val)
        
    df_tr = pandas_concat_wrapper(df_tr_list,axis=0)
    
    return df_tr
        

def compute_scale_pos_weight(y_vals):
     return np.sum(y_vals == False) / np.sum(y_vals == True)
     
        
def compute_sample_weight_by_year(year_vals):
    # weight data based on number of samples for each year
    # if requested.
    sample_weight = np.ones((year_vals.shape[0],))

    for yr in pd.unique(year_vals):
        yr_mask = year_vals == yr
        sample_weight[yr_mask] = (yr_mask.shape[0] / np.sum(yr_mask))
                
    return sample_weight


def compute_sample_weight_by_year_data_sector(year_vals,data_sector_vals):
    # weight data based on number of samples for each year/data sector
    # if requested.
    sample_weight = np.ones((year_vals.shape[0],))

    for sector in pd.unique(data_sector_vals):
        for yr in pd.unique(year_vals):
            mask = (year_vals == yr) & (data_sector_vals == sector)
            sample_weight[mask] = (mask.shape[0] / np.sum(mask))

    return sample_weight


def make_mdl(params):
    return xgboost.XGBClassifier(**params)


def train_model(df_tr_proc, mdl_in_cols, mdl_out_col, mdl_year_col, xgb_params, data_sector):
    """
    helper function to train a single advancement recommender. Meant to be called from train_models_kfolds
    see train_models_kfolds for parameter description.
    """
    df_use = df_tr_proc.copy()
    # compute scale pos weight for each fold
    xgb_params['scale_pos_weight'] = compute_scale_pos_weight(df_use[mdl_out_col].values)

    # compute sample weight per material
    if 'ap_data_sector' in df_use.columns:
        sample_weight = compute_sample_weight_by_year_data_sector(
            year_vals=df_use[mdl_year_col].values,
            data_sector_vals=df_use['ap_data_sector'].values
        )
        # lower sample weight of samples from other data sectors
        sample_weight[df_use['ap_data_sector'].values != data_sector] *= 0.5

    else:
        sample_weight = compute_sample_weight_by_year(df_use[mdl_year_col].values)

    # train model
    mdl = make_mdl(xgb_params)
    mdl.fit(
        df_use[mdl_in_cols].values,
        df_use[mdl_out_col].values,
        sample_weight=sample_weight
    )

    # calibrate outputs so that probability is closer to "probability to be advanced"
    cal_clf = CalibratedClassifierCV(mdl, method="sigmoid", cv="prefit")
    cal_clf.fit(df_use[mdl_in_cols].values, df_use[mdl_out_col].values)

    return cal_clf


def train_models_kfolds(df_in_proc, mdl_in_cols, mdl_out_col,fold_label, xgb_params, data_sector, mdl_year_col='analysis_year'):
    """
    train advancement recommender for each fold to get evaluation data for each fold.
    Outputs a single recommender trained on all data.

    inputs:
        df_in_proc : dataframe containing input data for predictions after processing has been performed.
        mdl_in_cols : list of columns to use as inputs for the model.
        mdl_out_col : target column (str)
        xgb_params : dictionary containing parameters for XGBoostClassifier.
        data_sector : data sector (used to sample data from other data sectors if present)
        mdl_year_col : column where year of model is store in df_in_proc. Used to sample years equally.

    output:
        mdl_list
        df_tr_pred
        df_te_pred
    """

    df_tr_pred_list = []
    df_te_pred_list = []
    mdl_list = []

    # train a model per fold
    for fold in range(int(np.max(fold_label))+1):
        #split by fold
        df_tr_proc = df_in_proc[fold_label != fold]
        df_te_proc = df_in_proc[fold_label == fold]

        if df_tr_proc.shape[0] > 0 and df_te_proc.shape[0] > 0:
            cal_clf = train_model(
                df_tr_proc,
                mdl_in_cols=mdl_in_cols,
                mdl_out_col=mdl_out_col,
                mdl_year_col=mdl_year_col,
                xgb_params=xgb_params,
                data_sector=data_sector
            )

            # get predictions on training and test set
            df_tr_proc['recommendation_score'] = cal_clf.predict_proba(df_tr_proc[mdl_in_cols])[:,1]
            df_te_proc['recommendation_score'] = cal_clf.predict_proba(df_te_proc[mdl_in_cols])[:,1]

            df_tr_pred_list.append(df_tr_proc)
            df_te_pred_list.append(df_te_proc)
            mdl_list.append(cal_clf)
        
    df_tr_pred = pandas_concat_wrapper(df_tr_pred_list,axis=0)
    df_te_pred = pandas_concat_wrapper(df_te_pred_list,axis=0)

    # train one final model on all data in df_in_proc
    """
    cal_clf = train_model(
        df_in_proc,
        mdl_in_cols=mdl_in_cols,
        mdl_out_col=mdl_out_col,
        mdl_year_col=mdl_year_col,
        xgb_params=xgb_params,
        data_sector=data_sector
    )

    mdl_list.append(cal_clf)
    """
    return mdl_list, df_tr_pred, df_te_pred


def train_models_kfolds_hyperopt(df_use, mdl_in_cols, mdl_out_col, mdl_year_col,data_sector, params_in):
    xgb_params = params_in.copy()
    df_te_pred_list = []
    # train a model per fold
    for fold in range(1 + int(df_use['kfold_label'].max())):
        # split by fold
        df_tr_proc = df_use[df_use['kfold_label'] != fold]
        df_te_proc = df_use[df_use['kfold_label'] == fold]

        # compute scale pos weight for each fold?
        xgb_params['scale_pos_weight'] = compute_scale_pos_weight(df_tr_proc[mdl_out_col].values)

        # compute sample weight per material
        if 'ap_data_sector' in df_tr_proc.columns:
            sample_weight = compute_sample_weight_by_year_data_sector(
                year_vals=df_tr_proc[mdl_year_col].values,
                data_sector_vals=df_tr_proc['ap_data_sector'].values
            )
            # lower sample weight of samples from other data sectors
            sample_weight[df_tr_proc['ap_data_sector'].values!=data_sector] *= 0.5

        else:
            sample_weight = compute_sample_weight_by_year(df_tr_proc[mdl_year_col].values)

        # train model
        mdl = make_mdl(xgb_params)
        mdl.fit(
            df_tr_proc[mdl_in_cols].values,
            df_tr_proc[mdl_out_col].values,
            sample_weight=sample_weight
        )

        # get predictions on test set
        df_te_proc['recommendation_score'] = mdl.predict_proba(df_te_proc[mdl_in_cols])[:, 1]
        df_te_pred_list.append(df_te_proc)

    df_te_pred = pandas_concat_wrapper(df_te_pred_list, axis=0)

    return df_te_pred


def extract_cv_folds_from_df(cv_fold_label,n_folds):
    i = 0
    out = []
    for i_fold in range(n_folds):
        train_idx = np.argwhere(cv_fold_label != i_fold).reshape((-1,))
        test_idx = np.argwhere(cv_fold_label == i_fold).reshape((-1,))
        out.append((train_idx,test_idx))
        #yield train_idx, test_idx
    return out

####################################################################
######################## model loading and predicting functions######
def predict_proba_list(mdl_list, x_te, kfold_vals=None):
    # predicts using all models (and takes average) if no kfold labels are provided,
    # if kfold labels, then predicts only using the corresponding model. This prevents some leakage in evaluation set
    y_proba = np.zeros((x_te.shape[0],))

    if kfold_vals is not None and len(mdl_list) == 1:
        for i in range(len(mdl_list)):
            y_proba[kfold_vals == i] = mdl_list[i].predict_proba(x_te[kfold_vals==i,:])[:,1]
        n_mdls = 1
    elif isinstance(mdl_list, list) == True:
        for i in range(len(mdl_list)):
            mdl_pred = mdl_list[i].predict_proba(x_te)[:,1]
            y_proba = y_proba + mdl_pred

        n_mdls = len(mdl_list)
    else:
        y_proba = mdl_list.prediction_proba(x_te)[:,1]
        n_mdls = 1

    return y_proba/n_mdls



def get_fname_local(fpath, stage, material_type='entry'):
    # get filenames in /folder
    fnames = os.listdir(fpath)

    # get file for this year, sector, stage
    fname_date, fname_ts = 0, 0
    mdl_fname, preproc_fname, meta_fname = '', '', ''
    # get model information
    for fname in fnames:
        if 'mdl_list' in fname and \
                'stg' + str(stage) in fname and \
                material_type in fname and \
                'pkl' in fname:
            f_split = fname.split('-') # last split contains timestamp YYYYMMDD_HH_MM_SS
            timestamp = int(''.join(f_split[-1][:-4].split('_'))) # [:-4] removes .pkl
            if timestamp >= fname_date:
                fname_date = timestamp
                mdl_fname = fname

    # use model information to get meta and preprocessor
    if mdl_fname != '':
        preproc_fname = 'preprocessor-' + '-'.join(mdl_fname.split('-')[1:])
        meta_fname = 'meta_info-' + '-'.join(mdl_fname.split('-')[1:])

    return mdl_fname, preproc_fname, meta_fname

def get_fname_s3(bucket, s3_fpath, ap_data_sector, year, stage):
    # get filenames in bucket/folder
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    
    fnames = []
    for object_summary in my_bucket.objects.filter(Prefix=os.path.join(s3_fpath)):
        fname = object_summary.key.split('/')[-1]
        if 'mdl_preprocessor' in fname:
            fnames.append(fname)
    
    # get file for this year, sector, stage
    fname_date, fname_ts = 0, 0
    mdl_fname, preproc_fname = '', ''
    # get model information
    for fname in fnames:
        if ap_data_sector in fname and 'stg' + str(stage) in fname and 'year'+ str(year) in fname:
            f_split = fname.split('-')
            if int(f_split[-2]) >= fname_date and int(f_split[-1][:-4]) >= fname_ts: # [:-4] removes .pkl
                fname_date = int(f_split[-2])
                fname_ts = int(f_split[-1][:-4])
                mdl_fname = fname
                
    # use model information to get param information
    if mdl_fname == '':
        meta_fname = ''
        mdl_preproc_dict = {}
        meta_info = {}
    else:
        meta_fname = 'mdl_meta_info-' + '-'.join(mdl_fname.split('-')[1:])
        
    return mdl_fname, meta_fname


def load_model_from_s3(bucket, s3_fpath, mdl_fname, meta_fname):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    # read in model
    mdl_preproc_dict = pickle.loads(s3.Bucket(bucket).Object(os.path.join(s3_fpath, mdl_fname)).get()['Body'].read())
    meta_info = pickle.loads(s3.Bucket(bucket).Object(os.path.join(s3_fpath, meta_fname)).get()['Body'].read())
    
    return mdl_preproc_dict, meta_info

####################################################################
######################### validation functions ####################
def get_evaluation_metrics(y_true, y_pred, y_proba):
    # get balanced accuracy
    acc = balanced_accuracy_score(y_true=y_true, y_pred=y_pred)  # mdl.score(x,y)

    # get confusion matrix
    conf_mat = confusion_matrix(y_true=y_true, y_pred=y_pred)
    conf_mat = np.transpose(np.transpose(conf_mat) / np.sum(conf_mat, axis=1))

    # get roc_auc score
    if np.sum(y_true == False) != 0 and np.sum(y_true == True) != 0:
        roc_auc = roc_auc_score(y_true=y_true, y_score=y_proba)
    else:
        roc_auc = np.nan

    # get f1 score
    f1 = f1_score(y_true=y_true, y_pred=y_pred)

    return acc, conf_mat, roc_auc, f1

def compute_auc_pr(df_in, score_col, adv_col, pos_label=1):
    # remove na's?
    df_use = df_in.copy()
    df_use = df_use.dropna(subset=[adv_col,score_col],how='any')
    if df_use.shape[0] == 0:
        return np.nan

    # else, compute.
    if pos_label == 1:
        precs, recalls, threshs = precision_recall_curve(
            y_true=df_use[adv_col],
            probas_pred=df_use[score_col],
            pos_label=pos_label
        )
    else:  # need to flip order of score column to match negative class
        precs, recalls, threshs = precision_recall_curve(
            y_true=df_use[adv_col],
            probas_pred=-1 * df_use[score_col],
            pos_label=pos_label
        )
    auc_pr = auc(recalls, precs)
    return auc_pr


def get_df_adv_metrics(mdl_name, dg, metric, value):
    # dictionary within a list for the constructor
    return pd.DataFrame(
        [{
            'mdl': mdl_name,
            'decision_group': dg,
            'metric': metric,
            'value': value
        }]
    )


def get_auc_pr_metrics(df_in, score_col, mdl_name,adv_col='was_adv', dg_name='all', metric_name = 'AUC_PR'):
    # compute auc_pr when treating both advancement and not advance as the positive class
    df_metrics_list = []
    for pos_label, suffix in zip([1, 0], ['adv', 'notadv']):
        aucpr = compute_auc_pr(
            df_in=df_in,
            score_col=score_col,
            adv_col=adv_col,
            pos_label=pos_label
        )
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric=metric_name + '_' + suffix,
            value=aucpr
        ))

    return pandas_concat_wrapper(df_metrics_list)


def get_balanced_accuracy_score(df_in, score_col, mdl_name,adv_col='was_adv', dg_name='all', metric_name = 'balanced_acc'):
    """
    computes balanced accuracy. Called in get_metrics_per_group, not meant to be used outside of that function
    """
    df_metrics_list = []

    df_use = df_in.copy()
    df_use = df_use.dropna(subset=[adv_col, score_col], how='any')

    if df_use.shape[0] == 0:
        bal_score = -1
    else:
        df_use = df_use.sort_values(by=score_col, ascending=False, na_position='last')
        bal_score = balanced_accuracy_score(
            y_true=df_use[adv_col],
            y_pred=np.arange(0, df_use.shape[0]) < np.sum(df_use[adv_col])
        )

    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric=metric_name,
        value=bal_score
    ))

    return pandas_concat_wrapper(df_metrics_list)

def get_future_advancement_rate(df_in, score_col, mdl_name, dg_name='all'):
    # remove year with no next advancements
    yrs_to_drop = []
    for yr in pd.unique(df_in['analysis_year']):
        if np.sum(df_in[df_in['analysis_year'] == yr]['was_adv_next']) == 0:
            yrs_to_drop.append(yr)

    for yr in yrs_to_drop:
        df_in = df_in[df_in['analysis_year'] != yr]

    # compute fraction of materials advanced in following year grouped by recommendation score
    # split into fifths based on percentile, then compute rate for each third
    df_metrics_list = []
    if np.sum(df_in['was_adv'] == True) > 10: # only if we have advancements from next year
        df_was_adv = df_in[df_in['was_adv'] == True]
        df_was_adv = df_was_adv.dropna(subset=[score_col])

        # do X% bins
        step_thresh = 20
        for low_thresh in range(0,100,step_thresh):
            if df_was_adv.shape[0] == 0: #overwrite outputs
                rate = np.nan
                low_raw_thresh=np.nan
                high_raw_thresh=np.nan
                df_group = pd.DataFrame()
            else:
                low_raw_thresh = np.percentile(df_was_adv[score_col], low_thresh)
                high_raw_thresh = np.percentile(df_was_adv[score_col], low_thresh+step_thresh)
                df_group = df_was_adv[(df_was_adv[score_col] > low_raw_thresh) & (df_was_adv[score_col] <= high_raw_thresh)]
                rate = np.sum(df_group['was_adv_next'])/df_group.shape[0]

            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='future_adv_rate_' + str(low_thresh) + 'P-' + str(low_thresh+step_thresh) + 'P',
                value=rate
            ))

            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='raw_thresh_'+str(low_thresh) + 'P',
                value=low_raw_thresh
            ))

            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='n_thresh_' + str(low_thresh) + 'P-' + str(low_thresh+step_thresh)+'P',
                value=df_group.shape[0]
            ))

            if low_thresh + step_thresh >= 100:
                # output the highest threshold as well
                df_metrics_list.append(get_df_adv_metrics(
                    mdl_name=mdl_name,
                    dg=dg_name,
                    metric='raw_thresh_' + str(100) + 'P',
                    value=high_raw_thresh
                ))

        # split based on advanced score directly (into good and bad), not based on percentiles
        # use 85 percentile of all scores as split?
        split_thresh = np.nanpercentile(df_in[score_col],85)
        df_high = df_was_adv[df_was_adv[score_col] > split_thresh]
        df_low = df_was_adv[df_was_adv[score_col] <= split_thresh]

        for df_group, group_name in zip([df_high, df_low], ['high','low']):
            df_group_adv = df_group[df_group['was_adv']]
            rate = np.sum(df_group_adv['was_adv_next'])/df_group_adv.shape[0]

            # store values
            for metric, value in zip(['future_adv_rate_'+group_name, 'n_group_'+group_name],
                                     [rate, df_group_adv.shape[0]]):
                df_metrics_list.append(get_df_adv_metrics(
                    mdl_name=mdl_name,
                    dg=dg_name,
                    metric=metric,
                    value=value
                ))

        # store thresholds
        for metric, value in zip(['raw_thresh_high_low_85P'],
                                 [split_thresh]):
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric=metric,
                value=value
            ))

        #  AUC PR, t test and linear reg stat
        if df_was_adv.shape[0] != 0:  # overwrite outputs
            # AUC PR (pos neg) for future adv rate
            # use advanced materials only
            df_metrics_list.append(get_auc_pr_metrics(
                df_was_adv,
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name=dg_name,
                adv_col='was_adv_next',
                metric_name='future_adv_AUC_PR'
            ))
            # use all materials
            df_metrics_list.append(get_auc_pr_metrics(
                df_in.dropna(subset=[score_col]),
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name=dg_name,
                adv_col='was_adv_next',
                metric_name='future_all_AUC_PR'
            ))

            # compute t stat: materials that are advanced again have a higher adv_rec_score?
            ttest_res = ttest_ind(
                a=df_was_adv[df_was_adv['was_adv_next'] == True][score_col],
                b=df_was_adv[df_was_adv['was_adv_next'] == False][score_col],
                equal_var=False,
                alternative='greater'  # test alternative that a > b
            )
            for metric, value in zip(['t_test_tstat', 't_test_pval'],
                                     [ttest_res.statistic, ttest_res.pvalue]):
                df_metrics_list.append(get_df_adv_metrics(
                    mdl_name=mdl_name,
                    dg=dg_name,
                    metric=metric,
                    value=value
                ))

            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_adv_mean',
                value=np.nanmean(df_was_adv[df_was_adv['was_adv_next'] == True][score_col])
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_adv_std',
                value=np.nanstd(df_was_adv[df_was_adv['was_adv_next'] == True][score_col])
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_adv_n',
                value=np.sum(df_was_adv['was_adv_next'] == True)
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_notadv_mean',
                value=np.nanmean(df_was_adv[df_was_adv['was_adv_next'] == False][score_col])
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_notadv_std',
                value=np.nanstd(df_was_adv[df_was_adv['was_adv_next'] == False][score_col])
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='t_test_notadv_n',
                value=np.sum(df_was_adv['was_adv_next'] == False)
            ))

    return pandas_concat_wrapper(df_metrics_list,axis=0)



def get_confusion_matrix_metrics(df_in, score_col, mdl_name, dg_name='all'):
    # compute confusion matrix for each model
    # sort by score so top materials are first, then only recommend the same number as advanced, then
    df_use = df_in.copy()
    df_use = df_use.dropna(subset=['was_adv',score_col],how='any')

    if df_use.shape[0] == 0:
        conf_mat = np.array([[0,0],[0,0]])
    else:
        df_use = df_use.sort_values(by=score_col, ascending=False, na_position='last')
        conf_mat = confusion_matrix(
            y_true=df_use['was_adv'],
            y_pred=np.arange(0, df_use.shape[0]) < np.sum(df_use['was_adv']),
            labels=[0,1]
        )

    df_metrics_list = []
    # store TP, FP, FN, TN
    for index, metric in zip([(0, 0), (0, 1), (1, 0), (1, 1)], ['TN', 'FP', 'FN', 'TP']):
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric=metric,
            value=conf_mat[index]
        ))
    # store total N
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='n',
        value=np.sum(conf_mat)
    ))

    # compute # of advanced materials in bottom X% of each decision group
    step=5
    if df_use.shape[0] > 0:
        for threshold in range(90,5-step,-step):
            conf_mat = confusion_matrix(
                y_true=df_use['was_adv'],
                y_pred=np.arange(0, df_use.shape[0]) >= df_use.shape[0] * (1 - threshold/100),
                labels=[0,1]
            )
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='perc_adv_in_bottom_'+str(threshold)+'perc',
                value=conf_mat[1, 1] / (conf_mat[1, 1] + conf_mat[1, 0])
            ))
            df_metrics_list.append(get_df_adv_metrics(
                mdl_name=mdl_name,
                dg=dg_name,
                metric='raw_thresh_' + str(threshold) + 'perc',
                value=np.percentile(df_use[score_col], threshold)
            ))

    return pandas_concat_wrapper(df_metrics_list,axis=0)


def get_metrics_for_random_samples(
            df_in,
            score_col,
            mdl_name='random',
            dg_name='all',
            n_repeats=100
        ):
    # metrics: AUC_PR, future_AUC_PR, future_adv_rate_low, future_adv_rate_high, future_adv_rate_0P-20P, etc.
    df_metrics_list = []
    # remove year with no next advancements
    yrs_to_drop = []
    for yr in pd.unique(df_in['analysis_year']):
        if np.sum(df_in[df_in['analysis_year'] == yr]['was_adv_next']) == 0:
            yrs_to_drop.append(yr)

    for yr in yrs_to_drop:
        df_in = df_in[df_in['analysis_year'] != yr]

    # AUC_PR: shuffle was_adv column, compute, repeat
    AUC_PR_adv_list = []
    AUC_PR_notadv_list = []
    df_shuffle = df_in.copy()
    for i_shuffle in range(n_repeats):
        df_shuffle['was_adv'] = df_shuffle['was_adv'].sample(frac=1, replace=False).values
        AUC_PR_adv_list.append(compute_auc_pr(
            df_in=df_shuffle,
            score_col=score_col,
            adv_col='was_adv',
            pos_label=1
        ))
        AUC_PR_notadv_list.append(compute_auc_pr(
            df_in=df_shuffle,
            score_col=score_col,
            adv_col='was_adv',
            pos_label=0
        ))
    # take average and std and append
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='AUC_PR_adv',
        value=np.nanmean(AUC_PR_adv_list)
    ))
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='AUC_PR_notadv',
        value=np.nanmean(AUC_PR_notadv_list)
    ))
    # std
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='AUC_PR_adv_std',
        value=np.nanstd(AUC_PR_adv_list)
    ))
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='AUC_PR_notadv_std',
        value=np.nanstd(AUC_PR_notadv_list)
    ))

    # future_AUC_PR: shuffle was_adv_next columns, compute, repeat
    # AUC PR (pos neg) for future adv rate
    # either use only advanced materials, or use all materials
    for use_adv_only in [0,1]:
        future_adv_AUC_PR_adv_list = []
        future_adv_AUC_PR_notadv_list = []
        df_shuffle = df_in.copy()
        if use_adv_only == 1:
            df_shuffle = df_shuffle[df_shuffle['was_adv'] == True]
            metric_subname = 'adv'
        else:
            metric_subname = 'all'
        for i_shuffle in range(n_repeats):
            df_shuffle['was_adv_next'] = df_shuffle['was_adv_next'].sample(frac=1, replace=False).values
            future_adv_AUC_PR_adv_list.append(compute_auc_pr(
                df_in=df_shuffle,
                score_col=score_col,
                adv_col='was_adv_next',
                pos_label=1
            ))
            future_adv_AUC_PR_notadv_list.append(compute_auc_pr(
                df_in=df_shuffle,
                score_col=score_col,
                adv_col='was_adv_next',
                pos_label=0
            ))
        # take average and std and append
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric='future_'+metric_subname+'_AUC_PR_adv',
            value=np.nanmean(future_adv_AUC_PR_adv_list)
        ))
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric='future_'+metric_subname+'_AUC_PR_notadv',
            value=np.nanmean(future_adv_AUC_PR_notadv_list)
        ))
        # std
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric='future_'+metric_subname+'_AUC_PR_adv_std',
            value=np.nanstd(future_adv_AUC_PR_adv_list)
        ))
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric='future_'+metric_subname+'_AUC_PR_notadv_std',
            value=np.nanstd(future_adv_AUC_PR_notadv_list)
        ))


    # future_adv_rate_high and future_adv_rate_low
    # split based on advanced score directly (into good and bad), not based on percentiles
    # use 85 percentile of all scores as split?
    df_was_adv = df_in[df_in['was_adv'] == True]
    split_thresh = np.nanpercentile(df_in[score_col], 85)

    df_shuffle = df_was_adv.copy()
    high_rate_list = []
    low_rate_list = []
    for i_shuffle in range(n_repeats):
        df_shuffle['was_adv_next'] = df_shuffle['was_adv_next'].sample(frac=1, replace=False).values # shuffle advancement decisions

        df_high = df_shuffle[df_shuffle[score_col] > split_thresh]
        df_low = df_shuffle[df_shuffle[score_col] <= split_thresh]

        for df_group, group_name in zip([df_high, df_low], ['high', 'low']):
            df_group_adv = df_group[df_group['was_adv']]
            rate = np.sum(df_group_adv['was_adv_next']) / df_group_adv.shape[0]
            if group_name == 'high':
                high_rate_list.append(rate)
            else:
                low_rate_list.append(rate)

    # mean, std
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='future_adv_rate_high',
        value=np.nanmean(high_rate_list)
    ))
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='future_adv_rate_high_std',
        value=np.nanstd(high_rate_list)
    ))
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='future_adv_rate_low',
        value=np.nanmean(low_rate_list)
    ))
    df_metrics_list.append(get_df_adv_metrics(
        mdl_name=mdl_name,
        dg=dg_name,
        metric='future_adv_rate_low_std',
        value=np.nanstd(low_rate_list)
    ))

    # store thresholds
    for metric, value in zip(['raw_thresh_high_low_85P'],
                             [split_thresh]):
        df_metrics_list.append(get_df_adv_metrics(
            mdl_name=mdl_name,
            dg=dg_name,
            metric=metric,
            value=value
        ))

    return pandas_concat_wrapper(df_metrics_list, axis=0)


def get_metrics_per_group(
    df_use,
    group_cols,
    mdl_names,
    score_cols
):
    """
    compute advancement and trait metrics for the advancement recommender (classifier)
    compute metrics per group in group cols

    inputs:
        df_use : data frame containing model predictions (proba output) and was_adv
        group_cols : list of columns to group metrics (ex. ['analysis_year,'decision_group_rm'] computes metrics
            for all combinations of years and RMs
            Set as None (the python keyword, not a str) to compute on all data, no groupings.
        score_cols : list of columns to use as scores. Will compute metrics for each score_column
            This lets us score the ML model, yield control, and other controls using the same function
        mdl_names : names of models corresponding to the score_col. This is the output name.
    outputs:
        list of data frames containing metrics for each group

    """
    df_out_list = []
    if group_cols is None:
        for mdl_name, score_col in zip(mdl_names, score_cols):
            df_out_list.append(get_auc_pr_metrics(
                df_use,
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name='all'
            ))

            df_out_list.append(get_balanced_accuracy_score(
                df_use,
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name='all'
            ))

            df_out_list.append(get_confusion_matrix_metrics(
                df_use,
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name='all'
            ))

            # compute next year advancement metric across all decision groups simultaneously for a given year
            df_out_list.append(get_future_advancement_rate(
                df_use,
                score_col=score_col,
                mdl_name=mdl_name,
                dg_name='all'
            ))

        # for some metrics, compute a mean/std value over many random samples...
        df_out_list.append(get_metrics_for_random_samples(
            df_use,
            score_col='adv_rec_score',
            mdl_name='random',
            dg_name='all'
        ))

    else: # group by group cols
        for index, df_dg in df_use.groupby(by=group_cols):
            for mdl_name, score_col in zip(mdl_names, score_cols):
                df_out_list.append(get_auc_pr_metrics(
                    df_dg,
                    score_col=score_col,
                    mdl_name=mdl_name,
                    dg_name=str(index[1])
                ))
                df_out_list.append(get_balanced_accuracy_score(
                    df_dg,
                    score_col=score_col,
                    mdl_name=mdl_name,
                    dg_name=str(index[1])
                ))
                df_out_list.append(get_confusion_matrix_metrics(
                    df_dg,
                    score_col=score_col,
                    mdl_name=mdl_name,
                    dg_name=str(index[1])
                ))

                # compute next year advancement metric across all decision groups simultaneously for a given year
                df_out_list.append(get_future_advancement_rate(
                    df_dg,
                    score_col=score_col,
                    mdl_name=mdl_name,
                    dg_name=str(index[1])
                ))

            # for some metrics, compute a mean/std value over many random samples...
            df_out_list.append(get_metrics_for_random_samples(
                df_dg,
                score_col='adv_rec_score',
                mdl_name='random',
                dg_name=str(index[1])
            ))

    return df_out_list


def compute_model_metrics(df_in,
                          compute_advancement_metrics=False,
                          yield_col='result_diff_YGSMN',
                          trait_prefix='result_'):
    # compute metrics based on model output
    # if compute_advancement_metrics == True, then assumes we have whether each material was actually advanced
    # ( df_use has 'was_adv' and 'was_adv_next' columns)
    # with this info, compute AUC-precision-recall curve, confusion matrix at normalized # of recommendations,
    # and % of advanced materials dropped in the bottom 75 % of materials

    # compute yield-based control. rank ordered result_diff_YGSMN within each decision group
    df_use = df_in.copy()

    decision_groups = pd.unique(df_use['decision_group'])
    df_use = df_use.sort_values(by=yield_col, ascending=False, na_position='last')
    df_use['yield_ranked'] = np.arange(df_use.shape[0], 0, -1)/df_use.shape[0] # high score is better
    df_use['yield_ranked'][df_use['yield_ranked'].isna()] = np.nan

    df_use['yield_percentile'] = 0
    for dg in decision_groups:
        dg_mask = df_use['decision_group'] == dg
        df_use.loc[dg_mask, 'yield_percentile'] = 100 * (1 - np.arange(0, np.sum(dg_mask)) / np.sum(dg_mask))

    df_use['yield_norm'] = (df_use[yield_col] - np.nanmean(df_use[yield_col]))/np.nanstd(df_use[yield_col])
    #df_use['yield_norm'][df_use['yield_norm'].isna()] = -200

    # compute random-number based control -- use get_metrics_for_random_samples instead
    #df_use['random_nums'] = np.random.rand(df_use.shape[0], 1)

    # fill in dme output if missing
    #df_use['abs_mean_prob'] = df_use['abs_mean_prob'].fillna(0)

    df_adv_metrics_list = []# = pd.DataFrame(columns=['mdl', 'decision_group', 'metric', 'value'])

    if 'abs_mean_prob' not in df_use.columns:
        df_use['abs_mean_prob'] = np.nan

    if compute_advancement_metrics == True:
        # compute advancement metrics across all decision groups simultaneously for a given year.
        df_adv_metrics_list.extend(get_metrics_per_group(
            df_use,
            group_cols=['ap_data_sector', 'analysis_year'],
            mdl_names=['ML', 'yield', 'yield_norm', 'dme'],
            score_cols=['adv_rec_score', 'yield_ranked', 'yield_norm', 'abs_mean_prob']
        ))

        # compute advancement metrics across all decision groups and years
        df_adv_metrics_list.extend(get_metrics_per_group(
            df_use,
            group_cols=None,
            mdl_names=['ML', 'yield', 'yield_norm', 'dme'],
            score_cols=['adv_rec_score', 'yield_ranked', 'yield_norm', 'abs_mean_prob']
        ))

        # compute advancement metrics across all maturity groups for all years
        df_adv_metrics_list.extend(get_metrics_per_group(
            df_use,
            group_cols=['ap_data_sector', 'decision_group_rm'],
            mdl_names = ['ML', 'yield', 'yield_norm', 'dme'],
            score_cols = ['adv_rec_score', 'yield_ranked', 'yield_norm', 'abs_mean_prob']
        ))


    # in addition, compute metrics based on the trait columns.
    # this happens regardless of compute_advancement_metrics because we do not need advancement information
    df_trait_metrics_list = []# =  pd.DataFrame(columns=['mdl', 'trait', 'metric', 'value'])
    trait_cols = df_use.columns[[trait_prefix in col for col in df_use.columns]]
    for mdl_name, score_col in zip(['ML','yield','yield_norm', 'dme'],
                                   ['adv_rec_score','yield_ranked','yield_norm', 'abs_mean_prob']):
        was_rec = df_use[score_col] > np.percentile(df_use[score_col], 80)
        mdl_names = [mdl_name for i in trait_cols]

        # compute median for recommended materials, not recommended materials, and the difference
        for metric_suffix, func in zip(['median', 'mean', 'std'], [np.nanmedian, np.nanmean, np.nanstd]):
            df_trait_metrics_list.append(
                pd.DataFrame(
                    data={
                        'mdl': mdl_names,
                        'trait': trait_cols,
                        'metric': 'rec ' + metric_suffix,
                        'value': func(df_use.loc[was_rec == True, trait_cols], axis=0)
                    }
                )
            )

            df_trait_metrics_list.append(
                pd.DataFrame(
                    data={
                        'mdl': mdl_names,
                        'trait': trait_cols,
                        'metric': 'notrec ' + metric_suffix,
                        'value': func(df_use.loc[was_rec == False, trait_cols], axis=0)
                    }
                )
            )

            if metric_suffix != 'std':
                df_trait_metrics_list.append(
                    pd.DataFrame(
                        data={
                            'mdl': mdl_names,
                            'trait': trait_cols,
                            'metric': 'rec-notrec ' + metric_suffix,
                            'value': func(df_use.loc[was_rec == True, trait_cols], axis=0) - func(
                                df_use.loc[was_rec == False, trait_cols], axis=0)
                        }
                    )
                )

    # output metrics
    return pandas_concat_wrapper(df_adv_metrics_list), pandas_concat_wrapper(df_trait_metrics_list,axis=0)


def pandas_concat_wrapper(df_list,axis=0):
    # wrapper handles case when list is empty
    if len(df_list) == 0:
        return pd.DataFrame()

    return pd.concat(df_list,axis=axis)


def compute_input_statistics(df_in, in_cols):
    """
    function computes statistics on data in df_in. Statistics are computed for each column in in_cols
    statistics computed per decision_group and across decision groups
    statistics computed per year and across years

    stats like: mean, median, std, percentiles, max, min if numerical
        number of members of each class, % missing

    df_in : dataframe containing batch data predictions were made on or model
        was trained on. It's assumed that in_cols are contained within df_in
    in_cols : list of input columns used for model. Stats are computed for
        these columns

    usage: compute_input_statistics(df_in=df_tr_proc, in_cols=mdl_class.in_cols)
    """

    # compute statistics per decision group per year
    df_out_list = []# = pd.DataFrame(columns=['decision_group', 'column', 'metric', 'value'])

    # compute stats across all years
    for col in in_cols:  # per column
        is_numeric = ('result' in col) | \
                     ('prediction' in col) | \
                     ('stderr' in col)

        # make sure column name is valid
        if col in df_in.columns:
            # compute metrics for this column, append to df out
            df_out_list.append(get_metrics_df_stats(
                dict_stats=compute_input_statistics_per_col(
                    df_in[col].values,
                    is_numeric
                ),
                decision_group='ALL',
                column=col
            ))

    return pandas_concat_wrapper(df_out_list,axis=0)


def get_metrics_df_stats(dict_stats,decision_group,column):
    df_out_list = []
    for metric in dict_stats.keys():
        df_out_list.append(
            pd.DataFrame([{
                'decision_group': decision_group,
                'column': column,
                'metric': metric,
                'value': dict_stats[metric]
            }])
        )

    return pandas_concat_wrapper(df_out_list,axis=0)


def compute_input_statistics_per_col(data, is_numeric_col):
    # returns a dictionary of metrics for the data
    # operates on a single column
    dict_stats = {}

    to_comp = []
    if is_numeric_col:
        to_comp.extend(['mean', 'median', 'std', 'percentiles', 'max', 'min','perc missing'])
    else:
        to_comp.extend(['count'])

    for comp in to_comp:
        if comp == 'mean':
            dict_stats['mean'] = np.nanmean(data)
        if comp == 'median':
            dict_stats['median'] = np.nanmedian(data)
        if comp == 'std':
            dict_stats['std'] = np.nanstd(data)
        if comp == 'percentiles':
            dict_stats['2perc'] = np.nanpercentile(data,2)
            dict_stats['10perc'] = np.nanpercentile(data, 10)
            dict_stats['25perc'] = np.nanpercentile(data, 25)
            dict_stats['75perc'] = np.nanpercentile(data, 75)
            dict_stats['90perc'] = np.nanpercentile(data, 90)
            dict_stats['98perc'] = np.nanpercentile(data, 98)
        if comp == 'max':
            dict_stats['max'] = np.nanmax(data)
        if comp == 'min':
            dict_stats['min'] = np.nanmin(data)
        if comp == 'count':
            dict_stats['count'] = np.sum(data == 1)
        if comp == 'perc missing':
            dict_stats['perc missing'] = 100 * np.sum(np.isnan(data)) / data.shape[0]

    return dict_stats



#####################################################################
################# defining constants per sector ####################

# may want to move this to a new file

def get_potential_meta_cols():
    cols = [
        'ap_data_sector',
        'analysis_year',
        'decision_group',
        'decision_group_rm',
        'source_id',
        'trial_id',
        'entry_identifier',
        'market_seg',
        'stage',
        'technology',
        'material_type',
        'pipeline_runid'
    ]

    return cols


def get_potential_var_cols():
    cols = [
        'var',
        'trait'
    ]

    return cols


def get_potential_value_cols():
    cols = [
        'alpha_value',
        'value'
    ]

    return cols


def get_potential_agg_dict():
    d = {
        'alpha_value': 'first',
        'selection_remark':'max',
        'rm_estimate': 'mean',
        'value': 'mean'
    }

    return d


def get_training_or_testing_cols(is_infer=0):
    if is_infer==1:
        cols = [
            'ap_data_sector',
            'analysis_year',
            # 'decision_group_rm',
            'entry_identifier',
            #'material_type',
            'material_type_simple',
            'pipeline_runid'
        ]
    else:
        cols = [
            'ap_data_sector',
            'analysis_year',
            #'decision_group_rm',
            'entry_identifier',
            #'material_type',
            'material_type_simple',
            'pipeline_runid'
        ]


    return cols


def get_potential_data_suffixes(is_infer=0):
    # set filenames to check in pred_adv_data_collected folder.
    # this list should contain all possible filenames.

    # this code should be replaced in the future with code that just grabs all files within the provided folder.
    suffixes = [
        #'trial_pheno_tall.csv',
        'pvs_data_tall.csv',
        'plot_result_tall.csv',
        'RM_tall.csv',
        'decisions_tall.csv'
        #'selection_remark_tall.csv'
    ]
    if is_infer:
        suffixes.remove('decisions_tall.csv')
    
    return suffixes



#%%%%%%%%%%%%%%%%%%%%%%%%%%################
########### DEPRECATED?
###########################################

# this approach to model parameters is convoluted and makes changing a single parameter difficult
# rethink whether these should be set, or should just accept a dictionary of parameters....
class ModelParameters:
    """
    Super class of model parameters, from which other classes should inherit
    model specific classes should override the __init__ function. set and get functions should be inherited.
    """
    def __init__(self):
        self.params = {}

    def get_params(self):
        return self.params

    # function to set a parameter. param_name is a str, val is anything
    # example: set_single_param('max_depth',3)
    def set_param(self, param_name=None, param_val=None):
        if param_name is not None:
            self.params[param_name] = param_val

    def set_params(self, params=None):
        self.params = params


class XGBoostModelParameters(ModelParameters):
    # xgboost parameters
    # mdl_params = XGBoostModelParameters()
    # xgboost.XGBClassifier(**mdl_params.params).
    def __init__(self, max_depth=7,
                learning_rate=0.01,
                verbosity=0, # 0-3, 0 is silent
                scale_pos_weight=1, # weight of the positive class, meant to handle imbalanced data
                booster = 'gbtree',
                gamma = 0,
                subsample = 1,
                reg_lambda = 1,
                n_estimators=100,
                enable_categorical = False):

        super().__init__()
        self.params = {
            'max_depth':max_depth,
            'learning_rate':learning_rate,
            'verbosity':verbosity,
            'scale_pos_weight':scale_pos_weight,
            'booster':booster,
            'gamma':gamma,
            'subsample':subsample,
            'reg_lambda':reg_lambda,
            'enable_categorical':enable_categorical,
            'n_estimators':n_estimators
        }


class RandomForestModelParameters(ModelParameters):
    # xgboost parameters
    # mdl_params = XGBoostModelParameters()
    # xgboost.XGBClassifier(**mdl_params.params).
    def __init__(self, max_depth=7,
                class_weight="balanced_subsample",
                bootstrap = True,
                n_estimators=100,
                ):
        super().__init__()
        self.params = {
            'max_depth':max_depth,
            'class_weight':class_weight,
            'bootstrap':bootstrap,
            'n_estimators':n_estimators
        }


class LogisticRegressionParameters(ModelParameters):
    # xgboost parameters
    # mdl_params = XGBoostModelParameters()
    # xgboost.XGBClassifier(**mdl_params.params).
    def __init__(self, class_weight='balanced', penalty='l2', C=1.0, fit_intercept=True, max_iter=1000):
        super().__init__()
        self.params = {
            'class_weight': class_weight,
            'penalty': penalty,
            'C': C,
            'fit_intercept': fit_intercept,
            'max_iter': max_iter
        }


        


class PredAdvMdl:
    def __init__(self,
                 mdl_func=xgboost.XGBClassifier,
                 params=XGBoostModelParameters().params,
                 in_cols=['prediction_YGSMN'],
                 out_col='was_adv',
                 weight_by_year=True,
                 year_col='analysis_year',
                 weight_by_col=False,
                 weight_col='',
                 perform_post_punishing=False,
                 post_punishing_data={}):  #

        self.mdl_func = mdl_func
        self.params = params
        self.mdl = None

        # get column names for important columns
        self.in_cols = in_cols
        self.out_col = out_col

        self.weight_by_year = weight_by_year
        self.year_col = year_col

        self.weight_by_col = weight_by_col
        self.weight_col = weight_col

        # post punishing vars
        self.perform_post_punishing = perform_post_punishing
        # punishing_data is a dict:
        # keys = columns in df_te
        # vals = (thresh, weight)
        self.post_punishing_data = post_punishing_data

        # useful flags, these can't be written to
        self.mdl_trained = False

    # def generate_train_test_data():

    def set_mdl_func(self, mdl_func=None):
        self.mdl_func = mdl_func
        self.mdl_trained = False

    def set_mdl_params(self, params=None):
        self.params = params
        self.mdl_trained = False
        
    def get_mdl_params(self, deep=True):
        return {"alpha": self.alpha, "recursive": self.recursive}
    
        self.mdl_func = mdl_func
        self.params = params
        self.mdl = None

        # get column names for important columns
        self.in_cols = in_cols
        self.out_col = out_col

        self.weight_by_year = weight_by_year
        self.year_col = year_col

        self.weight_by_col = weight_by_col
        self.weight_col = weight_col

        # post punishing vars
        self.perform_post_punishing = perform_post_punishing
        # punishing_data is a dict:
        # keys = columns in df_te
        # vals = (thresh, weight)
        self.post_punishing_data = post_punishing_data

        # useful flags, these can't be written to
        self.mdl_trained = False

    def set_scale_pos_weight(self, df_tr):
        scale_pos_weight = np.sum(df_tr[self.out_col] == False) / np.sum(df_tr[self.out_col] == True)
        self.params['scale_pos_weight'] = scale_pos_weight
        self.mdl_trained = False

    def make_mdl(self):
        if self.mdl_func is not None and self.params is not None:
            self.mdl = self.mdl_func(**self.params)
            self.mdl_trained = False

    # include to match sklearn format
    def fit(self, df_tr):
        self.fit_mdl(df_tr)
            
    def fit_mdl(self, df_tr):
        # make model with given params and function
        self.make_mdl()

        # weight data based on number of samples for each year
        # if requested.
        self.sample_weight = np.ones((df_tr.shape[0],))

        if self.weight_by_year == True:
            for yr in pd.unique(df_tr[self.year_col]):
                yr_mask = df_tr[self.year_col].values == yr
                if np.sum(yr_mask) > 0:
                    self.sample_weight[yr_mask] = (yr_mask.shape[0] / np.sum(yr_mask))

        if self.weight_by_col == True:
            # also weigh samples by weight in self.weight_col
            self.sample_weight = np.multiply(self.sample_weight, df_tr[self.weight_col].values)

        x_tr = df_tr[self.in_cols].values
        y_tr = df_tr[self.out_col].values
        if self.mdl_func == xgboost.XGBClassifier:
            self.mdl.fit(x_tr, y_tr, sample_weight=self.sample_weight)
        else:
            self.mdl.fit(x_tr, y_tr)

        self.mdl_trained = True

    def post_punish(self, df_te):
        for col in self.post_punishing_data.keys():
            thresh = self.post_punishing_data[col][0]
            weight = self.post_punishing_data[col][1]

            # use index to get correct column in x
            col_data = df_te[col].values - thresh
            col_data[col_data < 0] = 0

            punish_vals = weight * col_data
            punish_vals[np.isnan(punish_vals)] = 0
        return punish_vals

    def train_mdl(self, df_tr):
        # set scale pos weight
        if self.mdl_func == xgboost.XGBClassifier:
            self.set_scale_pos_weight(df_tr)

        # fit model
        self.fit_mdl(df_tr)

    def predict(self, df_te):
        # if punishing, set binary output as top 20% of scores
        if self.perform_post_punishing:
            y_proba = self.predict_proba(df_te)
            y_pred = y_proba > np.percentile(y_proba, 80)
        else:  # else use 0.5 as cutoff
            y_pred = self.mdl.predict(df_te[self.in_cols].values)

        return y_pred

    def predict_proba(self, df_te):
        if callable(getattr(self.mdl, "predict_proba", None)):
            y_proba = self.mdl.predict_proba(df_te[self.in_cols].values)[:, 1]
        elif callable(getattr(self.mdl, "decision_function", None)):
            y_proba = self.mdl.decision_function(df_te[self.in_cols].values)
        else:
            y_proba = np.zeros((df_te.shape[0],))

        if self.perform_post_punishing:
            punish_vals = self.post_punish(df_te)
            y_proba = y_proba - punish_vals

        return y_proba

    # get score given input data and output label.
    def score(self, df_te, y_te, get_all_scores=0):
        y_proba = self.predict_proba(df_te)
        y_pred = y_proba > 0.5
        
        # get roc_auc score
        if get_all_scores == 0:
            if np.sum(y_true == False) != 0 and np.sum(y_true == True) != 0:
                roc_auc = roc_auc_score(y_true=y_true, y_score=y_proba)
            else:
                roc_auc = np.nan
            return roc_auc
        else:
            return self.score_provided_predictions(y_true=y_te, y_pred=y_pred, y_proba=y_proba)
        
    # get score given predictions.
    def score_provided_predictions(self,y_true, y_pred, y_proba):
        # get balanced accuracy
        acc = balanced_accuracy_score(y_true=y_true, y_pred=y_pred)  # mdl.score(x,y)

        # get confusion matrix
        conf_mat = confusion_matrix(y_true=y_true, y_pred=y_pred)
        conf_mat = np.transpose(np.transpose(conf_mat) / np.sum(conf_mat, axis=1))

        # get roc_auc score
        if np.sum(y_true == False) != 0 and np.sum(y_true == True) != 0:
            roc_auc = roc_auc_score(y_true=y_true, y_score=y_proba)
        else:
            roc_auc = np.nan

        # get f1 score
        f1 = f1_score(y_true=y_true, y_pred=y_pred)

        return acc, conf_mat, roc_auc, f1
    
    
    
def getGroupingColsPerSector(analysis_sector="CORN_NA_SUMMER"):
    if analysis_sector == "CORN_NA_SUMMER":
        # build a model per stage
        grouping_cols = ['ap_data_sector', 'current_stage']

    elif analysis_sector == "CORN_EAME_SUMMER":
        # build a model per stage
        grouping_cols = ['ap_data_sector', 'current_stage']

    elif analysis_sector == "SOY_NA_SUMMER":
        # build a model per stage
        grouping_cols = ['ap_data_sector', 'current_stage']

    return grouping_cols


def getModelParametersPerSector(df_in, output_year, analysis_sector="CORN_NA_SUMMER", stage=0, args=[]):
    if analysis_sector == "CORN_NA_SUMMER":
        # get training and testing years
        test_yr = output_year  # not a list, a single year
        train_yrs = list(pd.unique(df_in['analysis_year']))  # list (even if just one year), of years to train with
        train_yrs.remove(test_yr)  # remove test year
        if test_yr >= 2020 and 2018 in train_yrs:
            train_yrs.remove(2018)

        # get input and output column names
        # model uses LR subtracted YGSMN and PLHTN, advancement rate rm
        # hyperparameters with the best performance according to a grid search, only tested on 2022.
        traits_to_use = ['ERHTN', 'ERTLP', 'GMSTP', 'GRSNP', 'LRTLP',
                         'STKLP', 'TWSMN', 'reliability']  # reliability is an agg of stderr cols
        mdl_in_cols = []
        mdl_in_cols.extend(['prediction_' + trait for trait in traits_to_use])
        mdl_in_cols.append('prediction_YGSMN-PLHTN-ERHTN-GMSTP')
        mdl_in_cols.append('prediction_PLHTN')
        mdl_in_cols.append('rm_estimate_proc')
        # mdl_in_cols.extend(['dummy_FAIL','dummy_MISS','dummy_PASS']) # only have this for 2022, model can't learn cross-year relationships without data from more years
        cols_to_norm = []
        mdl_out_col = 'was_adv'

        # get parameters for xgboost models
        mdl_params_class = XGBoostModelParameters(max_depth=3, reg_lambda=100, subsample=0.25, learning_rate=0.01)

    elif analysis_sector == "SOY_NA_SUMMER":
        # extract inputs from args
        extra_traits = args[0]
        numeric_raw_traits = args[1]
        numeric_diff_traits = args[2]
        text_traits = args[3]

        # get training and testing years
        test_yr = output_year  # not a list, a single year
        train_yrs = list(pd.unique(df_in['analysis_year']))  # list (even if just one year), of years to train with
        train_yrs.remove(test_yr)  # remove test year

        # get input and output column names
        mdl_in_cols = extra_traits.copy()
        mdl_in_cols.extend(['result_diff_' + trait for trait in numeric_diff_traits])
        mdl_in_cols.extend(['result_' + trait for trait in numeric_raw_traits])

        cols_to_norm = []
        mdl_out_col = 'was_adv'

        # get parameters for xgboost models
        mdl_params_class = XGBoostModelParameters(max_depth=5, reg_lambda=100, subsample=0.2, learning_rate=0.005,
                                                  n_estimators=500)

        # make sure each column in mdl_in col is also in the dataframe
        mdl_in_cols_use = mdl_in_cols.copy()
        for col in mdl_in_cols:
            if col not in df_in.columns:
                mdl_in_cols_use.remove(col)
        mdl_in_cols = mdl_in_cols_use

    return train_yrs, test_yr, mdl_in_cols, mdl_out_col, cols_to_norm, mdl_params_class