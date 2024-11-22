# import packages
import os
import sys
import json
import pandas as pd, numpy as np
import argparse
import xgboost

from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.performance_lib import preprocessor_class
from libs.performance_lib import performance_helper
from libs.config.config_vars import ENVIRONMENT
from sklearn.metrics import balanced_accuracy_score
from scipy.stats import uniform
from scipy.stats import loguniform
from scipy.spatial.distance import jensenshannon
from mango import Tuner, scheduler
from sklearn.metrics import roc_auc_score
import tarfile


def get_default_model_params(ap_data_sector):
    default_xgb_params = {
        'max_depth': 5,
        'learning_rate': 1e-2,
        'verbosity': 0,
        'booster': 'gbtree',
        'gamma': 0,
        'subsample': 0.75,
        'reg_lambda': 10,
        'enable_categorical': False,
        'n_estimators': 250,
        'eval_metric': balanced_accuracy_score
    }

    return default_xgb_params

# minimize objective function during hyperparameter runing
def xgb_hyperparm_objective_function(args_list):
    # df use, mdl_in_cols, mdl_out_col, ap_data_sector all come from script (acting as global variables....)
    results = []
    for hyper_par in args_list:
        df_te_pred = predictive_advancement_lib.train_models_kfolds_hyperopt(
            df_tr_all,
            mdl_in_cols=mdl_in_cols,
            mdl_out_col=mdl_out_col,
            mdl_year_col='analysis_year',
            params_in=hyper_par,
            data_sector=ap_data_sector
        )

        if 'ap_data_sector' in df_te_pred.columns:
            sample_weight = predictive_advancement_lib.compute_sample_weight_by_year_data_sector(
                year_vals=df_te_pred['analysis_year'].values,
                data_sector_vals=df_te_pred['ap_data_sector'].values
            )
            # lower sample weight of samples from other data sectors
            sample_weight[df_te_pred['ap_data_sector'].values!=ap_data_sector] *= 0.5
        else:
            sample_weight = predictive_advancement_lib.compute_sample_weight_by_year(df_te_pred['analysis_year'].values)

        temp_score = roc_auc_score(
            y_true=df_te_pred['was_adv'].values,
            y_score=df_te_pred['recommendation_score'].values,
            sample_weight=sample_weight
        )
        results.append(temp_score)
    return results


def compute_distribution(x, bounds, n_boot=5000, n_samp=None):
    if n_samp is None:
        n_samp = np.minimum(100,int(np.round(x.shape[0]/2)))

    x = x.reshape((-1,))
    x_counts, x_bins = np.histogram(
        x,
        bins='auto',
        density=False,
        range=bounds
    )

    # get jensen-shannon distance thresh by bootstrapping samples of x, binning and computing distribution.
    dists = []
    for i in range(n_boot):
        samp = np.random.choice(x, size=(n_samp,), replace=True)
        samp_counts, b = np.histogram(samp, bins=x_bins)
        dists.append(jensenshannon(x_counts, samp_counts))

    x_thresh = np.nanpercentile(dists, 95) # set thresh at 95% confidence

    return x_counts, x_bins, x_thresh


def train_advancement_model(df_in, do_hyperparam_tuning=False):
    global df_tr_all
    global mdl_in_cols
    global mdl_out_col

    df_tr_all = df_in.copy()

    # set mdl in and out cols
    mdl_out_col= 'was_adv'
    mdl_year_col = 'analysis_year'
    # mdl_in_cols = all cols except model out col, year col, kfold label

    mdl_in_cols= list(df_tr_all.columns)
    mdl_in_cols.remove(mdl_out_col)
    mdl_in_cols.remove(mdl_year_col)
    mdl_in_cols.remove('kfold_label')
    mdl_in_cols.remove('entry_identifier')
    mdl_in_cols.remove('ap_data_sector')

    cv_fold_label = df_tr_all['kfold_label'].values
    df_tr_all[mdl_out_col] = df_tr_all[mdl_out_col].astype(bool)

    # set default xgb params. Will use if not hyperparameter tuning
    # scale pos weight is computed at each fold.
    default_xgb_params = get_default_model_params(ap_data_sector)

    if do_hyperparam_tuning:

        # uniform(log, scale) gives [log, log+scale]
        # loguniform(min, max) gives [min, max]
        xgb_search = {
            'max_depth': range(5, 10),
            'n_estimators': range(50, 500, 25),
            'reg_lambda': range(50, 100, 5),
            'learning_rate': loguniform(10 ** -3, 0.5),
            'subsample': uniform(0.01, 0.99),
            'gamma':uniform(0,20),
            'min_child_weight':uniform(0,10)
        }

        configuration_dict = dict(
            batch_size=5,
            num_iteration=50,
            optimizer='Bayesian'  # ,
            # exploration=0.7,
            # exploration_decay=0.5
        )
        tuner = Tuner(xgb_search, xgb_hyperparm_objective_function, configuration_dict)
        opt = tuner.maximize()

        xgb_params_use = opt['best_params']
    else:
        opt = []
        xgb_params_use = default_xgb_params.copy()

    mdl_list, df_tr_pred, df_te_pred = predictive_advancement_lib.train_models_kfolds(
        df_in_proc=df_tr_all,
        mdl_in_cols=mdl_in_cols,
        mdl_out_col=mdl_out_col,
        fold_label=cv_fold_label,
        xgb_params=xgb_params_use,
        data_sector=ap_data_sector
    )

    acc, conf_mat, roc_auc, f1 = predictive_advancement_lib.get_evaluation_metrics(
        y_true=df_te_pred[mdl_out_col],
        y_pred=df_te_pred['recommendation_score'] > np.nanpercentile(df_te_pred['recommendation_score'], 50),
        y_proba=df_te_pred['recommendation_score']
    )

    print(
        "Train:",
        df_tr_pred.shape,
        predictive_advancement_lib.get_evaluation_metrics(
            y_true=df_tr_pred[mdl_out_col],
            y_pred=df_tr_pred['recommendation_score'] > np.nanpercentile(
                df_tr_pred['recommendation_score'], 100*np.sum(df_tr_pred[mdl_out_col] == False)/df_tr_pred.shape[0]
            ),
            y_proba=df_tr_pred['recommendation_score']
        )
    )
    print(
        "Test:",
        df_te_pred.shape,
        predictive_advancement_lib.get_evaluation_metrics(
            y_true=df_te_pred[mdl_out_col],
            y_pred=df_te_pred['recommendation_score'] > np.nanpercentile(
                df_te_pred['recommendation_score'], 100*np.sum(df_te_pred[mdl_out_col] == False)/df_te_pred.shape[0]
            ),
            y_proba=df_te_pred['recommendation_score']
        )
    )

    # compute review, advance, drop thresholds based on kfolds validation (df_te_pred)
    # materials below 'review_thresh' will be recommended to be dropped. Set threshold at 25% of advanced materials
        # prevent review thresh from being too low based on the 50th percentile of all predictions
    # below review thresh. Max = 0.75, since stop is not inclusive, use number larger than 0.75
    # set advance thresh as 90th percentile score
    if np.sum(df_te_pred['was_adv']) == 0: # use default, and something is weird
        review_thresh = 0.5
        adv_thresh = 0.75
    else:
        adv_scores = df_te_pred['recommendation_score'][df_te_pred['was_adv']].values
        review_thresh = np.maximum(np.nanpercentile(adv_scores,25),np.nanpercentile(df_te_pred['recommendation_score'],50))
        adv_thresh = np.maximum(review_thresh+0.01, np.nanpercentile(df_te_pred['recommendation_score'],90))

    # meta info
    cv_metrics = {
        'mean_acc': acc,
        'mean_roc_auc': roc_auc,
        'mean_f1': f1,
        'sum_conf_mat': conf_mat
    }
    if not do_hyperparam_tuning:
        opt = []

    # compute train metrics
    # prediction distribution (bin counts and edges)
    pred_count,pred_bins,pred_thresh = compute_distribution(df_te_pred['recommendation_score'].values, bounds=(0,1))

    # yield distribution (bin counts and edges)
    yield_col = [col for col in mdl_in_cols if 'YGSMN' in col or 'YSDMN' in col][0]

    hist_range = (0,500) # non check-relative yield
    if 'diff' in yield_col:
        hist_range = (-20,20)
    if 'perc' in yield_col:
        hist_range = (0,5)
    yield_count, yield_bins, yield_thresh = compute_distribution(df_te_pred[yield_col].values, bounds=hist_range)

    # fraction missing inputs per row (again bin counts and edges)
    n_missing = np.sum(np.isnan(df_te_pred[mdl_in_cols].values),axis=1)
    missing_count, missing_bins, missing_thresh = compute_distribution(n_missing, bounds=(0,len(mdl_in_cols)))

    train_metrics = {
        'pred_count':pred_count,
        'pred_bins':pred_bins,
        'pred_thresh':pred_thresh,
        'yield_count':yield_count,
        'yield_bins':yield_bins,
        'yield_thresh':yield_thresh,
        'missing_count':missing_count,
        'missing_bins':missing_bins,
        'missing_thresh':missing_thresh
    }

    meta_out = {
        'opt': opt,
        'mdl_in_cols': mdl_in_cols,
        'mdl_out_cols': mdl_out_col,
        'did_hyper_tune': do_hyperparam_tuning,
        'xgb_params': xgb_params_use,
        'xgb_version': xgboost.__version__,
        'cv_metrics': cv_metrics,
        'train_metrics': train_metrics,
        'review_thresh': review_thresh,
        'adv_thresh': adv_thresh
    }

    return mdl_list, meta_out, df_te_pred


def train(ap_data_sector,
        pipeline_runid,
        material_type,
        args,
        do_hyperparam_tuning=False,
        write_outputs=1):

    # load in training file for each stage, train model per stage, save
    fpath = args.s3_input_training_data_folder

    fnames = os.listdir(fpath)
    training_data_fnames = []
    stages = []
    for fname in fnames:
        fname_no_ext, ext = os.path.splitext(fname)
        if 'adv_model_training_data' in fname_no_ext and material_type in fname_no_ext and 'stg' in fname_no_ext:
            training_data_fnames.append(fname)
            stg_idx = fname_no_ext.find('stg')
            end_stg_idx = fname_no_ext[stg_idx:].find('-') + stg_idx
            stg = fname_no_ext[stg_idx+3:end_stg_idx]
            stages.append(stg)

    print(list(zip(training_data_fnames, stages)))

    for fname,stg in zip(training_data_fnames, stages):
        df_tr_all = pd.read_csv(os.path.join(
            fpath,
            fname
        ))

        mdl_list, meta_out, df_te_pred = train_advancement_model(
            df_tr_all,
            do_hyperparam_tuning=do_hyperparam_tuning
        )

        # save training and validation sets
        if write_outputs == 1:
            # check to see if file path exists
            data_out_dir = '/opt/ml/processing/data/train/{}'.format(pipeline_runid)
            model_out_dir = '/opt/ml/processing/data/train/adv_mdls'

            for out_dir in [data_out_dir, model_out_dir]:
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

            # put identifier and meta info into filename
            data_postfix = 'stg' + str(int(stg)) + '-' + material_type
            model_postfix = data_postfix + '-' + pipeline_runid

            # setup filenames
            training_data_fname = 'training_data_predictions_' + data_postfix + '.csv'
            #tarfile_fname = 'mdl-' + model_postfix + '.tar.gz'
            mdl_fname = 'mdl_list-'+model_postfix+'.pkl'
            meta_info_fname = 'meta_info-'+model_postfix+'.pkl'

            # save files
            performance_helper.save_object(meta_out, model_out_dir, meta_info_fname)
            performance_helper.save_object(mdl_list, model_out_dir, mdl_fname)
            df_te_pred.to_csv(os.path.join(data_out_dir, training_data_fname), index=False)

            """
            # package model and meta info into tar file
            tar = tarfile.open(os.path.join(model_out_dir, tarfile_fname), "w:gz")
            tar.add(os.path.join(model_out_dir,mdl_fname))
            tar.add(os.path.join(model_out_dir,meta_info_fname))
            tar.close()

            # delete model and meta info pickle file so to not upload to s3
            os.remove(os.path.join(model_out_dir,mdl_fname))
            os.remove(os.path.join(model_out_dir,meta_info_fname))
            """


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_training_data_folder', type=str,
                        help='s3 input training data folder', required=True)
    args = parser.parse_args()

    global ap_data_sector

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        output_year = data['forward_model_year']
        material_type = data['material_type']
        pipeline_runid = data['target_pipeline_runid']
        do_hyperparam_tuning = data['do_hyperparam_tuning'].lower() == 'true' # comes in as str (see compute_init_script_infer)

        try:
            train(
                ap_data_sector=ap_data_sector,
                pipeline_runid=pipeline_runid,
                material_type=material_type,
                do_hyperparam_tuning=do_hyperparam_tuning,
                args=args
            )

        except Exception as e:
            error_event(ap_data_sector, output_year, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()