import pandas as pd
import numpy as np
import pickle
import os
import datetime
import boto3
from libs.config.config_vars import S3_BUCKET

"""
create_model_input(df, mask)

Receives an input data structure for modeling and performs transformations to return X and y for the model. 
If a column mask is provided, it will be applied. This function may be applied to training sets and test sets.

Inputs:
    df: input dataset
    rfecv_mask: Column mask to apply to X, if one was generated during model fitting. (Optional)

Outputs:
    X: df to feed into a model
    y: dependent variable column to feed into a model

Author: Keri Rehm, 2023-Jun-09
"""


def create_model_input(df, rfecv_mask=np.empty(shape=0), ap_data_sector=None):
    info_cols = ["year", "ap_data_sector", "trial_id", "plot_barcode", "be_bid",
                 "loc_selector", "cpifl", "cperf", "analysis_year", "YGSMN"]

    sec_info_cols = ["par_hp1_be_bid", "par_hp2_be_bid"]

    sec_info_cols_sample = ["par_hp1_sample", "par_hp2_sample"]
    drop_cols = []
    # y = df["YGSMN"]
    # X = df.drop(columns=info_cols)

    if ap_data_sector == "CORN_NA_SUMMER":
        # X["maturity_group"] = pd.Categorical(X.maturity_group)
        df["maturity_group"] = df["maturity_group"].astype('int')
        print('maturity group: ', df.maturity_group.unique().tolist())
        y = df["YGSMN"]
        X = df.drop(columns=info_cols + sec_info_cols)

    if ap_data_sector == "CORN_BRAZIL_SUMMER":
        df = df[df.tpp_region != 'B']
        df["tpp_region"] = df["tpp_region"].astype('int')
        print('tpp_region: ', df.tpp_region.unique().tolist())
        y = df["YGSMN"]
        X = df.drop(columns=info_cols + sec_info_cols)

    if ap_data_sector == "CORN_BRAZIL_SAFRINHA":
        df = df[df.tpp_region != 'B']
        df["tpp_region"] = df["tpp_region"].astype('int')
        print('tpp_region: ', df.tpp_region.unique().tolist())
        y = df["YGSMN"]
        X = df.drop(columns=info_cols + sec_info_cols_sample)

    if ap_data_sector == "CORNGRAIN_EAME_SUMMER":
        # df["et"] = df["et"].astype('int')
        # print('et: ', df.ET.unique().tolist())
        y = df["YGSMN"]
        X = df.drop(columns=info_cols + ['et'] + sec_info_cols)

    if ap_data_sector == "SUNFLOWER_EAME_SUMMER":
        # df = df[df.tpp_region != '4']
        # df["tpp_region"] = df["tpp_region"].astype(str)
        print('tpp_region: ', df.tpp_region.unique().tolist())
        y = df["YGSMN"]
        X = df.drop(columns=info_cols + ['tpp_region'])

    if rfecv_mask.shape[0] > 0:
        X = X.loc[:, rfecv_mask == 1]

    return X, y


def create_model_input_cornsilage(df, rfecv_mask=np.empty(shape=0), ap_data_sector=None):
    info_cols = ["year", "ap_data_sector", "trial_id", "plot_barcode", "be_bid", "par_hp1_be_bid", "par_hp2_be_bid",
                 "loc_selector", "cpifl", "cperf", "analysis_year", "YSDMN"]
    drop_cols = []
    # y = df["YGSMN"]
    # X = df.drop(columns=info_cols)

    if ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
        # df["et"] = df["et"].astype('int')
        # print('et: ', df.ET.unique().tolist())
        y = df["YSDMN"]
        X = df.drop(columns=info_cols + ['et'])

    if rfecv_mask.shape[0] > 0:
        X = X.loc[:, rfecv_mask == 1]

    return X, y


def create_model_input_soy(df, data_sector, rfecv_mask=np.empty(shape=0)):
    # info_cols = ["year", "ap_data_sector", "trial_id", "plot_barcode", "be_bid", "par_hp1_be_bid", "par_hp2_be_bid", "loc_selector", "cpifl", "cperf", "analysis_year", "YGSMN"]
    if data_sector == 'SOY_BRAZIL_SUMMER':
        info_cols = ["year", "ap_data_sector", 'tpp_region', "trial_id", "plot_barcode", "be_bid", "loc_selector",
                     "cpifl", "cperf", "analysis_year", "YGSMN"]
    if data_sector == 'SOY_NA_SUMMER':
        info_cols = ["year", "ap_data_sector", 'maturity_zone', "trial_id", "plot_barcode", "be_bid", "loc_selector",
                     "cpifl", "cperf", "analysis_year", "YGSMN"]
    drop_cols = []
    y = df["YGSMN"]
    X = df.drop(columns=info_cols)
    # X["maturity_group"] = pd.Categorical(X.maturity_group)

    if rfecv_mask.shape[0] > 0:
        X = X.loc[:, rfecv_mask == 1]

    return X, y


"""
create_output
Generates model predictions based on an input df, supplemental data df, model estimator, and mask (optional). 
Performs the transformation YSHMN -> YGSMN as per SPIRIT doc

Inputs:
    df: cropFact+other features that are used in model
    suff_df: Supplemental df of information that is not used in model but is useful for interpretation, explainability, and graphing
    estimator: Model estimator object
    rfecv_mask: Column mask to apply to X, if one was generated during model fitting. (Optional)
    
Outputs:
    out_df: output with supplemental info, features that were used in the model, y values used to train model, and model predictions 
    
Author: Keri Rehm, 2023-Jun-09
"""


def create_output(df, suff_df, estimator,
                  lower_estimator=None, middle_estimator=None, upper_estimator=None, rfecv_mask=np.empty(shape=0),
                  ap_data_sector=None):
    X, y = create_model_input(df, rfecv_mask, ap_data_sector)
    df["YGSMNp"] = estimator.predict(X)
    df['YGSMNp_10'] = lower_estimator.predict(X)
    df['YGSMNp_50'] = middle_estimator.predict(X)
    df['YGSMNp_90'] = upper_estimator.predict(X)
    out_df = suff_df.merge(df, on=["year", "ap_data_sector", "analysis_year", "trial_id"], how="inner")

    return out_df


def create_output_cornsilage(df, suff_df, estimator,
                             lower_estimator=None, middle_estimator=None, upper_estimator=None,
                             rfecv_mask=np.empty(shape=0),
                             ap_data_sector=None):
    X, y = create_model_input_cornsilage(df, rfecv_mask, ap_data_sector)
    df["YSDMNp"] = estimator.predict(X)
    df['YSDMNp_10'] = lower_estimator.predict(X)
    df['YSDMNp_50'] = middle_estimator.predict(X)
    df['YSDMNp_90'] = upper_estimator.predict(X)
    out_df = suff_df.merge(df, on=["year", "ap_data_sector", "analysis_year", "trial_id"], how="inner")

    return out_df


def create_output_soy(df, data_sector, suff_df, estimator, lower_estimator=None, middle_estimator=None,
                      upper_estimator=None,
                      rfecv_mask=np.empty(shape=0)):
    X, y = create_model_input_soy(df, data_sector, rfecv_mask)
    df["YGSMNp"] = estimator.predict(X)
    df['YGSMNp_10'] = lower_estimator.predict(X)
    df['YGSMNp_50'] = middle_estimator.predict(X)
    df['YGSMNp_90'] = upper_estimator.predict(X)
    out_df = suff_df.merge(df, on=["year", "ap_data_sector", "analysis_year", "trial_id"], how="inner")

    return out_df


def save_object(obj, fpath, fname):
    with open(os.path.join(fpath, fname), 'wb') as f:
        pickle.dump(obj, f)


def load_object(fpath, fname):
    with open(os.path.join(fpath, fname), 'rb') as f:
        obj_load = pickle.load(f)

    return obj_load


def generate_file_identifier():
    # make sure time fields have correct digits so that they are ordered appropriately
    d = datetime.datetime.today()
    date_vals = [str(d.year), str(d.month), str(d.day)]
    date_keys = ['year', 'month', 'day']
    date_dict = {'year': 4, 'month': 2, 'day': 2}

    ts_vals = [str(d.hour), str(d.minute), str(d.second), str(d.microsecond)]
    ts_keys = ['hour', 'minute', 'second', 'microsecond']
    ts_dict = {'hour': 2, 'minute': 2, 'second': 2, 'microsecond': 6}

    for i_d in range(len(date_vals)):
        if date_keys[i_d] != 'year' and len(date_vals[i_d]) < date_dict[
            date_keys[i_d]]:  # nothing needs to happen for year, will always have 4.
            # append '0' as many times as necessary to generate correct number of digits
            n_append = date_dict[date_keys[i_d]] - len(date_vals[i_d])
            prepend_str = ''.join(['0' for i_append in range(n_append)])

            date_vals[i_d] = prepend_str + date_vals[i_d]

    for i_ts in range(len(ts_vals)):
        if ts_keys[i_ts] != 'year' and len(ts_vals[i_ts]) < ts_dict[
            ts_keys[i_ts]]:  # nothing needs to happen for year, will always have 4.
            # append '0' as many times as necessary to generate correct number of digits
            n_append = ts_dict[ts_keys[i_ts]] - len(ts_vals[i_ts])
            prepend_str = ''.join(['0' for i_append in range(n_append)])

            ts_vals[i_ts] = prepend_str + ts_vals[i_ts]

    d_str = ''.join(date_vals) + '-' + ''.join(ts_vals)

    return d_str


def folder_files(prefix_name_filter):
    s3 = boto3.resource('s3')
    bucket_name = S3_BUCKET
    bucket = s3.Bucket(bucket_name)
    prefix_name = prefix_name_filter
    list_names = []
    for obj in bucket.objects.filter(Prefix=prefix_name):
        abc = obj.key

        abc = abc.replace(prefix_name, '')

        list_names.append(abc)

    for ln in list_names:
        if ".pkl" not in ln:
            list_names.remove(ln)

    return list_names


def find_latest_model(files_in_folder, all_models=None):
    for f in files_in_folder:
        if ".pkl" not in f:
            files_in_folder.remove(f)

    pkl_files_set = list()
    for am in files_in_folder:
        print('before', am)
        if all_models is True:
            if "allmodels" in am:
                print('after', am)
                pkl_files_set += [am]
                print('pkl_files_set: ', pkl_files_set)
            else:
                next
        else:
            print('else called')
            pkl_files_set = files_in_folder

    pkl_files = pkl_files_set
    print('pkl_files: ', pkl_files)
    n_files = len(pkl_files)
    print('n_files: ', n_files)
    if n_files == 1:
        # latest_model = "lightgbm_model.pkl"
        latest_model = pkl_files[0]
        model_date_time = latest_model.split('_')[2]
        # print('m: ', m)
        model_date = model_date_time.split('-')[0]
        # print('model_date',model_date)
        model_time = model_date_time.split('-')[1]

        return latest_model, model_date, model_time

    else:
        fname_date, fname_ts = 0, 0

        for m in pkl_files:
            # print(m, ' of ', pkl_files)
            m1 = m.replace('.pkl', '')
            if m1 == 'lightgbm_model':
                next
            else:
                model_date_time = m1.split('_')[2]
                # print('m: ', m)
                model_date = model_date_time.split('-')[0]
                # print('model_date',model_date)
                model_time = model_date_time.split('-')[1]
                # print('model_time',model_time)

                if int(model_date) >= fname_date and int(model_time) >= fname_ts:
                    fname_date = int(model_date)
                    fname_ts = int(model_time)
                # print(fname_date, '.......' , fname_ts)
                if all_models is True:
                    latest_model = 'lightgbm_allmodels_' + model_date + '-' + model_time + '.pkl'
                else:
                    latest_model = 'lightgbm_model_' + model_date + '-' + model_time + '.pkl'
        print('latest_model: ', latest_model)
        return latest_model, model_date, model_time
