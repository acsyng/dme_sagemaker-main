import numpy as np
import pandas as pd

import boto3
import os

import pickle  # for saving
import json
import datetime

from libs.config.config_vars import CONFIG


def performance_create_event(ap_data_sector,
                             target_pipeline_runid,
                             status,
                             message):
    """
    function to create an event for the performance pipeline.
    This mimics event_bridge.event.create_event, except only uses inputs that exist for the performance pipeline.
    Breaking this function out from the common file prevents changes in the common file from breaking this code.

    inputs:
        ap_data_sector : data sector of pipeline run
        target_pipeline_runid : runid as string
        status : status of event ('ERROR','TIMEOUT','RUN_REPORT')
        message : string message to put in event
    """

    detail = {
        'pipeline-runid': target_pipeline_runid,
        'ap_data_sector': ap_data_sector,
        'trigger': 'ap_sagemaker_pipeline',
        'phase': '3',
        'status': status,
        'msg': message
    }
    eb = boto3.client('events', region_name='us-east-1', verify=False)
    eb.put_events(
        Entries=[
            {
                'Source': 'syngenta.ap.events',
                'DetailType': 'A&P Workstream Events',
                'EventBusName': CONFIG.get("event_bus"),
                'Detail': json.dumps(detail)
            }
        ]
    )


# helper functions for performance pipeline.
# filter RM to analysis_year and data sector, filter decision group rms
def get_hybrid_rm(df_in, ap_data_sector, analysis_year):
    if isinstance(analysis_year, str):
        analysis_year = int(analysis_year)

    df_out = df_in.copy()
    df_out = df_out[(df_out['analysis_year'] == analysis_year) &
                     (df_out['ap_data_sector_name'] == ap_data_sector) &
                      # multiple conditions nested
                     ((np.abs(df_out['decision_group_rm']-df_out['number_value']) <= 15) |
                         (df_out['decision_group_rm'].isna()) |
                         (df_out['decision_group_rm'] < 25))]
    return df_out


# given a list of strings, generate SQL string to check if var is in that list of strings
# SQL Code: var in (search_ids[0], search_ids[1]....)
def make_search_str(search_ids):
    search_id_str = '('
    # get unique list of source ids
    uniq_search_ids = pd.unique(search_ids)

    if uniq_search_ids.shape[0] == 0:
        return '(\'\')'

    for search_id in uniq_search_ids:
        if search_id is not None:
            if isinstance(search_id, str):
                search_id_str = search_id_str + "'" + search_id + "',"
            else:
                search_id_str = search_id_str + str(search_id) + ","

    # remove last comma, then close parantheses
    search_id_str = search_id_str[:-1] + ')'
    return search_id_str


# get environment prefix for saving files on s3. This is specific to the performance pipeline
def get_s3_prefix(env, project_name='performance'):
    return os.path.join(env, 'dme' ,project_name)


# check if a file exists on s3. fpath = s3 path after bucket
# fpath ='uat/dme/performance/reformatting_performance_pipeline_temp_out/data/SOY_BRAZIL_SUMMER/adv_model_training_data.csv' for example
def check_if_file_exists_s3(fname, bucket='us.com.syngenta.ap.nonprod'):
    # fpath is the full file name, not including the bucket.
    s3_client = boto3.client('s3')
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=fname, MaxKeys=1)
    return 'Contents' in res


# check if a files exists either locally or on s3. 
# fname = full path (not including s3 bucket if applicable)
def check_if_file_exists(fname, read_from_s3=0, bucket=None):
    if read_from_s3 == 1:
        return check_if_file_exists_s3(fname=fname, bucket=bucket)
    else:
        return os.path.exists(fname)


def download_folder_s3(bucket, s3_path, local_path):
    """
    downloads all files in a folder from s3 to a local folder

    bucket : s3 bucket
    s3_path : path to s3 folder , must end in '/' or whatever the separator is
    local_path : path to folder where all files should be downloaded. must end in '/' (or whatever the separator is)

    example usage:
        download_folder_s3(
            bucket='us.com.syngenta.ap.prod'
            s3_path='prod/data/dme/performance/CORN_INDIA_DRY/data_ingestion/'
            local_path='/root/dme_sagemaker/dme_sagemaker/performance_pipeline/test_recipes/new_data/'
        )
        This grabs all ingested data for corn_india_dry and puts it in new_data on the local machine

    """
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix = s3_path):
        local_fname = os.path.join(local_path, obj.key.split(os.path.sep)[-1])
        if not os.path.exists(local_path):
            os.makedirs(os.path.dirname(local_path))
        bucket.download_file(obj.key, local_fname) # save to same path


# parse source id for information about the maturity group
# useful when provided decision group
def parse_source_id_for_rm(source_id):
    source_id_split = source_id.split('-')
    if len(source_id_split)>2:
        rm_out = int(source_id_split[2])
        if rm_out <= 10:
            rm_out = rm_out*5+80
    else:
        rm_out = np.nan

    return rm_out

# parse source id for information about the experiment stage
# useful when provided decision group
def parse_source_id_for_stage(source_id):
    source_id_split = source_id.split('-')
    if len(source_id_split)>2:
        stage = int(source_id_split[1])
    else:
        stage = np.nan

    return stage

# condenses cperf, cagrf, cmatf... flags into a single chkfl for each entry/trait
# df_in needs the following
#     dme_chkfl : which type of check corresponds to this row (usually trait)
#     cperf, cagrf, cmatf, cregf, crtnf : flag denoting whether entry is a performance, agronomic, ... check 
def get_chkfl(df_in):
    df_out = df_in.copy()
    df_out['chkfl'] = 0
    check_names =  ['cperf','cagrf','cmatf','cregf','crtnf']

    for check_name in check_names:
        df_out['chkfl'][df_out['dme_chkfl']==check_name] = df_out[check_name][df_out['dme_chkfl']==check_name]
    df_out = df_out.drop(columns=check_names)

    return df_out


# save object to fpath, fname. 
def save_object(obj, fpath, fname):
    if fname.split('.')[-1] == 'pkl':
        with open(os.path.join(fpath, fname), 'wb') as f:
            pickle.dump(obj, f)
    if fname.split('.')[-1] == 'json':
        with open(os.path.join(fpath, fname), 'w') as f:
            json.dump(obj, f)


# load object (pickle) from fpath, fname
def load_object(fpath, fname):
    with open(os.path.join(fpath,fname), 'rb') as f:
        obj_load = pickle.load(f)

    return obj_load


# get current date, hour, minute, second, microsecond as a file id
def generate_datetime_id():
    # make sure time fields have correct digits so that they are ordered appropriately
    d = datetime.datetime.today()
    date_vals = [str(d.year), str(d.month), str(d.day)]
    date_keys = ['year', 'month', 'day']
    date_dict = {'year': 4, 'month': 2, 'day': 2}

    ts_vals = [str(d.hour), str(d.minute), str(d.second), str(d.microsecond)]
    ts_keys = ['hour', 'minute', 'second', 'microsecond']
    ts_dict = {'hour': 2, 'minute': 2, 'second': 2, 'microsecond': 6}

    # add 0's in front of each date/timestamp val until correct number of digits are reached
    for i_d in range(len(date_vals)):
        # nothing needs to happen for year, will always have 4 digits
        if date_keys[i_d] != 'year' and \
                len(date_vals[i_d]) < date_dict[date_keys[i_d]]:
            # append '0' as many times as necessary to generate correct number of digits
            n_append = date_dict[date_keys[i_d]] - len(date_vals[i_d])
            prepend_str = ''.join(['0' for i_append in range(n_append)])
            date_vals[i_d] = prepend_str + date_vals[i_d]

    for i_ts in range(len(ts_vals)):
        # nothing needs to happen for year, will always have 4.
        if ts_keys[i_ts] != 'year' \
                and len(ts_vals[i_ts]) < ts_dict[ts_keys[i_ts]]:
            # append '0' as many times as necessary to generate correct number of digits
            n_append = ts_dict[ts_keys[i_ts]] - len(ts_vals[i_ts])
            prepend_str = ''.join(['0' for i_append in range(n_append)])

            ts_vals[i_ts] = prepend_str + ts_vals[i_ts]

    d_str = ''.join(date_vals) + '-' + ''.join(ts_vals)

    return d_str


# helper function to check if a material was in the same stage in two years
# also checks to make sure material is not a check or in stage 13, which is dropped
def check_same_stage(df_in, current_stage, next_stage):
    return (df_in[current_stage] == df_in[next_stage]) & \
        (df_in[current_stage] < 7) & (df_in[next_stage] < 7)


# helper function to check if a material was in a higher stage in the next year, implying that it was advanced
# also checks to make sure material is not a check or in stage 13, which is dropped
def check_advancement(df_in, current_stage, next_stage, adv_same_stage=0):
    if adv_same_stage == 1:
        out = ((df_in[current_stage] <= df_in[next_stage]) &
                (df_in[current_stage] < 7) & (df_in[next_stage] <= 7))
    else:
        out = ((df_in[current_stage] < df_in[next_stage]) &
                (df_in[current_stage] < 7) & (df_in[next_stage] <= 7))

    out[(df_in[current_stage].isna()) | (df_in[next_stage].isna())] = np.nan

    return out


def compute_advancements(df_in, adv_same_stage=0):
    df_out = df_in
    was_adv_sector = check_advancement(df_out, 'current_stage', 'next_stage',adv_same_stage=adv_same_stage)
    if 1 == 0 and 'current_stage_other' in df_out.columns:
        was_adv_country = df_out['after_current_season_other'] & \
                          check_advancement(
                              df_out, 'current_stage', 'current_stage_other', adv_same_stage=adv_same_stage
                          )
    else:
        was_adv_country = np.zeros_like(was_adv_sector)
    if 'next_stage_other' in df_out.columns:
        was_adv_season_year = check_advancement(
            df_out, 'current_stage', 'next_stage_other', adv_same_stage=adv_same_stage
        )
    else:
        was_adv_season_year = np.zeros_like(was_adv_sector)

    df_out['was_adv'] = (was_adv_sector) | (was_adv_country) | (was_adv_season_year)

    df_out['was_adv_next'] = (df_out['was_adv']) & check_advancement(
        df_out,
        current_stage='next_stage',
        next_stage='third_stage'
    )

    # make sure these variables have type bool
    df_out['was_adv'] = df_out['was_adv'].astype(bool)
    df_out['was_adv_next'] = df_out['was_adv_next'].astype(bool)

    return df_out


# helper function to write df or object to s3
# this writes a file locally (local_fpath + fname), then uploads to s3 (s3_path + fname), then deletes local file
def write_to_s3(
    obj, 
    fname, 
    local_fpath='/root/dme_sagemaker/dme_sagemaker/performance_pipeline/preprocess_train_recipes',
    s3_path='uat/dme/performance/reformatting_performance_pipeline_temp_out/data', 
    bucket='us.com.syngenta.ap.nonprod'
):
    # s3 vars
    region = boto3.Session().region_name # not necessary
    s3 = boto3.client('s3')
    # write locally
    local_fname = os.path.join(local_fpath,fname)
    s3_fname = os.path.join(s3_path, fname)
    
    if isinstance(obj, pd.DataFrame):
        obj.to_csv(
            local_fname,
            index=False,
        )
    else:
        save_object(obj, local_fpath, fname)

    # upload to s3
    s3.upload_file(
        os.path.join(local_fpath, fname),
        Bucket=bucket,
        Key=s3_fname,
        ExtraArgs={
            'ServerSideEncryption':'aws:kms',
            'SSEKMSKeyId':CONFIG['output_kms_key']
        }
    )
    
    # delete local file
    os.remove(os.path.join(local_fpath, fname))


# function to order output columns for the advancement recommender model. Puts meta columns, recommendation
# and recommenation score first, then orders data columns by prediction (AAE), trial_pheno.
def order_output_columns(df_in):
    df_out = df_in.copy()
    chkfl_cols = [col for col in df_out.columns if 'chkfl' in col or 'cpifl' in col]
    chkfl_cols.extend(['material_type_simple', 'yield_missing'])

    # check for column existance first
    for col in chkfl_cols:
        if col in df_out.columns:
            df_out = df_out.drop(columns=col)

    # meta cols desired -- full list of potential meta cols across train and infer pipeline, hence both entry id
    # and entry identifier (it's either one or the other)
    meta_cols_desired = ['ap_data_sector', 'analysis_year', 'pipeline_runid', 'decision_group', 'dg_stage', 'entry_id',
                 'entry_identifier','material_type', 'current_stage', 'next_stage', 'prev_stage', 'third_stage', 'was_adv',
                 'was_adv_next', 'adv_rec_score', 'recommendation', 'adv_rec_warning', 'weighted_missing',
                 'shap_explanation']

    data_col_order = ['prediction_', 'prediction_perc_', 'stderr_', 'count_',
                      'result_', 'result_diff_', 'result_perc_']

    # check for meta columns, place in order
    meta_cols = []
    for m_col in meta_cols_desired:
        if m_col in df_out.columns:
            meta_cols.append(m_col)

    # check for data columns, place in correct order.
    data_cols = []
    for data_col in data_col_order:
        to_check_for = ['perc_', 'diff_', 'outlier', 'shap']
        data_cols_temp = [] # sort within each group
        for df_col in df_out.columns:
            keep_col = 1
            for to_check_f in to_check_for:
                if to_check_f in df_col and to_check_f not in data_col:
                    keep_col = 0

            if keep_col == 1 and data_col in df_col:
                data_cols_temp.append(df_col)

        # sort columns within each group
        data_cols_temp.sort()
        if len(data_cols_temp) > 0:
            data_cols.extend(data_cols_temp)

    # get shap and outlier columns
    shap_cols = [col + '_shap' for col in data_cols if col + '_shap' in df_out.columns]
    outlier_cols = [col + '_outlier' for col in data_cols if col + '_outlier' in df_out.columns]

    # get remaining columns
    remaining_cols = list(set(df_out.columns).difference(
        meta_cols + data_cols + shap_cols + outlier_cols + ['mdl_cols']
    ))

    # reorder columns
    if 'mdl_cols' in df_out.columns:
        df_out = df_out[meta_cols + data_cols + shap_cols + remaining_cols + outlier_cols + ['mdl_cols']]
    else:
        df_out = df_out[meta_cols + data_cols + shap_cols + remaining_cols + outlier_cols]

    return df_out


####################################################
################ potentially deprecated below######
def generate_filename(descr_list, fname_prefix, uniq_id, ext='.pkl'):
    # mdl descr list contains descriptive vars -- [ap_data_sector, output_year, stage]
    # mdl prefix describes the object that is saved (mdl, preprocessor, etc.)
    # mdl name is a concatenation of mdl_descr_list, mdl_prefix, and date + timestamp (as a unique identifier)
    # generate model name
    fname = fname_prefix
    for descr in descr_list:
        fname = fname + '-' + descr

    return fname + '-' + uniq_id + ext



def get_dme_output_metrics_schema():
    # manually define output schema.
    out_cols = [
        'ap_data_sector',
        'analysis_year',
        'analysis_type',
        'pipeline_runid',
        'source_id',
        'stage',
        'decision_group_rm',
        'material_type'
    ]
    # breakout columns
    for i in [1, 2, 3, 4]:
        out_cols.append('breakout_level_' + str(i))
        out_cols.append('breakout_level_' + str(i) + '_value')

    # metric, material, and output cols
    out_cols.extend([
        'metric_type',
        'metric_method',
        'entry_id',
        'check_entry_id',
        'metric',
        'trait',
        'prediction',
        'stddev',
        'diff_prediction',
        'diff_stddev',
        'cpifl',
        'chkfl',
        'check_prediction',
        'check_stddev',
        'check_diff_prediction',
        'check_diff_stddev',
        'check_chkfl',
        'count',
        'check_count',
        'abs_mean_pctchk',
        'abs_mean_prob',
        'abs_var_prob',
        'rel_mean_zscore',
        'rel_mean_prob',
        'rel_var_prob',
        'dme_text'
    ])

    return out_cols


def check_for_year_offset(ap_data_sector):
    # return True if harvest and plant year are offset (planted in ~November, harvested in ~April)
    return ('_BRAZIL_' in ap_data_sector and 'SAFRINHA' not in ap_data_sector) \
        or '_LAS_SUMMER' in ap_data_sector \
        or '_LAN_SUMMER' in ap_data_sector \
        or ap_data_sector == 'CORN_INDIA_DRY' \
        or ap_data_sector == 'CORN_BANGLADESH_DRY'

