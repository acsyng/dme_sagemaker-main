"""
Useful functions for portfolio project
"""
import numpy as np
import pandas as pd
import os
import boto3
import pickle

# given an array of strings, generate SQL string to check if var is in that list of strings
# SQL Code: var in (search_ids[0], search_ids[1]....)
# currently handles ' within search ids...other escape characters may cause issues
def make_search_str(search_ids):
    search_id_str = '('
    # get unique list of source ids
    uniq_search_ids = pd.unique(search_ids)

    if uniq_search_ids.shape[0] == 0:
        return '(\'\')'

    for search_id in uniq_search_ids:
        if isinstance(search_id, str):
            if "'" in search_id:  # put \' in string...
                search_id_split = search_id.split("'")

                search_id_str = search_id_str + "'"

                for s in search_id_split:
                    search_id_str = search_id_str + s + "\\" + "'"

                search_id_str = search_id_str[:-2] + "',"  # remove last addition|
            else:
                search_id_str = search_id_str + "'" + search_id + "',"

    # remove last comma, then close parantheses
    search_id_str = search_id_str[:-1] + ')'
    return search_id_str


# join strings, keep unique entries via a comma separate string, remove duplicates
# x is an array of strings
# x : ['hi','there','hi']
# out : 'hi,there'

# meant to be used in a pandas groupby ... groupby().agg(lambda x : join_synonyms_unique(x))
def join_synonyms_unique(x):
    if x.shape[0] == 0:
        return ""

    out_str = ""
    for syn in x:  # for each item
        if syn not in out_str:  # if we haven't seen it, append it
            out_str = out_str + syn + ","
    return out_str[:-1]  # remove final comma


# for synonyms, remove prefix if present. This takes "USGH GH3392E3" and makes it "GH3392E3" which matches other tables
# this is used when building material table.
def remove_synonym_prefix(x):
    temp = x.split(' ')
    if len(temp) > 1:
        return temp[-1]
    else:
        return x


# helper function to remove "Brand" from variety name...
# x is a string
def remove_brand(x):
    if isinstance(x, str):
        return x.replace('Brand', '').replace('BRAND', '').replace('brand', '').strip()
    else:
        return x


def get_cropfact_data_ingestion(df_trial):
    """


    """

    # pivot trialing data, prepare for cropfact
    df_trial_piv = pd.pivot_table(
        df_trial[df_trial['trait'] == 'GMSTP'][
            ['ap_data_sector', 'year', 'x_longitude', 'y_latitude', 'irrigation', 'plant_date', 'harvest_date',
             'be_bid', 'trait', 'result_numeric_value']],
        index=['ap_data_sector', 'year', 'x_longitude', 'y_latitude', 'irrigation', 'plant_date', 'harvest_date',
               'be_bid'],
        columns='trait',
        values='result_numeric_value'
    ).reset_index()

    # round moisture to 0 decimals
    df_trial_piv['GMSTP'] = df_trial_piv['GMSTP'].round(0)
    df_trial_piv = df_trial_piv.drop(columns=['be_bid']).drop_duplicates()

    # preprocess df_trial_piv...
    # get unique cropfact inputs.
    df_cropfact_in = df_trial_piv.copy()

    # convert dates to datetime
    df_cropfact_in['plant_date'] = pd.to_datetime(df_cropfact_in['plant_date'])
    df_cropfact_in['harvest_date'] = pd.to_datetime(df_cropfact_in['harvest_date'])

    # preprocess unique inputs for cropfact
    df_uniq_input = cropfact_async.prepareDataFrameForCropFact(df_cropfact_in)
    df_uniq_input.head()

    # Compute recipe outputs from inputs
    # parse dataframe and build jsons.
    # these jsons are inputs to the cropfact api
    jsons = cropfact_async.getCropFactJSONWrapper(df_uniq_input)
    print(len(jsons))

    # get crop fact data
    my_api = "https://cropfact.syngentaaws.org/services/cropphysiology"
    my_header = {'x-api-key': 'N1sgY9MO7O2vky35RBSLL20napGp3qRH6LWOAdKT', 'Content-Type': 'application/json'}
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    # df_cropfact = loop.run_until_complete(cropfact_async.getRecords(jsons, my_api, my_header))

    max_tries = 50
    n_tries = 0
    n_error = 1000  # initial value to start the while loop, just needs to be bigger than 0 to start
    batch_size = 50

    # so cropfact sometimes complains about 'too many requests' all at once.
    # this code will retry the calls that returned 'too many requests' at a slightly smaller batch_size
    # until either the number of retries is exceeded or there are no more 'too many requests' errors
    df_cropfact = pd.DataFrame()

    jsons_use = jsons.copy()
    while n_tries < max_tries and n_error > 0:
        print('n_tries: ', n_tries, ' of max_tries: ', max_tries)
        start_time = time.time()
        df_cropfact_temp = loop.run_until_complete(
            cropfact_async.getRecords(jsons_use, my_api, my_header, batch_size=batch_size));
        print('n_tries: ', n_tries, '  df_cropfact_temp.shape: ', df_cropfact_temp.shape)

        dur = time.time() - start_time

        n_calls = len(jsons_use)
        # get idx of 'too many requests' error
        error_idx = np.nonzero((df_cropfact_temp['error'].values == 'Too Many Requests') | (
                df_cropfact_temp['error'].values == 'Endpoint request timed out'))[0]
        jsons_use = list(np.array(jsons_use)[error_idx])

        # drop rows of df_cropfact_temp with errors, append to df_cropfact
        df_cropfact_temp = df_cropfact_temp[df_cropfact_temp['error'].isnull()]
        df_cropfact = pd.concat((df_cropfact, df_cropfact_temp), axis=0)
        # update while loop parameters
        n_error = error_idx.shape[0]
        n_tries = n_tries + 1
        print('n_error: ', n_error)
        print('n_tries: ', n_tries)
        # update batch_size
        batch_size = np.minimum(batch_size, np.ceil(n_error / 5).astype(int))
        if batch_size <= 0:
            batch_size = 1

    print("done")

    print('df_cropfact shape after loop run until complete: ', df_cropfact.shape)
    df_cropfact = df_cropfact[df_cropfact['error'].isnull()]  # only keep calls that returned data

    # preprocess df before merging, convert date format, convert irr format, perform rounding,
    # replace plant_date with plant_date_as_date, same with harvest_date. This is consistent with df_cropfact format

    # convert dates to datetime
    df_trial_piv['plant_date'] = pd.to_datetime(df_trial_piv['plant_date'])
    df_trial_piv['harvest_date'] = pd.to_datetime(df_trial_piv['harvest_date'])

    df_trial_piv = cropfact_async.prepareDataFrameForCropFact(df_trial_piv,
                                                              to_dos=['perform_rounding', 'convert_irr_format',
                                                                      'convert_date_format'])

    # convert irrigation/drainage column to presence or absence
    df_trial_piv['irr_stat'] = 'Absence'
    df_trial_piv['irr_stat'][(df_trial_piv['irr_conv'] == 'IRR') | (df_trial_piv['irr_conv'] == 'LIRR')] = 'Presence'
    df_trial_piv['drain_stat'] = 'Absence'
    df_trial_piv['drain_stat'][df_trial_piv['irr_conv'] == 'TILE'] = 'Presence'
    # update df_crop's irrigation to the same presence/absence format

    df_cropfact.Irrigation.loc[~df_cropfact['Irrigation'].isnull()] = 'Presence'
    df_cropfact.Irrigation.loc[df_cropfact['Irrigation'].isnull()] = 'Absence'
    # rename columns in df_crop
    df_crop_mapper = {'Planting': 'plant_date', 'Harvest': 'harvest_date', 'Irrigation': 'irr_stat',
                      'DrainageSystem': 'drain_stat', 'GMSTP_input': 'GMSTP'}
    df_crop_pre_merge = df_cropfact.rename(columns=df_crop_mapper)
    df_crop_pre_merge['plant_date'] = pd.to_datetime(df_crop_pre_merge['plant_date'])
    df_crop_pre_merge['harvest_date'] = pd.to_datetime(df_crop_pre_merge['harvest_date'])

    # do merge
    on_list = ['x_longitude', 'y_latitude', 'plant_date', 'harvest_date', 'irr_stat', 'drain_stat', 'GMSTP']

    df_out = df_trial_piv.merge(right=df_crop_pre_merge, how='left', on=on_list)
    # drop rows if missing data
    # grain yield potential will be empty if no cropfact data
    df_out = df_out.dropna(subset=['GMSTP', 'GrainYieldPotential'])
    # df_out=df_out.drop_duplicates()

    date_cols = df_out.columns[df_out.columns.str.contains('PhenologicalStage')]

    for colname in date_cols:
        df_out[colname] = pd.to_datetime(df_out[colname], yearfirst=True).dt.dayofyear

    col_header_dict = {'PhenologicalStage': 'Stage', 'Planting': 'P', 'Harvest': 'H'}

    for key, newstr in col_header_dict.items():
        df_out.columns = df_out.columns.str.replace(key, newstr)

    df_out.columns = df_out.columns.str.lower()

    for col in ['calciumcarbonatecontent', 'rootdepthconstraint']:
        df_out[col] = df_out[col].astype(float)

    # get irrflag and tileflag from irr_conv
    df_out['irrflag'] = df_out['irr_conv'] == 'IRR'
    df_out['tileflag'] = df_out['irr_conv'] == 'TILE'

    df_out = df_out.drop(columns=
                         ['plant_date_as_date', 'harvest_date_as_date', 'error', 'countrysubdivision'] +
                         [col for col in df_out.columns if 'stage' in col]
                         )

    return df_out


# check if a file exists on s3. fpath = s3 path after bucket
# fpath ='uat/dme/performance/reformatting_performance_pipeline_temp_out/data/SOY_BRAZIL_SUMMER/adv_model_training_data.csv' for example
# do not use a / at the beginning of fpath.
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


def get_folder_contents_s3(bucket, fpath):
    s3_client = boto3.client('s3')
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=fpath)
    return res


def get_latest_timestamp(bucket, fpath):
    """
    Gets most recent time stamp from S3 "folder" containing "folders" with many timestamps.
    Time stamps are in YYYYMMDD_HH_MM_SS format

    inputs :
        bucket = S3 bucket
        fpath = starting file path stem. List of timestamps is next in folderpath

    Outputs :
        most recent time stamp.
    """
    res = get_folder_contents_s3(bucket=bucket, fpath=fpath)

    # go through keys, get most recent time stamp
    curr_stamp = None
    for cont in res['Contents']:
        temp_fpath = cont['Key']
        time_stamp = temp_fpath.replace(fpath, '').split('/')[0]

        if curr_stamp is None:
            curr_stamp = time_stamp
        elif time_stamp > curr_stamp:  # we use YYYYMMDD_HH_MM_SS format so that this comparison is true
            curr_stamp = time_stamp

    return curr_stamp


def load_placement_model_s3(bucket, fpath):
    """
    this code loads a model in from S3. It takes in the path the folder the model exists in, not the path to the model
    this gets around the timestamp in the model name

    Function returns the model

    Inputs:
    Bucket : s3 bucket
    fpath : path to folder model exists in

    Outputs:
    model

    """
    s3_client = boto3.client('s3')
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=fpath)

    fname = None
    for cont in res['Contents']:
        if 'lightgbm_model_' in cont['Key']:
            fname = cont['Key']

    if fname is not None:
        resp = s3_client.get_object(Bucket=bucket, Key=fname)
        return pickle.loads(resp['Body'].read())
    else:
        return None


def replace_nan_protected(s, to_replace, replace_with):
    if isinstance(s, str):
        return s.replace(to_replace, replace_with)
    else:
        return s


def clean_decimal_points(df_in):
    # replace .0 and .1 and ... at end of string that exists for corn data
    df_out = df_in.copy()
    for col in df_out.columns:
        if df_out[col].dtype == object:
            for i in range(10):
                df_out[col] = df_out[col].apply(lambda x: replace_nan_protected(x, '.' + str(i), ''))
            # replace just decimal point if no number follows
            df_out[col] = df_out[col].apply(lambda x: replace_nan_protected(x, '.', ''))
    return df_out


def clean_prefix_str(s, pref):
    if isinstance(s, str):
        for i in range(10):
            s = s.replace(pref + str(i), '')
        # replace only pref if no number follows.
        s = s.replace(pref,'')
    return s


def alias_bebid(df_in, df_alias, in_col='be_bid', alias_col='be_bid', truth_col='be_bid_truth'):
    # given a dataframe mapping bebid to the "truth" be bid,
    # convert all bebids to the "truth" be_bid

    # merge with alias map, drop original be_bid columns, rename "truth" column
    df_out = df_in.merge(
        df_alias,
        left_on=in_col,
        right_on=alias_col,
        how='left'
    )
    if in_col != alias_col:
        drop_cols = [in_col, alias_col]
    else:
        drop_cols = [in_col]

    # if no alias, move original column to truth col, then drop original columns and rename truth col.
    # this keeps the original be bid in df_in if it isn't in df_alias
    df_out[truth_col] = df_out[[truth_col]].combine_first(
        df_out[[in_col]].rename(columns={in_col:truth_col})
    )

    df_out = df_out.drop(columns=drop_cols).rename(columns={truth_col:in_col})

    return df_out


def parse_rm(x, data_sector):
    # split strings and numeric - searches specifically for country code (first 2 chars), then brand code (next 2 chars)
    # returns country code, brand code, and number following
    num = ''.join(i for i in x if i.isalpha() == False)

    if data_sector == 'SOY_NA_SUMMER': # they use 0,1,2,3 scale.
        # if no period ("."), put period at front. Then make float
        if '.' not in num:
            num = '.' + num
    # CORN NA uses 80,85,90, etc. scale in their databases
    num = float(num)

    # first two chars are country, rest is brand code
    s = ''.join(i for i in x if i.isalpha() == True)

    if len(s) > 2:
        country_code = s[0:2]
        brand_code = s[2:]
    else:
        country_code = s
        brand_code = ''

    return country_code, brand_code, num

