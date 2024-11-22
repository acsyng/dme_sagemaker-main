import json
import os
import pandas as pd
import numpy as np

from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.placement_lib.geno_queries import get_trial_data, \
    get_trial_data_CORN_BRAZIL_SUMMER, get_trial_data_CORN_BRAZIL_SAFRINHA, \
    get_trial_data_SOY_BRAZIL, get_trial_data_SOY_NA, get_trial_data_CORNGRAIN_EAME_SUMMER, \
    get_trial_data_SUNFLOWER_EAME_SUMMER, get_trial_data_CORNSILAGE_EAME_SUMMER, get_het_pools_sunflower_receiver, \
    get_het_pools_sunflower_donor, get_sunflower_be_bid_sample, get_sunflower_receiver, get_sunflower_donor, \
    get_sunflower_bebids
from libs.event_bridge.event import error_event
import boto3

from libs.placement_lib.teams_notification import teams_notification
from libs.placement_s3_functions import get_s3_prefix
from libs.config.config_vars import ENVIRONMENT, S3_BUCKET


def trial_data_by_year_function(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, logger, rec_df=None,
                                donor_df=None):
    try:

        print('DKU_DST_analysis_year')
        print('DKU_DST_analysis_year: ', DKU_DST_analysis_year)
        print('DKU_DST_analysis_year')

        if int(DKU_DST_analysis_year) == 2023 and DKU_DST_ap_data_sector == "CORN_NA_SUMMER":
            s3 = boto3.resource('s3')

            bucket_name = S3_BUCKET
            bucket = s3.Bucket(bucket_name)
            sql_df = pd.DataFrame()
            buckets_objects = bucket.objects.filter(
                Prefix=os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_tops_data_query_3/data/tops_data_query_3',
                                    DKU_DST_ap_data_sector, DKU_DST_analysis_year, ''))
            for obj in buckets_objects:
                if "_SUCCESS" in obj.key:
                    next
                else:
                    # print(obj.key)
                    df = pd.read_parquet(os.path.join('s3://', bucket_name, obj.key))
                    df['ap_data_sector'] = DKU_DST_ap_data_sector
                    # print('df shape: ', df.shape)
                    sql_df = pd.concat([sql_df, df], ignore_index=True)
                    # print('grid_df shape: ', grid_df.shape)

            print('tops_corn_2023_df shape: ', sql_df.shape)
        else:
            if DKU_DST_ap_data_sector == 'CORN_NA_SUMMER':
                sql_df = get_trial_data(DKU_DST_analysis_year)
                print('CORN_NA_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'SOY_BRAZIL_SUMMER':
                sql_df = get_trial_data_SOY_BRAZIL(DKU_DST_analysis_year)
                print('SOY_BRAZIL_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'CORNGRAIN_EAME_SUMMER':
                sql_df = get_trial_data_CORNGRAIN_EAME_SUMMER(DKU_DST_analysis_year)
                print('CORNGRAIN_EAME_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
                sql_df = get_trial_data_CORNSILAGE_EAME_SUMMER(DKU_DST_analysis_year)
                print('CORNSILAGE_EAME_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'SOY_NA_SUMMER':
                sql_df = get_trial_data_SOY_NA(DKU_DST_analysis_year)
                print('SOY_NA_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'CORN_BRAZIL_SUMMER':
                sql_df = get_trial_data_CORN_BRAZIL_SUMMER(DKU_DST_analysis_year)
                print('CORN_BRAZIL_SUMMER sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'CORN_BRAZIL_SAFRINHA':
                sql_df = get_trial_data_CORN_BRAZIL_SAFRINHA(DKU_DST_analysis_year)
                print('CORN_BRAZIL_SAFRINHA sql_df shape: ', sql_df.shape)
            elif DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
                sql_df_init = get_trial_data_SUNFLOWER_EAME_SUMMER(DKU_DST_analysis_year)
                print('SUNFLOWER_EAME_SUMMER sql_df_init shape: ', sql_df_init.shape)

                sql_df = sql_df_init.merge(donor_df, how='left', on='be_bid')  # .merge(rec_df, how='left', on='be_bid')
                print('SUNFLOWER_EAME_SUMMER sql_df shape: ', sql_df.shape)
            else:
                print("Enter valid ap data sector")

        # Get trial data and quick QC
        trial_data_df = sql_df.drop(["trait", "result"], axis=1).drop_duplicates()
        print('trial_data_df.shape initial qc: ', trial_data_df.shape)

        # QC 1: start to standardize irrigation
        trial_data_df['irrigation'][(trial_data_df['irrigation'] != 'IRR') & (trial_data_df['irrigation'] != 'LIRR') & (
                trial_data_df['irrigation'] != 'TILE')] = 'NONE'
        # QC 3: fill in any planting/harvest years that aren't equal to exp year
        # plant_mask = ~(trial_data_df["plant_date"].astype(str).str[:4] == trial_data_df["year"].astype(str))
        # trial_data_df["plant_date"].loc[plant_mask] = trial_data_df["year"].loc[plant_mask].astype(str) + trial_data_df["plant_date"].loc[plant_mask].astype(str).str[4:]
        # harvest_mask = ~(trial_data_df["harvest_date"].astype(str).str[:4] == trial_data_df["year"].astype(str))  # TODO: make compatible with winter trials
        # trial_data_df["harvest_date"].loc[harvest_mask] = trial_data_df["year"].loc[harvest_mask].astype(str) + trial_data_df["harvest_date"].loc[harvest_mask].astype(str).str[4:]

        # QC 4: groupby loc_selector and fill in missing plant, harvest
        trial_data_df["plant_date"] = trial_data_df[["loc_selector", "plant_date"]].groupby(
            ["loc_selector"]).ffill().bfill()
        trial_data_df["harvest_date"] = trial_data_df[["loc_selector", "harvest_date"]].groupby(
            ["loc_selector"]).ffill().bfill()

        print('fill in missing plant, harvest trial_data_df.shape: ', trial_data_df.shape)

        # QC 5: drop duplicates
        trial_data_df = trial_data_df.drop_duplicates()
        print('# QC 5: drop duplicates trial_data_df.shape: ', trial_data_df.shape)

        if DKU_DST_ap_data_sector == 'CORNSILAGE_EAME_SUMMER':
            drop_cols = ['SDMCP', 'YSDMN']
        else:
            drop_cols = ['GMSTP', 'YGSMN']
        # plot_data_df = sql_df[["plot_barcode", "trait", "result"]].pivot_table(columns="trait", index="plot_barcode", values="result").reset_index().dropna(subset=['GMSTP', 'YGSMN'])
        plot_data_df = sql_df[["plot_barcode", "trait", "result"]].pivot_table(columns="trait", index="plot_barcode",
                                                                               values="result").reset_index().dropna(
            subset=drop_cols)

        print('plot_data_df pivot table, plot_data_df.shape: ', plot_data_df.shape)
        if DKU_DST_ap_data_sector not in ("SOY_BRAZIL_SUMMER", "SOY_NA_SUMMER") and "HAVPN" in plot_data_df.columns:
            plot_data_df["HAVPN"] = plot_data_df["HAVPN"].fillna(value=84000)

        if DKU_DST_ap_data_sector != "CORNSILAGE_EAME_SUMMER":
            plot_data_df["GMSTP_cf"] = np.minimum(plot_data_df["GMSTP"].round().astype(int), 34)

        trial_data_by_year_df = trial_data_df.merge(plot_data_df, on="plot_barcode", how="inner")

        print('trial_data_by_year_df after merge with plot_data_df, trial_data_by_year_df.shape: ', trial_data_by_year_df.shape)
        trial_data_by_year_df = trial_data_by_year_df.loc[~(trial_data_by_year_df.x_longitude.isnull()) &
                                                          ~(trial_data_by_year_df.y_latitude.isnull()) &
                                                          ~(trial_data_by_year_df.plant_date.isnull()) &
                                                          ~(trial_data_by_year_df.harvest_date.isnull()), :]

        print('trial_data_by_year_df after filtering for x y plant and harvest, trial_data_by_year_df.shape: ', trial_data_by_year_df.shape)

        if DKU_DST_ap_data_sector == "CORNSILAGE_EAME_SUMMER":
            trial_data_by_year_df["cropfact_id"] = trial_data_by_year_df["loc_selector"] + "_" + trial_data_by_year_df[
                                                                                                     "plant_date"].astype(
                str).str[:10] + "_" + trial_data_by_year_df["harvest_date"].astype(str).str[:10] + "_" + \
                                                   trial_data_by_year_df["irrigation"].astype(str).str[:10]
        else:
            trial_data_by_year_df["cropfact_id"] = trial_data_by_year_df["loc_selector"] + "_" + trial_data_by_year_df[
                                                                                                     "plant_date"].astype(
                str).str[:10] + "_" + trial_data_by_year_df["harvest_date"].astype(str).str[:10] + "_" + \
                                                   trial_data_by_year_df["irrigation"].astype(str).str[:10] + "_" + \
                                                   trial_data_by_year_df["GMSTP_cf"].astype(str)

        trial_data_by_year_df["plant_date"] = pd.to_datetime(trial_data_by_year_df["plant_date"].astype(str).str[:10], yearfirst=True)
        trial_data_by_year_df["harvest_date"] = pd.to_datetime(
            trial_data_by_year_df["harvest_date"].astype(str).str[:10], yearfirst=True, errors="coerce")
        print('trial_data_by_year_df before harvest data drop na, trial_data_by_year_df.shape: ', trial_data_by_year_df.shape)
        trial_data_by_year_df = trial_data_by_year_df.dropna(axis=0, subset=['harvest_date'])

        print('before writing to db trial_data_by_year_df.shape: ', trial_data_by_year_df.shape)

        # Write recipe outputs
        trial_data_by_year_dir_path = os.path.join('/opt/ml/processing/data/trial_data_by_year', DKU_DST_ap_data_sector,
                                                   DKU_DST_analysis_year)

        trial_data_by_year_data_path = os.path.join(trial_data_by_year_dir_path, 'trial_data_by_year.parquet')
        print('trial_data_by_year_data_path: ', trial_data_by_year_data_path)
        isExist = os.path.exists(trial_data_by_year_dir_path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(trial_data_by_year_dir_path)

        trial_data_by_year_df.to_parquet(trial_data_by_year_data_path)

    except Exception as e:
        logger.error(e)
        error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
        message = f'Placement trial_data_by_year error report for {ENVIRONMENT} {pipeline_runid} {DKU_DST_ap_data_sector} {DKU_DST_analysis_year}'
        print('message: ', message)
        teams_notification(message, None, pipeline_runid)
        print('teams message sent')
        raise e


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_ap_data_sector = data['ap_data_sector']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger(pipeline_runid)

        historical_build = data['historical_build']

        # years = ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']
        if historical_build == 'True':
            years = [str(x) for x in range(2014, int(DKU_DST_analysis_year) + 1)]
        else:
            years = [str(DKU_DST_analysis_year)]

        if DKU_DST_ap_data_sector == 'SUNFLOWER_EAME_SUMMER':
            # batch_size = 3000
            # print('Starting query for be_bid and sample_id')
            # be_bid_sample_df = get_sunflower_be_bid_sample()
            # print('be_bid_sample_df.shape: ', be_bid_sample_df.shape)
            # be_bid_sample_df.info()
            # print('Starting query for rec df')
            # rec_df_init = get_sunflower_receiver(be_bid_sample_df, batch_size)
            # print('rec_df_init.shape: ', rec_df_init.shape)
            # rec_df = rec_df_init.drop(columns=['be_bid']).merge(be_bid_sample_df, how='inner', left_on='receiver_p',
            #                                                     right_on='be_bid').rename(
            #     columns={'receiver_p': 'par_hp2_be_bid', 'sample_id': 'par_hp2_sample'})
            # print('rec_df.shape: ', rec_df.shape)
            # rec_df.info(verbose=True)
            # print('Starting query for donor df')
            # donor_df_init = get_sunflower_donor(be_bid_sample_df, batch_size)
            # print('donor_df_init.shape: ', donor_df_init.shape)
            # donor_df = donor_df_init.drop(columns=['be_bid']).merge(be_bid_sample_df, how='inner', left_on='donor_p',
            #                                                         right_on='be_bid').rename(
            #     columns={'donor_p': 'par_hp1_be_bid', 'sample_id': 'par_hp1_sample'})
            # print('donor_df.shape: ', donor_df.shape)
            print('Starting query for be_bids')
            donor_df = get_sunflower_bebids()
            rec_df = None

            # print('Starting query for rec and donor df')
            # rec_df = get_het_pools_sunflower_receiver()
            # rec_df = rec_df.rename(columns={'receiver_p': 'par_hp2_be_bid',
            #                                 'receiver_sample_id': 'par_hp2_sample'})
            # print('rec_df.shape: ', rec_df.shape)
            # donor_df = get_het_pools_sunflower_donor()
            # donor_df = donor_df.rename(columns={'donor_p': 'par_hp1_be_bid',
            #                                     'donor_sample_id': 'par_hp1_sample'})
            # print('donor_df.shape: ', donor_df.shape)
        else:
            rec_df = None
            donor_df = None

        for input_year in years:
            file_path = os.path.join(get_s3_prefix(ENVIRONMENT), 'compute_trial_data_by_year/data/trial_data_by_year',
                                     DKU_DST_ap_data_sector, input_year, 'trial_data_by_year.parquet')

            # check_file_exists = check_if_file_exists_s3(file_path)
            # if check_file_exists is False:
            print('Creating file in the following location: ', file_path)
            trial_data_by_year_function(DKU_DST_ap_data_sector, input_year, pipeline_runid, logger, rec_df, donor_df)
            print('File created')
            #    print()

            # else:
            #    print('File exists here: ', file_path)


if __name__ == '__main__':
    main()
