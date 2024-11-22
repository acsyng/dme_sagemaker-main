import json
import os

import pandas as pd

from libs.dme_sql_queries import query_check_entries, \
    get_data_sector_config, get_tops_checks
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.metric_utils import create_check_df


def compute_trial_checks(analysis_type, analysis_year, ap_data_sector, logger):
    logger.info('compute_trial_checks:====START compute_trial_checks====')
    # Compute recipe outputs
    data_sector_config = get_data_sector_config(ap_data_sector)

    checks_df = query_check_entries(ap_data_sector,
                                    analysis_year,
                                    analysis_type,
                                    data_sector_config["spirit_crop_guid"].iloc[0],
                                    data_sector_config["entry_id_source"].iloc[0])
    logger.info('compute_trial_checks:checks_df dataset')
    checks_df = create_check_df(ap_data_sector, analysis_type, checks_df)

    if (ap_data_sector == 'CORN_NA_SUMMER'):
        logger.info('compute_trial_checks:retrieve TOPS check info')
        tops_checks_df = get_tops_checks(ap_data_sector, analysis_year, analysis_type)

        logger.info('compute_trial_checks:concatenate & filter old checks + TOPS checks')
        checks_df = pd.concat([checks_df, tops_checks_df])

        checks_df = checks_df.groupby(
            ['ap_data_sector', 'analysis_year', 'analysis_type', 'source_id', 'entry_id', 'material_type'],
            as_index=False
        ).agg(
            {'cpifl': 'max',
             'cperf': 'max',
             'cagrf': 'max',
             'cmatf': 'max',
             'cregf': 'max',
             'crtnf': 'max',
             'par1_entry_id': 'first',
             'par2_entry_id': 'first'}
        )

        cols = checks_df.columns.tolist()
        cols = cols[0:5] + cols[6:12] + cols[5:6] + cols[12:14]

        checks_df = checks_df[cols]

    # correct for no-check cases
    group_cols = ["analysis_year", "ap_data_sector", "source_id", "material_type"]
    checks_df.loc[checks_df.groupby(group_cols)["cpifl"].transform('max') == 0, "cpifl"] = 1

    for chkfl in ['cperf', 'cagrf', 'cmatf', 'cregf', 'crtnf']:
        checks_df.loc[checks_df.groupby(group_cols)[chkfl].transform('max') == 0, chkfl] = \
            checks_df.loc[checks_df.groupby(group_cols)[chkfl].transform('max') == 0, 'cpifl']

    data_path = os.path.join('/opt/ml/processing/data', 'trial_checks.parquet')

    logger.info(f'compute_trial_checks:result_count:{checks_df.count()}')
    checks_df.to_parquet(data_path)
    logger.info('====compute_trial_checks:====END of the step====')


if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        analysis_year = data['analysis_year']
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        logger = CloudWatchLogger.get_logger()
        try:
            compute_trial_checks(analysis_type, analysis_year, ap_data_sector, logger)
        except Exception as e:
            logger.error(e)
            error_event(ap_data_sector, analysis_year, pipeline_runid, str(e))
            raise e