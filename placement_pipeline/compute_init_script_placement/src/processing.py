import argparse
import json
import os
from libs.event_bridge.event import error_event, create_event
from libs.config.config_vars import CONFIG
from libs.logger.cloudwatch_logger import CloudWatchLogger


def main():
    parser = argparse.ArgumentParser(description='sql variables')
    parser.add_argument('--ap_data_sector', help='Input String ', required=True)
    parser.add_argument('--analysis_year', help='Input String', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--historical_build', help='Input String', required=True)

    args = parser.parse_args()

    logger = CloudWatchLogger.get_logger(args.target_pipeline_runid)
    try:
        create_event(CONFIG.get('event_bus'), args.ap_data_sector, args.analysis_year, args.target_pipeline_runid,
                     None, None, 'START',
                     f'Start Sagemaker Placement pipeline: {args.target_pipeline_runid}', None)
        logger.info('compute_init_script_placement_step:====START compute_init_script_placement_step====')
        query_vars_dict = {'ap_data_sector': args.ap_data_sector, 'analysis_year': args.analysis_year,
                           'analysis_type': 'SingleExp', 'target_pipeline_runid': args.target_pipeline_runid,
                           'historical_build': args.historical_build}

        out_dir = '/opt/ml/processing/data/'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(os.path.join(out_dir, 'query_variables.json'), 'w') as outfile:
            json.dump(query_vars_dict, outfile)

        logger.info('compute_init_script_placement_step:====END compute_init_script_placement_step====')
    except Exception as e:
        logger.error(e)
        error_event(args.ap_data_sector, args.analysis_year, args.target_pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
