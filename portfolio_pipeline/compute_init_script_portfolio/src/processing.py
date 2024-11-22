import argparse
import json
import os
import pkg_resources

from libs.dme_sql_queries import get_source_ids
from libs.event_bridge.event import error_event
from libs.performance_lib import predictive_advancement_lib
from libs.config.config_vars import ENVIRONMENT


def main():
    parser = argparse.ArgumentParser(description='sql variables')
    parser.add_argument('--ap_data_sector', help='Input String ', required=True)
    parser.add_argument('--analysis_year', help='Input String', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)

    args = parser.parse_args()
    try:
        query_vars_dict = {
            'ap_data_sector': args.ap_data_sector,
            'analysis_year': args.analysis_year,
            'target_pipeline_runid': args.target_pipeline_runid,
            'force_refresh': args.force_refresh,
            'analysis_type':'SingleExp', # current hack to get around postgress logger thing
            'analysis_run_group': 'portfolio' # to get logger to work
       }

        out_dir = '/opt/ml/processing/data/'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(os.path.join(out_dir,'query_variables.json'), 'w') as outfile:
            json.dump(query_vars_dict, outfile)
    except Exception as e:
        error_event(args.ap_data_sector, args.analysis_year, args.target_pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
