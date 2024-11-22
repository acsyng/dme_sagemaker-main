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
    parser.add_argument('--forward_model_year', help='Input String', required=True)
    parser.add_argument('--train_years', help='Input String', required=True)
    parser.add_argument('--material_type', help='Input parent or entry', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--stages', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)
    parser.add_argument('--do_hyperparam_tuning', help='Input String', required=True)

    args = parser.parse_args()
    try:
        # convert stages from comma-separated strings to list integer
        query_vars_dict = {
            'ap_data_sector': args.ap_data_sector,
            'forward_model_year': args.forward_model_year,
            'train_years': args.train_years.split(','),
            'material_type' : args.material_type,
            'target_pipeline_runid': args.target_pipeline_runid,
            'do_hyperparam_tuning': args.do_hyperparam_tuning,
            'stages': args.stages.split(','),
            'force_refresh': args.force_refresh,
            'analysis_type': 'MultiExp',  # need this for the CloudWatchLogger....
            'analysis_run_group': 'performance'  # to get logger to work
       }

        print(query_vars_dict)

        """
        # code to print out installed packages
        installed_packages = pkg_resources.working_set
        for package in installed_packages:
            print(f"{package.key}=={package.version}")
        """

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
