import argparse
import json

from libs.event_bridge.event import error_event


def main():  
    parser = argparse.ArgumentParser(description='sql variables')
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)
    parser.add_argument('--analysis_type', help='Input String',required=True)

    args = parser.parse_args()
    try:
        query_vars_dict = {}
        query_vars_dict['target_pipeline_runid'] = args.target_pipeline_runid
        query_vars_dict['force_refresh'] = args.force_refresh
        query_vars_dict['analysis_type'] = args.analysis_type

        with open('/opt/ml/processing/data/query_variables.json', 'w') as outfile:
            json.dump(query_vars_dict, outfile)
    except Exception as e:
        error_event(args.ap_data_sector, args.analysis_year, args.target_pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
