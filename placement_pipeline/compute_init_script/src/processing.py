import argparse
import json

from libs.dme_sql_queries import get_source_ids
from libs.event_bridge.event import error_event


def main():  
    parser = argparse.ArgumentParser(description='sql variables')
    parser.add_argument('--ap_data_sector', help='Input String ', required=True)
    parser.add_argument('--analysis_year', help='Input String', required=True)
    parser.add_argument('--analysis_type', help='Input String', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)
    parser.add_argument('--breakout_level', help='Input String', required=True)

    args = parser.parse_args()
    try:
        query_vars_dict = {}

        source_ids = get_source_ids(args.ap_data_sector, args.analysis_year, args.analysis_type,
                                    args.target_pipeline_runid, args.force_refresh)

        query_vars_dict['ap_data_sector'] = args.ap_data_sector
        query_vars_dict['analysis_year'] =  args.analysis_year
        query_vars_dict['analysis_type'] = args.analysis_type
        query_vars_dict['target_pipeline_runid'] = args.target_pipeline_runid
        query_vars_dict['force_refresh'] = args.force_refresh
        query_vars_dict['source_ids'] = source_ids
        query_vars_dict['breakout_level'] = args.breakout_level

        with open('/opt/ml/processing/data/query_variables.json', 'w') as outfile:
            json.dump(query_vars_dict, outfile)
    except Exception as e:
        error_event(args.ap_data_sector, args.analysis_year, args.target_pipeline_runid, str(e))
        raise e


if __name__ == '__main__':
    main()
