import json
import argparse

from libs.event_bridge.event import error_event
from libs.performance_lib.load_ingested_data import load_ingested_data


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--s3_input_data_ingestion_folder', type=str,
                        help='s3 input data ingestion folder', required=True)
    parser.add_argument('--output_load_data_ingestion_folder', type=str,
                        help='local output folder', required=True)
    args = parser.parse_args()

    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['input_years']
        #material_type = data['material_type']
        pipeline_runid = data['target_pipeline_runid']

        try:

            marker_traits = load_ingested_data(
                ap_data_sector=ap_data_sector,
                input_years=input_years,
                get_parents=0,
                pipeline_runid=pipeline_runid,
                args=args
            )

        except Exception as e:
            error_event(ap_data_sector, '', pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()
