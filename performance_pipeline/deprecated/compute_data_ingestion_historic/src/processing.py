# import packages
import os
import json

from libs.event_bridge.event import error_event
from libs.performance_lib import data_ingestion_with_preprocessing_lib


def data_ingestion(ap_data_sector,
                     input_years,
                    write_outputs=1):

    # setup out dir if writing to s3
    if write_outputs == 1:
        # check to see if file path exists
        out_dir = '/opt/ml/processing/data/data_ingestion'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    else:
        out_dir=None

    # files are saved within ingestion functions
    if ap_data_sector == 'SOY_BRAZIL_SUMMER':
        data_ingestion_lib.run_soy_brazil_ingestion(
            input_years=input_years,
            write_outputs=write_outputs,
            out_dir=out_dir
        )


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        input_years = data['input_years']
        pipeline_runid = data['target_pipeline_runid']

        try:
            data_ingestion(
                ap_data_sector=ap_data_sector,
                input_years=input_years
            )

        except Exception as e:
            error_event(ap_data_sector, pipeline_runid, str(e))
            raise e


if __name__ == '__main__':
    main()