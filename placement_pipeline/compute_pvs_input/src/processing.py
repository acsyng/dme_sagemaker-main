import json
import os

from libs.event_bridge.event import error_event
from libs.dme_sql_queries import merge_pvs_input


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)       
        DKU_DST_ap_data_sector = data['ap_data_sector']
        DKU_DST_analysis_year = data['analysis_year']
        DKU_DST_analysis_type = data['analysis_type']
        current_source_ids = data['source_ids']
        pipeline_runid = data['target_pipeline_runid']
        try:
            # Compute recipe outputs
            pvs_input_py_df = merge_pvs_input(DKU_DST_ap_data_sector,
                                          DKU_DST_analysis_year,
                                          DKU_DST_analysis_type,
                                          current_source_ids)
            data_path = os.path.join('/opt/ml/processing/data', 'pvs_input.parquet')
            pvs_input_py_df.to_parquet(data_path)
        except Exception as e:
            error_event(DKU_DST_ap_data_sector, DKU_DST_analysis_year, pipeline_runid, str(e))
            raise e

    
if __name__ == '__main__':
    main()
