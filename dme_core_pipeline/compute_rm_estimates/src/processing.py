import json

import pandas as pd
import s3fs

from libs.config.config_vars import S3_BUCKET, S3_DATA_PREFIX
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.postgres.postgres_connection import PostgresConnection

INSERT_RM_ESTIMATES = """INSERT INTO dme.rm_estimates(
    ap_data_sector, 
    entry_id,
    rm_estimate,
    analysis_year,
    rm_model,
    decision_group_name
)
SELECT ap_data_sector, 
    entry_id,
    rm_estimate,
    analysis_year,
    rm_model,
    decision_group_name
FROM {}
"""

if __name__ == '__main__':
    logger = CloudWatchLogger.get_logger()
    logger.info('compute_rm_estimates start')
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        analysis_type = data['analysis_type']
        pipeline_runid = data['target_pipeline_runid']
        parquet_path = (f's3a://{S3_BUCKET}/{S3_DATA_PREFIX}/{pipeline_runid}/{analysis_type}/'
                        f'compute_erm_test_combined/*.parquet')
        s3 = s3fs.S3FileSystem()
        parquet_files = s3.glob(parquet_path)
        pc = PostgresConnection()
        for file in parquet_files:
            temp_table = pc.get_temp_table('rm_estimates')
            logger.info(f'Put {file} to dme.rm_estimates table')
            with s3.open(file, 'rb') as s3f:
                df = pd.read_parquet(s3f)
                pc.write_pandas_df(df, temp_table)
            pc.run_sql(INSERT_RM_ESTIMATES.format(temp_table))
            pc.run_sql(f'DROP TABLE {temp_table}')
    logger.info('compute_rm_estimates end')
