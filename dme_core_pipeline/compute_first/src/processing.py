import argparse
import json
import os
import s3fs

import numpy as np
import pandas as pd
import polars as pl

from libs.config.config_vars import CONFIG
from libs.denodo.denodo_connection import DenodoConnection
from libs.dme_sql_queries import (
    query_trial_input,
    query_pvs_input,
    query_check_entries,
)
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.metric_utils import create_check_df


def main():
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument(
        "--s3_output_trial_numeric_input",
        type=str,
        help="s3 output trial_numeric_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_trial_text_input",
        type=str,
        help="s3 output trial_text_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_pvs_input",
        type=str,
        help="s3 output pvs_input bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_trial_checks",
        type=str,
        help="s3 output trial_checks bucket",
        required=True,
    )

    args = parser.parse_args()
    with open(
        "/opt/ml/processing/input/input_variables/query_variables.json", "r"
    ) as f:
        data = json.load(f)
        ap_data_sector = data["ap_data_sector"]
        analysis_year = data["analysis_year"]
        analysis_run_group = data["analysis_run_group"]
        current_source_ids = data["source_ids"]
        pipeline_runid = data["target_pipeline_runid"]
        breakout_level = data["breakout_level"]
        logger = CloudWatchLogger.get_logger()
        try:
            s3 = s3fs.S3FileSystem(
                s3_additional_kwargs={
                    "ServerSideEncryption": "aws:kms",
                    "KMSKeyId": CONFIG["output_kms_key"],
                }
            )

            logger.info("compute_pvs_input:====START compute_pvs_input====")
            # Compute recipe outputs
            pvs_input_df = query_pvs_input(
                ap_data_sector,
                analysis_year,
                analysis_run_group,
                current_source_ids,
                breakout_level,
            )

            logger.info(f"compute_pvs_input:result_count:{pvs_input_df.count()}")
            print(pvs_input_df.schema)
            pvs_input_df.write_parquet(
                args.s3_output_pvs_input,
                compression="gzip",
                compression_level=2,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": ["decision_group"],
                    "filesystem": s3,
                },
            )
            logger.info("compute_pvs_input:====END compute_pvs_input====")

            logger.info("compute_trial_checks:====START compute_trial_checks====")

            checks_df = query_check_entries(
                ap_data_sector, analysis_year, analysis_run_group, current_source_ids
            )
            logger.info("compute_trial_checks:checks_df dataset")
            checks_df = create_check_df(ap_data_sector, checks_df)

            logger.info(f"compute_trial_checks:result_count:{checks_df.count()}")
            print(checks_df.schema)
            checks_df.write_parquet(
                args.s3_output_trial_checks,
                compression="gzip",
                compression_level=2,
                use_pyarrow=True,
                pyarrow_options={
                    "filesystem": s3,
                },
            )
            logger.info("====compute_trial_checks:====END of the step====")

            logger.info(
                "compute_trial_numeric_input:====START compute_trial_numeric_input===="
            )
            trial_numeric_input_df = query_trial_input(
                ap_data_sector,
                analysis_year,
                current_source_ids,
                breakout_level,
                "numeric",
            )
            logger.info(
                f"compute_trial_numeric_input:result_count:{trial_numeric_input_df.count()}"
            )
            print(trial_numeric_input_df.schema)

            trial_numeric_input_df.write_parquet(
                args.s3_output_trial_numeric_input,
                compression="gzip",
                compression_level=2,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": ["decision_group"],
                    "filesystem": s3,
                },
            )
            logger.info("compute_trial_numeric_input:====END of the step========")

            logger.info(
                "compute_trial_text_input:====START compute_trial_text_input===="
            )
            trial_text_input_df = query_trial_input(
                ap_data_sector,
                analysis_year,
                current_source_ids,
                breakout_level,
                "alpha",
            )
            logger.info(
                f"compute_trial_text_input:result_count:{trial_text_input_df.shape[0]}"
            )

            with s3.open(args.s3_output_trial_text_input, mode="wb") as f:
                trial_text_input_df.write_csv(
                    f,
                )
            logger.info("compute_trial_text_input:====END of the step========")

        except Exception as e:
            logger.error(e)
            error_event(
                ap_data_sector,
                analysis_year,
                pipeline_runid,
                str(e),
                analysis_run_group,
            )
            raise e


if __name__ == "__main__":
    main()
