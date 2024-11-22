import argparse
import json
import s3fs
import time

from libs.config.config_vars import CONFIG
from libs.dme_sql_queries import get_source_ids
from libs.event_bridge.event import error_event, create_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.config.config_vars import CONFIG
from libs.dme_sql_queries import (
    query_trial_input,
    query_pvs_input,
    query_check_entries,
)
from libs.metric_utils import create_check_df


def main():
    parser = argparse.ArgumentParser(description="sql variables")
    parser.add_argument(
        "--ap_data_sector", help="Input string ap_data_sector", required=True
    )
    parser.add_argument(
        "--analysis_year", help="Input string analysis_year", required=True
    )
    parser.add_argument(
        "--target_pipeline_runid", help="Input string pipeline_runid", required=True
    )
    parser.add_argument(
        "--force_refresh", help="Input String force refresh flag", required=True
    )
    parser.add_argument(
        "--breakout_level", help="Input String breakout level(s)", required=True
    )
    parser.add_argument(
        "--analysis_run_group", help="Input String analysis_run_group", required=True
    )
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
    logger = CloudWatchLogger.get_logger(
        args.target_pipeline_runid, args.analysis_run_group
    )
    try:
        create_event(
            CONFIG.get("event_bus"),
            args.ap_data_sector,
            args.analysis_year,
            args.target_pipeline_runid,
            args.analysis_run_group,
            None,
            "START",
            f"Start Sagemaker DME pipeline: {args.target_pipeline_runid}",
            args.breakout_level,
        )
        if args.force_refresh == "false":
            logger.info(
                "compute_init_script_step: force_refresh is false, wait 5 minutes to ensure pvs update is completed"
            )
            time.sleep(300)

        logger.info("compute_init_script_step:====START compute_init_script_step====")
        query_vars_dict = {}

        source_ids = get_source_ids(
            args.ap_data_sector,
            args.analysis_year,
            args.analysis_run_group,
            args.breakout_level,
            args.force_refresh,
        )
        logger.info(
            f"compute_init_script_step source count={len(source_ids.split(','))}, source_ids= {source_ids}"
        )

        query_vars_dict["ap_data_sector"] = args.ap_data_sector
        query_vars_dict["analysis_year"] = args.analysis_year
        query_vars_dict["target_pipeline_runid"] = args.target_pipeline_runid
        query_vars_dict["force_refresh"] = args.force_refresh
        query_vars_dict["source_ids"] = source_ids
        query_vars_dict["breakout_level"] = args.breakout_level
        query_vars_dict["analysis_run_group"] = args.analysis_run_group

        with open("/opt/ml/processing/data/query_variables.json", "w") as outfile:
            json.dump(query_vars_dict, outfile)
        logger.info("compute_init_script_step:====END compute_init_script_step====")
        if str(source_ids) == "('')":
            logger.info("compute_init_script_step: no source ids for input data")
            create_event(
                CONFIG.get("event_bus"),
                args.ap_data_sector,
                args.analysis_year,
                args.target_pipeline_runid,
                args.analysis_run_group,
                None,
                "END",
                f"Empty data in Sagemaker DME pipeline: {args.target_pipeline_runid}",
                args.breakout_level,
            )
        else:
            s3 = s3fs.S3FileSystem(
                s3_additional_kwargs={
                    "ServerSideEncryption": "aws:kms",
                    "KMSKeyId": CONFIG["output_kms_key"],
                }
            )

            logger.info("compute_pvs_input:====START compute_pvs_input====")
            # Compute recipe outputs
            pvs_input_df = query_pvs_input(
                args.ap_data_sector,
                args.analysis_year,
                args.analysis_run_group,
                source_ids,
                args.breakout_level,
            )

            logger.info(f"compute_pvs_input:result_count:{pvs_input_df.count()}")
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
                args.ap_data_sector,
                args.analysis_year,
                args.analysis_run_group,
                source_ids,
            )
            logger.info("compute_trial_checks:checks_df dataset")
            checks_df = create_check_df(args.analysis_run_group, checks_df)

            logger.info(f"compute_trial_checks:result_count:{checks_df.count()}")
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
                args.ap_data_sector,
                args.analysis_year,
                args.analysis_run_group,
                source_ids,
                args.breakout_level,
                "numeric",
            )
            logger.info(
                f"compute_trial_numeric_input:result_count:{trial_numeric_input_df.count()}"
            )

            if trial_numeric_input_df.shape[0] > 0:
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
                args.ap_data_sector,
                args.analysis_year,
                args.analysis_run_group,
                source_ids,
                args.breakout_level,
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
            args.ap_data_sector,
            args.analysis_year,
            args.target_pipeline_runid,
            str(e),
            args.analysis_run_group,
        )
        raise e


if __name__ == "__main__":
    main()
