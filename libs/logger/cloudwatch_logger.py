import json
import logging
import os

import watchtower
import boto3

QUERY_VARIABLES = "/opt/ml/processing/input/input_variables/query_variables.json"


class CloudWatchLogger:
    _logger = None

    @classmethod
    def get_logger(cls, stream_name=None, logger_name="root"):
        if cls._logger is None:
            cls._setup_logger(stream_name, logger_name)
        return cls._logger

    @classmethod
    def _setup_logger(cls, stream_name=None, logger_name="root"):
        boto3.setup_default_session(region_name="us-east-1")
        if os.path.exists(QUERY_VARIABLES):
            with open(QUERY_VARIABLES, "r") as f:
                data = json.load(f)
                cls._logger = logging.getLogger(data["analysis_run_group"])
                stream_name = data["target_pipeline_runid"]
        elif stream_name:
            cls._logger = logging.getLogger(logger_name)
        else:
            raise ValueError("Stream name is empty for CloudWatch logger")

        handler = watchtower.CloudWatchLogHandler(
            log_group=os.getenv("CLOUD_WATCH_GROUP"), stream_name=stream_name
        )
        handler.setFormatter(
            fmt=logging.Formatter(
                "%(name)s - %(levelname)s - %(funcName)s - %(filename)s:%(lineno)d - %(message)s"
            )
        )
        cls._logger.addHandler(handler)
        cls._logger.setLevel(logging.INFO)
