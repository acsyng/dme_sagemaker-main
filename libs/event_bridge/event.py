import json
import os

import boto3

from libs.config.config_vars import CONFIG, CLOUD_WATCH_GROUP


def create_event(
    event_bus,
    ap_data_sector,
    analysis_year,
    target_pipeline_runid,
    analysis_run_group,
    stages=None,
    status=None,
    message=None,
    breakout_level=None,
):

    kw_group = os.getenv("CLOUD_WATCH_GROUP").replace("/", "$252F")
    detail = {
        "pipeline-runid": target_pipeline_runid,
        "ap_data_sector": ap_data_sector,
        "trigger": "ap_sagemaker_pipeline",
        "phase": "3",
        "stages": stages,
        "analysis_year": analysis_year,
        "analysis_type": "late",
        "analysis_run_group": analysis_run_group,
        "breakout_level": breakout_level,
        "status": status,
        "msg": message,
        "log": (
            f"https://console.aws.amazon.com/cloudwatch/home?"
            f"#logsV2:log-groups/log-group/{kw_group}/log-events/{target_pipeline_runid}"
        ),
    }
    eb = boto3.client("events", region_name="us-east-1", verify=False)
    eb.put_events(
        Entries=[
            {
                "Source": "syngenta.ap.events",
                "DetailType": "A&P Workstream Events",
                "EventBusName": event_bus,
                "Detail": json.dumps(detail),
            }
        ]
    )


def error_event(
    ap_data_sector, analysis_year, pipeline_runid, message, analysis_run_group=None
):
    create_event(
        CONFIG.get("event_bus"),
        ap_data_sector,
        analysis_year,
        pipeline_runid,
        analysis_run_group,
        None,
        "ERROR",
        message,
    )
