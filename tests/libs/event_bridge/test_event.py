import os

from unittest.mock import patch

from libs.event_bridge.event import create_event, error_event


@patch('libs.event_bridge.event.boto3.client')
def test_create_event(boto3_mock):
    os.environ['CLOUD_WATCH_GROUP'] = '/ap/sagemaker/test'
    create_event('test', 'CORN_NA_SUMMER', 2024, '20240101_00_00_00', 'phenohybrid1yr', ['5', '6'], 'TEST', 'Test')
    detail = ('{"pipeline-runid": "20240101_00_00_00", "ap_data_sector": "CORN_NA_SUMMER", '
              '"trigger": "ap_sagemaker_pipeline", "phase": "3", "stages": ["5", "6"], "analysis_year": 2024, '
              '"analysis_type": "late", "analysis_run_group": "phenohybrid1yr", "breakout_level": null, '
              '"status": "TEST", "msg": "Test", '
              '"log": "https://console.aws.amazon.com/cloudwatch/home?#logsV2:log-groups/log-group/'
              '$252Fap$252Fsagemaker$252Ftest/log-events/20240101_00_00_00"}')
    boto3_mock.return_value.put_events.assert_called_with(Entries=[
        {
            'Source': 'syngenta.ap.events',
            'DetailType': 'A&P Workstream Events',
            'EventBusName': 'test',
            'Detail': detail}])


@patch('libs.event_bridge.event.boto3.client')
def test_error_event(boto3_mock):
    os.environ['CLOUD_WATCH_GROUP'] = '/ap/sagemaker/test'
    error_event('CORN_NA_SUMMER', 2024, '20240101_00_00_00', 'Error')
    detail = ('{"pipeline-runid": "20240101_00_00_00", "ap_data_sector": "CORN_NA_SUMMER", '
              '"trigger": "ap_sagemaker_pipeline", "phase": "3", "stages": null, "analysis_year": 2024, '
              '"analysis_type": "late", "analysis_run_group": null, "breakout_level": null, "status": "ERROR", '
              '"msg": "Error", "log": "https://console.aws.amazon.com/cloudwatch/home?#logsV2:log-groups/log-group'
              '/$252Fap$252Fsagemaker$252Ftest/log-events/20240101_00_00_00"}')
    boto3_mock.return_value.put_events.assert_called_with(Entries=[
        {
            'Source': 'syngenta.ap.events',
            'DetailType': 'A&P Workstream Events',
            'EventBusName': 'ap_single_eventbus_dev',
            'Detail': detail}])