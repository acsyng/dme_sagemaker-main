import argparse
import json
import boto3


def portfolio_create_event(event_bus,
                             ap_data_sector,
                             target_pipeline_runid,
                             status,
                             message):
    detail = {
        'pipeline-runid': target_pipeline_runid,
        'ap_data_sector': ap_data_sector,
        'trigger': 'ap_sagemaker_pipeline',
        'phase': '3',
        'status': status,
        'msg': message
    }
    eb = boto3.client('events', region_name='us-east-1', verify=False)
    eb.put_events(
        Entries=[
            {
                'Source': 'syngenta.ap.events',
                'DetailType': 'A&P Workstream Events',
                'EventBusName': event_bus,
                'Detail': json.dumps(detail)
            }
        ]
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--event_bus', type=str, required=True)
    parser.add_argument('--ap_data_sector', type=str, required=True)
    parser.add_argument('--target_pipeline_runid', type=str, required=True)
    parser.add_argument('--status', type=str, required=True)
    parser.add_argument('--message', type=str, required=True)

    args = parser.parse_args()
    portfolio_create_event(
        args.event_bus,
        args.ap_data_sector,
        args.target_pipeline_runid,
        args.status,
        args.message
    )
