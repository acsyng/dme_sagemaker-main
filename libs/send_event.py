import argparse

from libs.event_bridge.event import create_event
from libs.logger.cloudwatch_logger import CloudWatchLogger


def main():
    parser = argparse.ArgumentParser(description='app inputs and outputs')
    parser.add_argument('--event_bus', type=str, required=True)
    parser.add_argument('--ap_data_sector', type=str, required=True)
    parser.add_argument('--analysis_year', type=str, required=True)
    parser.add_argument('--target_pipeline_runid', type=str, required=True)
    parser.add_argument('--analysis_run_group', type=str, required=True)
    parser.add_argument('--status', type=str, required=True)
    parser.add_argument('--message', type=str, required=True)
    parser.add_argument('--stages', type=str, required=True)
    parser.add_argument('--breakout_level', type=str, required=True)
    parser.add_argument('--analysis_type', type=str, required=False)
    args = parser.parse_args()
    if not args.analysis_type or args.analysis_type != 'SingleExp':
        CloudWatchLogger.get_logger(args.target_pipeline_runid, args.analysis_type).info(
            f'Send event to {args.event_bus}: {args.message}')
        create_event(args.event_bus, args.ap_data_sector, args.analysis_year, args.target_pipeline_runid,
                     args.analysis_run_group, args.stages.split(','), args.status, args.message, args.breakout_level)


if __name__ == '__main__':
    main()
