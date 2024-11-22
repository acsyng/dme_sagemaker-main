from unittest.mock import patch, MagicMock

from libs.send_event import main


@patch('libs.send_event.create_event')
@patch('libs.send_event.CloudWatchLogger')
@patch('libs.send_event.argparse.ArgumentParser')
def test_main(mock_ap, mock_cwl, mock_ce):
    args = MagicMock()
    args.analysis_type = 'MultiExp'
    args.event_bus = 'test_eb'
    args.ap_data_sector = 'CORN_NA_SUMMER'
    args.analysis_year = 2024
    args.target_pipeline_runid = '20240101_00_00_00'
    args.analysis_run_group = 'phenohybrid1yr'
    args.stages = '5,6'
    args.status = 'START'
    args.message = 'test'
    args.breakout_level = 'wce'
    mock_ap.return_value.parse_args.return_value = args
    main()
    mock_ce.assert_called_with('test_eb', 'CORN_NA_SUMMER', 2024, '20240101_00_00_00', 'phenohybrid1yr', ['5', '6'],
                               'START', 'test', 'wce')
