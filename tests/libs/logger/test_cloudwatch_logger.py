import os

from unittest.mock import patch

from libs.logger.cloudwatch_logger import CloudWatchLogger


@patch('libs.logger.cloudwatch_logger.logging.getLogger')
@patch('libs.logger.cloudwatch_logger.watchtower.CloudWatchLogHandler')
@patch('libs.logger.cloudwatch_logger.boto3.setup_default_session')
def test_cloudwatch_logger(boto3_mock, cloudwatch_logging_mock, logging_mock):
    os.environ['ENVIRONMENT'] = 'test'
    logger = CloudWatchLogger.get_logger('test')
    logger.info('test')
    logging_mock.return_value.info.assert_called_with('test')
