from unittest.mock import patch, mock_open

from libs.model.train import ModelTrain


@patch('libs.model.train.pd')
@patch('libs.model.train.s3fs.S3FileSystem')
@patch('libs.model.train.boto3.Session')
@patch('libs.model.train.sagemaker.Session')
@patch('libs.model.train.CloudWatchLogger')
@patch('libs.model.train.json.load')
@patch('builtins.open', new_callable=mock_open)
def test_process(mock_file, mock_json_load, mock_logger, mock_sagemaker_session, mock_boto3_session, mock_s3fs,
                 mock_pd):
    mock_json_load.return_value = {
        'ap_data_sector': 'CORN_NA_SUMMER',
        'analysis_year': '2024',
        'analysis_type': 'SingleExp',
        'source_ids': '12345',
        'target_pipeline_runid': '20240101_00_00_00',
        'breakout_level': 'wce',
    }
    mock_boto3_session.return_value.client.return_value.describe_auto_ml_job.return_value = {
        'AutoMLJobStatus': 'Completed',
        'AutoMLJobSecondaryStatus': 'Completed',
        'AutoMLJobArtifacts': {
            'CandidateDefinitionNotebookLocation': 'test',
            'DataExplorationNotebookLocation': 'test'
        },
        'BestCandidate': {
            'CandidateName': 'test',
            'FinalAutoMLJobObjectiveMetric': {
                'MetricName': 'test',
                'Value': 'test'
            },
            'InferenceContainers': [

            ]
        }
    }
    mt = ModelTrain('train.csv', 'test.csv', 'test_target', 'test_output')
    mock_file.assert_called_once_with('/opt/ml/processing/input/input_variables/query_variables.json', 'r')
    mt.process()
    mt.predict()