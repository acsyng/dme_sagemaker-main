import json
from time import sleep

import boto3
import pandas as pd
import s3fs
import sagemaker
from sagemaker.deserializers import CSVDeserializer
from sagemaker.predictor import Predictor
from sagemaker.serializers import CSVSerializer

from libs.config.config_vars import CONFIG, S3_DATA_PREFIX, S3_BUCKET, ENVIRONMENT
from libs.logger.cloudwatch_logger import CloudWatchLogger

CHUNK_SIZE = 100000


class ModelTrain:
    def __init__(self, train_file, test_file, target, output):
        self.boto3_session = boto3.Session(region_name='us-east-1')
        self.session = sagemaker.Session(self.boto3_session)
        self.bucket = CONFIG.get('bucket')
        self.role = CONFIG.get('role')
        self.train_file = train_file
        self.test_file = test_file
        self.output_kms_key = CONFIG.get('output_kms_key')
        self.target = target
        self.tags = CONFIG['tags']
        self.extra_args = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': self.output_kms_key}
        self.instance_type = CONFIG['instance_type']
        self.output = output
        self.sm = self.boto3_session.client(service_name='sagemaker', region_name=self.boto3_session.region_name)
        self.logger = CloudWatchLogger.get_logger()
        with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
            data = json.load(f)
            ap_data_sector = data['ap_data_sector'].replace('_', '-').lower()
            self.analysis_type = data['analysis_type']
            pipeline_runid = data['target_pipeline_runid']
            ml_prefix = f'ap-{ap_data_sector}-{self.target}-{ENVIRONMENT}'
            self.auto_ml_job_name = f'ap-{pipeline_runid.replace("_", "")}-job'
            self.model_name = f'{ml_prefix}-model'
            self.epc_name = f'{ml_prefix}-epc'
            self.ep_name = f'{ml_prefix}-ep'
            self.prefix = f'{S3_DATA_PREFIX}/{pipeline_runid}/{self.analysis_type}'
            self.data_path = f's3://{S3_BUCKET}/{self.prefix}/'

    def create_auto_ml_job(self):
        self.logger.info(f'create_auto_ml_job {self.auto_ml_job_name}')
        csv_name = 'train.csv'
        with open(csv_name, 'a') as f:
            for analysis_type in ('SingleExp', 'MultiExp', 'MynoET'):
                file_name = f'{self.data_path}{self.train_file}/*.parquet'.replace(self.analysis_type, analysis_type)
                train_df = self.read_parquet(file_name)
                self.logger.info(f'Write to csv file {csv_name}')
                train_df.to_csv(f, index=False, header=f.tell() == 0)
        self.upload_file(csv_name)

        input_data_config = [
            {
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': f'{self.data_path}{csv_name}',
                    }
                },
                'TargetAttributeName': self.target,
            }
        ]
        output_data_config = {
            'KmsKeyId': self.output_kms_key,
            'S3OutputPath': f'{self.data_path}model'
        }
        
        try:
            describe_response = self.sm.describe_auto_ml_job(AutoMLJobName=self.auto_ml_job_name)
            self.logger.info(f'Found existing ML job {self.auto_ml_job_name}. Update with new train file.')
        except self.sm.exceptions.ResourceNotFound:
            self.logger.info(f'ML job {self.auto_ml_job_name} not found. Try to create.')
            self.sm.create_auto_ml_job(
                AutoMLJobName=self.auto_ml_job_name,
                InputDataConfig=input_data_config,
                OutputDataConfig=output_data_config,
                AutoMLJobConfig={'CompletionCriteria': {'MaxCandidates': 5}},
                RoleArn=self.role,
                Tags=self.tags
            )
            describe_response = self.sm.describe_auto_ml_job(AutoMLJobName=self.auto_ml_job_name)

        self.logger.info(describe_response['AutoMLJobStatus'] + ' - ' + describe_response['AutoMLJobSecondaryStatus'])
        job_run_status = describe_response['AutoMLJobStatus']

        while job_run_status not in ('Failed', 'Completed', 'Stopped'):
            describe_response = self.sm.describe_auto_ml_job(AutoMLJobName=self.auto_ml_job_name)
            job_run_status = describe_response['AutoMLJobStatus']

            self.logger.info(
                describe_response['AutoMLJobStatus'] + ' - ' + describe_response['AutoMLJobSecondaryStatus']
            )
            sleep(30)

        self.logger.info(describe_response['AutoMLJobArtifacts']['CandidateDefinitionNotebookLocation'])
        self.logger.info(describe_response['AutoMLJobArtifacts']['DataExplorationNotebookLocation'])
        self.candidate_nbk = describe_response['AutoMLJobArtifacts']['CandidateDefinitionNotebookLocation']
        self.data_explore_nbk = describe_response['AutoMLJobArtifacts']['DataExplorationNotebookLocation']

    @staticmethod
    def split_s3_path(s3_path):
        path_parts = s3_path.replace('s3://', '').split('/')
        bucket = path_parts.pop(0)
        key = '/'.join(path_parts)
        return bucket, key
    
    def train_model(self):
        self.logger.info(f'train_model {self.model_name}')
        s3_bucket, candidate_nbk_key = self.split_s3_path(self.candidate_nbk)
        _, data_explore_nbk_key = self.split_s3_path(self.data_explore_nbk)

        self.session.download_data(path='./', bucket=s3_bucket, key_prefix=candidate_nbk_key)

        self.session.download_data(path='./', bucket=s3_bucket, key_prefix=data_explore_nbk_key)

        best_candidate = self.sm.describe_auto_ml_job(AutoMLJobName=self.auto_ml_job_name)['BestCandidate']
        best_candidate_name = best_candidate['CandidateName']
        self.logger.info(best_candidate)
        self.logger.info('CandidateName: ' + best_candidate_name)
        self.logger.info(
            'FinalAutoMLJobObjectiveMetricName: '
            + best_candidate['FinalAutoMLJobObjectiveMetric']['MetricName']
        )
        self.logger.info(
            'FinalAutoMLJobObjectiveMetricValue: '
            + str(best_candidate['FinalAutoMLJobObjectiveMetric']['Value'])
        )

        sm_dict = self.sm.list_candidates_for_auto_ml_job(AutoMLJobName=self.auto_ml_job_name)

        for item in sm_dict['Candidates']:
            self.logger.info(item['CandidateName'], item['FinalAutoMLJobObjectiveMetric'])
            self.logger.info(item['InferenceContainers'][1]['Image'])

        model_arn = self.sm.create_model(
            Containers=best_candidate['InferenceContainers'], ModelName=self.model_name, ExecutionRoleArn=self.role
        )

        self.logger.info(f'model_arn: {model_arn}')

        self.sm.create_endpoint_config(
            EndpointConfigName=self.epc_name,
            ProductionVariants=[
                {
                    'InstanceType': self.instance_type,
                    'InitialInstanceCount': 1,
                    'ModelName': self.model_name,
                    'VariantName': 'main',
                }
            ],
        )

        create_endpoint_response = self.sm.create_endpoint(EndpointName=self.ep_name, EndpointConfigName=self.epc_name)
        self.logger.info(f'create_endpoint_response: {create_endpoint_response}')
        self.sm.get_waiter('endpoint_in_service').wait(EndpointName=self.ep_name)

    def predict(self):
        self.logger.info('predict')
        predictor = Predictor(
            endpoint_name=self.ep_name,
            sagemaker_session=self.session,
            serializer=CSVSerializer(),
            deserializer=CSVDeserializer(),
        )

        test_data = self.read_parquet(f'{self.data_path}{self.test_file}/*.parquet')
        out_df = None
        for i in range(0, int(len(test_data) / CHUNK_SIZE) + 1):
            try:
                self.logger.info(f'Prediction iteration #{i}')
                offset = i * CHUNK_SIZE
                data = test_data[offset:offset + CHUNK_SIZE]
                self.logger.info("predictor.predict start")
                prediction = predictor.predict(data.copy().drop([self.target], axis=1).to_csv(
                    sep=',', header=False, index=False))
                self.logger.info("predictor.predict end")
                prediction_df = data.copy().drop(['prediction'], axis=1)
                prediction_df['prediction'] = pd.DataFrame(prediction)
                if out_df is None:
                    out_df = prediction_df
                else:
                    out_df = pd.concat([out_df, prediction_df], ignore_index=True, sort=False)
            except Exception as e:
                self.logger.error(e)
                raise e

        if out_df is not None:
            self.logger.info('Save prediction to S3')
            out_df.to_csv(self.output, index=False)
            self.upload_file(self.output)

    def clear(self):
        self.logger.info(f'Clear endpoint {self.ep_name}')
        try:
            self.sm.delete_endpoint(EndpointName=self.ep_name)
            self.sm.delete_endpoint_config(EndpointConfigName=self.epc_name)
            self.sm.delete_model(ModelName=self.model_name)
        except Exception as e:
            self.logger.error(e)

    def process(self):
        self.logger.info(f'Process {self.ep_name}')
        response = self.sm.list_endpoints(NameContains=self.ep_name)
        if 'Endpoints' in response and len(response['Endpoints']) > 0:
            endpoint_arn = response['Endpoints'][0]['EndpointArn']
            self.logger.info(f'Found endpoint {endpoint_arn}. Skip model train.')
            self.clear()
        self.create_auto_ml_job()
        self.train_model()

    def read_parquet(self, parquet_path):
        self.logger.info(f'read_parquet {parquet_path}')
        s3 = s3fs.S3FileSystem()
        parquet_files = s3.glob(parquet_path)
        dfs = []
        for file in parquet_files:
            self.logger.info(f'read {file}')
            with s3.open(file, 'rb') as f:
                dfs.append(pd.read_parquet(f))
        df = pd.concat(dfs, ignore_index=True)
        return df

    def upload_file(self, file_name):
        self.logger.info(f'Upload file: {file_name}')
        self.session.upload_data(path=file_name, bucket=self.bucket, key_prefix=self.prefix,
                                 extra_args=self.extra_args)