import argparse
import boto3

from libs.config.config_vars import CONFIG

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Input parameters')
    parser.add_argument('--ap_data_sector', help='Input String ', required=True)
    parser.add_argument('--analysis_type', help='Input String', required=True)
    parser.add_argument('--forward_model_year', help='Input String', required=True)
    parser.add_argument('--material_type', help='Input String', required=True)
    parser.add_argument('--target_pipeline_runid', help='Input String', required=True)
    parser.add_argument('--force_refresh', help='Input String', required=True)
    parser.add_argument('--is_inferring', help='Input String', required=True)
    args = parser.parse_args()

    if args.is_inferring == '1':
        sagemaker_client = boto3.client('sagemaker', region_name='us-east-1')
        pipeline_name = CONFIG['performance_pipeline_arn']
        forward_model_year = args.forward_model_year
        analysis_type = args.analysis_type
        ap_data_sector = args.ap_data_sector
        execution_name = f'{ap_data_sector}-{forward_model_year}-{analysis_type}'.replace('_', '-')
        pipeline_parameters = {
            'ap_data_sector': ap_data_sector,
            'forward_model_year': forward_model_year,
            'material_type': args.material_type,
            'target_pipeline_runid': args.target_pipeline_runid,
            'force_refresh': 'True'
        }
        response = sagemaker_client.start_pipeline_execution(
            PipelineExecutionDescription=f'Sagemaker Performance pipeline',
            PipelineExecutionDisplayName=execution_name,
            PipelineName=pipeline_name,
            PipelineParameters=[{'Name': k, 'Value': v} for k, v in pipeline_parameters.items()])
