from unittest.mock import patch

from libs.pipeline.base_pipeline import BasePipeline


@patch('libs.pipeline.base_pipeline.ProcessingStep')
@patch('libs.pipeline.base_pipeline.S3Uploader')
@patch('libs.pipeline.base_pipeline.PySparkProcessor')
@patch('libs.pipeline.base_pipeline.ScriptProcessor')
@patch('libs.pipeline.base_pipeline.PipelineSession')
def test_base_pipeline(mock_ps, mock_sp, mock_pp, mock_s3, mock_processing_step):
    bp = BasePipeline()
    bp.create_sagemaker_session()
    bp.set_parameters()
    bp.init_script()
    spark_step = bp.create_spark_processing_step('test')
