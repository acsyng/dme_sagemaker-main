from sagemaker.workflow.pipeline import Pipeline

from libs.config.config_vars import ENVIRONMENT
from libs.pipeline.base_pipeline import BasePipeline


class ErmModelPipeline(BasePipeline):
    def compute_train_model(self):
        self.train_model_step = self.create_spark_processing_step('train_model')

    def create_pipeline(self):
        self.init_script_step = True
        self.create_sagemaker_session()
        self.set_parameters()
        self.compute_train_model()
        self.pipeline_preparation()

    def pipeline_preparation(self):
        self.pipeline = Pipeline(
            name=f'ap-{ENVIRONMENT}-erm-train',
            parameters=[self.param_ap_data_sector,
                        self.param_analysis_year,
                        self.param_analysis_type,
                        self.param_target_pipeline_runid,
                        self.param_analysis_run_group,
                        self.param_stages,
                        self.param_breakout_level,
                        self.param_force_refresh],
            steps=[self.train_model_step],
            sagemaker_session=self.sagemaker_session
        )

        self.pipeline.upsert(role_arn=self.role, tags=self.tags)


if __name__ == '__main__':
    ep = ErmModelPipeline()
    ep.create_pipeline()
