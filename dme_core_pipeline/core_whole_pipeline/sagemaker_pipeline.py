from sagemaker.processing import ProcessingOutput, ProcessingInput
from sagemaker.workflow import ParameterString
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionEquals
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.properties import PropertyFile

from libs.config.config_vars import ENVIRONMENT
from libs.pipeline.base_pipeline import BasePipeline


class SagemakerPipeline(BasePipeline):

    def init_script(self):
        self.query_variables = PropertyFile(
            name="query_variables",
            output_name="output",
            path="query_variables.json",
        )
        self.init_script_step = self.create_python_processing_step(
            name="init_script",
            job_inputs=[],
            job_outputs=[
                ProcessingOutput(
                    output_name="output",
                    source=self.output_source,
                    destination=self.get_destination(),
                )
            ],
            job_arguments=[
                "--ap_data_sector",
                self.param_ap_data_sector,
                "--analysis_year",
                self.param_analysis_year,
                "--target_pipeline_runid",
                self.param_target_pipeline_runid,
                "--force_refresh",
                self.param_force_refresh,
                "--breakout_level",
                self.param_breakout_level,
                "--analysis_run_group",
                self.param_analysis_run_group,
                "--s3_output_pvs_input",
                self.get_destination("pvs_input.parquet"),
                "--s3_output_trial_checks",
                self.get_destination("trial_checks.parquet"),
                "--s3_output_trial_numeric_input",
                self.get_destination("trial_numeric_input.parquet"),
                "--s3_output_trial_text_input",
                self.get_destination("trial_text_input.csv"),
            ],
            property_files=[self.query_variables],
        )

    def compute_metric_output2(self):
        s3_input_trial_numeric_output = self.get_destination("trial_numeric_output")
        s3_input_trial_text_output = self.get_destination("trial_text_output")
        s3_input_pvs_output = self.get_destination("pvs_output")
        s3_output_old_metric_output = self.get_destination("old_metric_output", False)
        s3_output_metric_output = self.get_destination("metric_output", False)
        s3_output_h2h_metric_output = self.get_destination("h2h_metric_output", False)
        s3_output_rating_metric_output = self.get_destination(
            "rating_metric_output", False
        )
        s3_dme_output_agg = self.get_dme_output("ap_dme_output_agg")
        s3_dme_output_h2h = self.get_dme_output("ap_dme_output_h2h")

        self.metric_output2_step = self.create_python_processing_step(
            name="metric_output2",
            job_arguments=[
                "--s3_input_trial_numeric_output",
                s3_input_trial_numeric_output,
                "--s3_input_trial_text_output",
                s3_input_trial_text_output,
                "--s3_input_pvs_output",
                s3_input_pvs_output,
                "--s3_output_old_metric_output",
                s3_output_old_metric_output,
                "--s3_output_metric_output",
                s3_output_metric_output,
                "--s3_output_h2h_metric_output",
                s3_output_h2h_metric_output,
                "--s3_output_rating_metric_output",
                s3_output_rating_metric_output,
                "--s3_dme_output_agg",
                s3_dme_output_agg,
                "--s3_dme_output_h2h",
                s3_dme_output_h2h,
            ],
        )

    def compute_second(self):
        s3_input_trial_checks = self.get_destination("trial_checks.parquet")
        s3_input_metric_config = self.get_static_data("metric_config.csv")
        s3_input_regression_cfg = self.get_static_data("regression_cfg.csv")
        s3_output_pvs_output = self.get_destination("pvs_output", True)
        s3_input_trial_text_input = self.get_destination("trial_text_input.csv")
        s3_output_trial_text_output = self.get_destination("trial_text_output", True)
        s3_input_trial_numeric_input = self.get_destination(
            "trial_numeric_input.parquet"
        )
        s3_output_trial_numeric_output = self.get_destination(
            "trial_numeric_output", True
        )
        self.second_step = self.create_spark_processing_step(
            "second",
            [
                "--s3_input_pvs_input",
                self.get_destination("pvs_input.parquet"),
                "--s3_input_trial_checks",
                s3_input_trial_checks,
                "--s3_input_metric_config",
                s3_input_metric_config,
                "--s3_input_regression_cfg",
                s3_input_regression_cfg,
                "--s3_output_pvs_output",
                s3_output_pvs_output,
                "--s3_input_trial_text_input",
                s3_input_trial_text_input,
                "--s3_output_trial_text_output",
                s3_output_trial_text_output,
                "--s3_input_trial_numeric_input",
                s3_input_trial_numeric_input,
                "--s3_output_trial_numeric_output",
                s3_output_trial_numeric_output,
            ],
        )

    def compute_erm_predict(self):
        self.erm_predict_step = self.create_python_processing_step("erm_predict")

    def set_parameters(self):
        super().set_parameters()
        self.param_dme_erm_reg = ParameterString(
            name="dme_erm_reg", default_value="False"
        )

    def pipeline_preparation(self):
        # self.second_step.add_depends_on([self.first_step])
        self.metric_output2_step.add_depends_on([self.second_step])
        empty_dataset_condition = ConditionEquals(
            left=JsonGet(
                step_name=self.init_script_step.name,
                property_file=self.query_variables,
                json_path="source_ids",
            ),
            right="('')",
        )
        erm_condition = ConditionEquals(
            left=self.param_dme_erm_reg,
            right="True",
        )
        condition_step = ConditionStep(
            name=f"ap-{ENVIRONMENT}-condition_step",
            conditions=[empty_dataset_condition],
            if_steps=[],
            else_steps=[self.second_step],
        )
        erm_condition_step = ConditionStep(
            name=f"ap-{ENVIRONMENT}-erm-condition_step",
            conditions=[erm_condition],
            if_steps=[self.erm_predict_step],
            else_steps=[],
        )
        condition_step.add_depends_on([self.init_script_step])
        erm_condition_step.add_depends_on([self.init_script_step])
        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[
                self.param_ap_data_sector,
                self.param_analysis_year,
                self.param_target_pipeline_runid,
                self.param_analysis_run_group,
                # self.param_stages,
                self.param_breakout_level,
                self.param_force_refresh,
                self.param_dme_erm_reg,
            ],
            steps=[
                self.init_script_step,
                condition_step,
                erm_condition_step,
                self.metric_output2_step,
            ],
            sagemaker_session=self.sagemaker_session,
        )

        self.pipeline.upsert(role_arn=self.role, tags=self.tags)

    def create_pipeline(self):
        self.create_sagemaker_session()
        self.set_parameters()
        self.init_script()
        self.compute_second()
        self.compute_metric_output2()
        self.compute_erm_predict()
        # self.compute_dme_output_metrics()
        self.pipeline_preparation()


if __name__ == "__main__":
    sp = SagemakerPipeline()
    sp.create_pipeline()
