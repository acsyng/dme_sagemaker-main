from sagemaker.workflow.pipeline import Pipeline

from libs.pipeline.base_pipeline import BasePipeline


class ErmModelPipeline(BasePipeline):

    def compute_erm_first(self):
        self.erm_first_step = self.create_python_processing_step('erm_first')

    def compute_pvs_for_erm2(self):
        self.pvs_for_erm2_step = self.create_spark_processing_step('pvs_for_erm2')

    def compute_pvs_for_erm_prepared(self):
        self.pvs_for_erm_prepared_step = self.create_spark_processing_step('pvs_for_erm_prepared')

    def compute_erm_train(self):
        self.erm_train_step = self.create_spark_processing_step('erm_train')

    def compute_train_predict_rmtn(self):
        self.train_predict_rmtn_step = self.create_spark_processing_step('train_predict_rmtn')

    def compute_trial_checks(self):
        self.trial_checks_step = self.create_python_processing_step('trial_checks')

    def create_pipeline(self):
        self.create_sagemaker_session()
        self.set_parameters()
        self.init_script()
        self.compute_erm_first()
        self.compute_trial_checks()
        self.compute_pvs_for_erm2()
        self.compute_pvs_for_erm_prepared()
        self.compute_erm_train()
        self.pipeline_preparation()

    def pipeline_preparation(self):
        self.erm_first_step.add_depends_on([self.init_script_step])
        self.trial_checks_step.add_depends_on([self.erm_first_step])
        self.pvs_for_erm2_step.add_depends_on([self.trial_checks_step])
        self.pvs_for_erm_prepared_step.add_depends_on([self.pvs_for_erm2_step])
        self.erm_train_step.add_depends_on([self.pvs_for_erm_prepared_step])
        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[self.param_ap_data_sector,
                        self.param_analysis_year,
                        self.param_analysis_type,
                        self.param_target_pipeline_runid,
                        self.param_analysis_run_group,
                        self.param_stages,
                        self.param_breakout_level,
                        self.param_force_refresh],
            steps=[self.init_script_step, self.erm_first_step, self.trial_checks_step, self.pvs_for_erm2_step,
                   self.pvs_for_erm_prepared_step, self.erm_train_step],
            sagemaker_session=self.sagemaker_session
        )

        self.pipeline.upsert(role_arn=self.role, tags=self.tags)


if __name__ == '__main__':
    ep = ErmModelPipeline()
    ep.create_pipeline()
