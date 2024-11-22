import sys

from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput, NetworkConfig
from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterString, ParameterBoolean
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import CacheConfig
from sagemaker.workflow.steps import ProcessingStep

from libs.config.config_vars import CONFIG, ENVIRONMENT, DME_PROJECT, ENVIRONMENT_VARS

cache_config = CacheConfig(enable_caching=True, expire_after="30d")


class PlacementPipeline:
    def __init__(self):
        print("start init in Placement Pipeline!")
        self.bucket = CONFIG.get('bucket')
        self.pipeline_name = f'ap-{ENVIRONMENT}-{DME_PROJECT}'
        self.role = CONFIG.get('role')
        self.subnets = CONFIG.get('subnets')
        self.security_group_ids = CONFIG.get('security_group_ids')
        self.script_base_job_name = CONFIG.get('script_base_job_name')
        self.spark_base_job_name = CONFIG.get('spark_base_job_name')
        self.image_uri = CONFIG.get('image_uri')
        self.script_instance_count = CONFIG.get('script_instance_count')
        self.spark_instance_count = CONFIG.get('spark_instance_count')
        self.instance_type = CONFIG.get('instance_type')
        self.output_kms_key = CONFIG.get('output_kms_key')
        self.tags = [{'Key': 'Application', 'Value': 'Advancement placement'},
                     {'Key': 'Cost Center', 'Value': CONFIG.get('cost_center')}]
        self.input_libs = CONFIG.get('input_libs')
        self.input_variables = CONFIG.get('input_variables')
        self.output_source = CONFIG.get('output_source')
        self.output_destination = CONFIG.get('output_destination')
        self.spark_output = CONFIG.get('spark_output')
        self.output_destination_placement = CONFIG.get('output_destination_placement')
        self.spark_output_placement = CONFIG.get('spark_output_placement')
        self.extra_args = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': self.output_kms_key}
        self.event_bus = CONFIG.get('event_bus')
        self.init_script_step = None
        self.steps = []

    def make_python_processor(self, network_config, image_uri=None, instance_type=None, instance_count=None):
        """
        wrapper function to make a python processor given an image, instance type, and count
        network_config : Sagemaker NetworkConfig object provided our subnets and security_group_ids
        image_uri : uri to the image to load. Default (None) grabs uri from config_vars.py. Could also provide AWS uri,
            or a different custom build uri
        instance_type : string containing type of instance to use. 'ml.m5.2xlarge','m3.xlarge', 'm3.medium' for example
        instance_count : number of instances to use. Note: main() will run on each instance if not handled explicitly.
            This will create multiple saved files, multiple models, multiple calls to denodo, etc.
        """

        # get default image, instance type, and instance count from config file if nothing provided
        if image_uri is None:
            image_uri = self.image_uri
        if instance_type is None:
            instance_type = self.instance_type
        if instance_count is None:
            instance_count = self.script_instance_count

        python_processor = ScriptProcessor(
            command=['python3'],
            base_job_name=self.script_base_job_name,
            image_uri=image_uri,
            role=self.role,
            instance_count=instance_count,
            instance_type=instance_type,
            network_config=network_config,
            output_kms_key=self.output_kms_key,
            tags=self.tags,
            env=ENVIRONMENT_VARS
        )

        return python_processor

    def create_sagemaker_session(self):
        self.sagemaker_session = PipelineSession(default_bucket=self.bucket)

        self.postgres_driver = S3Uploader.upload(
            local_path='../../driver/postgresql-42.5.4.jar',
            desired_s3_uri=f'{self.output_destination}driver',
            kms_key=self.output_kms_key
        )

        self.libs_uri = S3Uploader.upload(
            local_path='../../libs',
            desired_s3_uri=f'{self.output_destination}libs',
            kms_key=self.output_kms_key
        )

        self.egg_uri = S3Uploader.upload(
            local_path=f'../../dist/sagemaker_dme_lib-1.0.0-py3.{sys.version_info.minor}.egg',
            desired_s3_uri=f'{self.output_destination}egg',
            kms_key=self.output_kms_key
        )

    def make_processors(self):
        network_config = NetworkConfig(enable_network_isolation=False,
                                       subnets=self.subnets,
                                       security_group_ids=self.security_group_ids)

        # make default processor based on config_vars
        self.python_processor_default = self.make_python_processor(
            network_config=network_config
        )

        self.python_processor_large = self.make_python_processor(
            network_config=network_config,
            instance_type='ml.m5.4xlarge',
            instance_count=1
        )

        self.spark_processor_default = PySparkProcessor(framework_version='3.2',
                                                        base_job_name=self.spark_base_job_name,
                                                        sagemaker_session=self.sagemaker_session,
                                                        role=self.role,
                                                        instance_count=self.spark_instance_count,
                                                        # instance_type=self.instance_type,
                                                        instance_type='ml.m5.4xlarge',
                                                        network_config=network_config,
                                                        output_kms_key=self.output_kms_key,
                                                        tags=self.tags,
                                                        image_uri=self.image_uri,
                                                        env=ENVIRONMENT_VARS)

        self.spark_processor_large = PySparkProcessor(framework_version='3.2',
                                                      base_job_name=self.spark_base_job_name,
                                                      sagemaker_session=self.sagemaker_session,
                                                      role=self.role,
                                                      instance_count=self.spark_instance_count,
                                                      # instance_type=self.instance_type,
                                                      instance_type='ml.m5.12xlarge',
                                                      volume_size_in_gb=60,
                                                      network_config=network_config,
                                                      output_kms_key=self.output_kms_key,
                                                      tags=self.tags,
                                                      image_uri=self.image_uri,
                                                      env=ENVIRONMENT_VARS)

    def set_parameters(self):
        self.param_ap_data_sector = ParameterString(name='ap_data_sector')
        self.param_analysis_year = ParameterString(name='analysis_year')
        self.param_target_pipeline_runid = ParameterString(name='target_pipeline_runid')
        self.param_historical_build = ParameterString(name='historical_build', default_value='False')

    def create_python_processing_step(self, name, job_inputs=None, job_outputs=None, job_arguments=None,
                                      python_processor=None):

        # set base_python_processor as default
        if python_processor is None:
            python_processor = self.python_processor_default

        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination_placement}compute_{name}/src',
            kms_key=self.output_kms_key
        )
        inputs = [
            # Mount the libs dir with your code here.
            ProcessingInput(
                input_name='libraries',
                source=self.libs_uri,
                destination=self.input_libs)]
        if self.init_script_step:
            inputs += [ProcessingInput(
                input_name='input_variables',
                source=self.init_script_step.properties.ProcessingOutputConfig.Outputs['output'].S3Output.S3Uri,
                destination=self.input_variables
            )]
        if job_inputs:
            inputs += job_inputs

        if job_outputs:
            outputs = job_outputs
        else:
            outputs = [
                # Mount the src dir with your code here.
                ProcessingOutput(
                    output_name='output',
                    source=self.output_source,
                    destination=f'{self.output_destination_placement}compute_{name}/data'),
            ]

        step = ProcessingStep(name=f'ap-{ENVIRONMENT}-compute_{name}_step',
                              processor=python_processor,
                              code=f'{input_src_uri}/processing.py',
                              inputs=inputs,
                              outputs=outputs,
                              job_arguments=job_arguments,
                              kms_key=self.output_kms_key)

        self.steps.append(step)
        return step

    def create_spark_processing_step(self, name, arguments=None, python_processor=None, spark_processor=None):
        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination_placement}compute_{name}/src',
            kms_key=self.output_kms_key
        )

        # set base_python_processor as default
        if python_processor is None:
            python_processor = self.python_processor_default

        if spark_processor is None:
            spark_processor = self.spark_processor_default

        inputs = [ProcessingInput(
            input_name='input_variables',
            source=
            self.init_script_step.properties.ProcessingOutputConfig.Outputs[
                'output'].S3Output.S3Uri,
            destination='/opt/ml/processing/input/input_variables')]
        step = ProcessingStep(
            name=f'ap-{ENVIRONMENT}-{name}_step',
            step_args=spark_processor.run(
                submit_app=f'{input_src_uri}/processing.py',
                submit_py_files=[self.egg_uri],
                submit_jars=[self.postgres_driver],
                arguments=arguments,
                inputs=inputs,
                kms_key=self.output_kms_key,
                spark_event_logs_s3_uri=f'{self.output_destination_placement}compute_{name}/spark_event_logs',
                logs=False)
        )
        self.steps.append(step)
        return step

    def init_script(self):
        self.init_script_step = self.create_python_processing_step(
            'init_script_placement',
            job_arguments=['--ap_data_sector', self.param_ap_data_sector,
                           '--analysis_year', self.param_analysis_year,
                           '--target_pipeline_runid', self.param_target_pipeline_runid,
                           '--historical_build', self.param_historical_build
                           ])

    def compute_pvs_input_step(self):
        self.pvs_input_step = self.create_python_processing_step('pvs_input')

    def compute_comparison_set(self):
        self.comparison_set_step = self.create_python_processing_step('comparison_set')

    def compute_giga_distance(self):
        self.giga_distance_step = self.create_python_processing_step('giga_distance')

    def compute_comparison_set_v1(self):
        self.comparison_set_v1_step = self.create_python_processing_step('comparison_set_v1',
                                                                         python_processor=self.python_processor_large)

    def compute_sample_comps_for_grm(self):
        self.sample_comps_for_grm_step = self.create_python_processing_step(
            'sample_comps_for_grm',
            [
                ProcessingInput(
                    input_name='sample_comp_set',
                    source=f'{self.output_destination_placement}compute_comparison_set/data/comparison_set',
                    destination='/opt/ml/processing/input/data')
            ])

    def compute_sample_geno_output(self):
        self.sample_geno_output_step = self.create_python_processing_step(
            name='sample_geno_output',
            job_inputs=[
                ProcessingInput(
                    input_name='variant_id_count',
                    source=f'{self.output_destination_placement}compute_sample_comps_for_grm/data/sample_comps_for_grm',
                    destination='/opt/ml/processing/input/data')
            ],
            python_processor=self.python_processor_large)

    def compute_sample_geno_output_giga(self):
        self.sample_geno_output_giga_step = self.create_python_processing_step(
            name='sample_geno_output_giga',
            job_inputs=[
                ProcessingInput(
                    input_name='variant_id_count',
                    source=f'{self.output_destination_placement}compute_sample_comps_for_grm/data/sample_comps_for_grm',
                    destination='/opt/ml/processing/input/data')
            ])

    def compute_hetpool1_all_years(self):
        self.hetpool1_all_years_step = self.create_python_processing_step('hetpool1_all_years')

    def compute_hetpool2_all_years(self):
        self.hetpool2_all_years_step = self.create_python_processing_step('hetpool2_all_years')

    def compute_soy_cropfact_grid_prep(self):
        self.soy_cropfact_grid_prep_step = self.create_python_processing_step('soy_cropfact_grid_prep')

    def compute_corn_cropfact_grid_prep(self):
        self.corn_cropfact_grid_prep_step = self.create_python_processing_step('corn_cropfact_grid_prep')

    def compute_pca_output_hetpool1(self):
        self.pca_output_hetpool1_step = self.create_python_processing_step(
            'pca_output_hetpool1',
            job_inputs=[
                ProcessingInput(
                    input_name='hetpool1_all_years',
                    source=f'{self.output_destination_placement}compute_hetpool1_all_years/data/hetpool1_all_years',
                    destination=f'{self.output_source}hetpool1_all_years/')
            ],
            job_arguments=[
                '--s3_input_hetpool1_folder', f'{self.output_source}hetpool1_all_years/'
            ])

    def compute_pca_output_hetpool2(self):
        self.pca_output_hetpool2_step = self.create_python_processing_step(
            'pca_output_hetpool2',
            [
                ProcessingInput(
                    input_name='hetpool2_all_years',
                    source=f'{self.output_destination_placement}compute_hetpool2_all_years/data/hetpool2_all_years',
                    destination=f'{self.output_source}hetpool2_all_years/')
            ],
            job_arguments=[
                '--s3_input_hetpool2_folder', f'{self.output_source}hetpool2_all_years/'
            ])

    def compute_trial_data_by_year(self):
        self.trial_data_by_year_step = self.create_python_processing_step('trial_data_by_year')

    def compute_trial_data_all_years(self):
        self.trial_data_all_years_step = self.create_python_processing_step('trial_data_all_years')

    def compute_cropFact_api_output_by_year(self):
        self.cropFact_api_output_by_year_step = self.create_python_processing_step('cropFact_api_output_by_year')

    def compute_trial_feature_keep_array(self):
        self.trial_feature_keep_array_step = self.create_python_processing_step('trial_feature_keep_array')

    def compute_materials_without_geno(self):
        self.materials_without_geno_step = self.create_python_processing_step(
            'materials_without_geno',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'),
                ProcessingInput(
                    input_name='trial_data_train',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data',
                    destination=f'{self.output_source}trial_data_train/'
                )
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/',
                '--s3_input_trial_data_train_folder', f'{self.output_source}trial_data_train/'
            ],
            python_processor=self.python_processor_large)

    def compute_trial_data_train(self):
        self.trial_data_train_step = self.create_python_processing_step(
            name='trial_data_train',
            job_inputs=[
                ProcessingInput(
                    input_name='cropFact_api_output_all_years',
                    source=f'{self.output_destination_placement}compute_trial_feature_keep_array/data/cropFact_api_output_all_years',
                    destination=f'{self.output_source}cropFact_api_output_all_years/'),
                ProcessingInput(
                    input_name='trial_data_all_years',
                    source=f'{self.output_destination_placement}compute_trial_data_all_years/data/trial_data_all_years',
                    destination=f'{self.output_source}trial_data_all_years/'),
                ProcessingInput(
                    input_name='trial_feature_keep_array',
                    source=f'{self.output_destination_placement}compute_trial_feature_keep_array/data/trial_feature_keep_array',
                    destination=f'{self.output_source}trial_feature_keep_array/'),
                ProcessingInput(
                    input_name='pca_output_hetpool1',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool1/data/pca_output_hetpool1',
                    destination=f'{self.output_source}pca_output_hetpool1/'
                ),
                ProcessingInput(
                    input_name='pca_output_hetpool2',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool2/data/pca_output_hetpool2',
                    destination=f'{self.output_source}pca_output_hetpool2/'
                )],
            job_arguments=[
                '--s3_input_pca_output_hetpool1_folder', f'{self.output_source}pca_output_hetpool1/',
                '--s3_input_pca_output_hetpool2_folder', f'{self.output_source}pca_output_hetpool2/',
                '--s3_input_trial_feature_keep_array_folder', f'{self.output_source}trial_feature_keep_array/',
                '--s3_input_trial_data_all_years_folder', f'{self.output_source}trial_data_all_years/',
                '--s3_input_cropFact_api_output_all_years_folder', f'{self.output_source}cropFact_api_output_all_years/'

            ])

    def compute_train_data_lgbm(self):
        self.train_data_lgbm_step = self.create_python_processing_step(
            'train_data_lgbm',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data/train_data',
                    destination=f'{self.output_source}train_data_input/'
                )
            ],
            job_outputs=[
                # Mount the src dir with your code here.
                ProcessingOutput(
                    output_name='pickle_model',
                    source=f'{self.output_source}train_data_lgbm/',
                    destination=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm')
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_input/'

            ],
            python_processor=self.python_processor_large)

    def compute_train_data_lgbm_1(self):
        self.train_data_lgbm_1_step = self.create_python_processing_step(
            'train_data_lgbm_1',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'
                ),
                ProcessingInput(
                    input_name='trial_data_train',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data',
                    destination=f'{self.output_source}trial_data_train/'
                )
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/',
                '--s3_input_trial_data_train_folder', f'{self.output_source}trial_data_train/'
            ],
            python_processor=self.python_processor_large)

    def compute_test_data_lgbm(self):
        self.test_data_lgbm_step = self.create_python_processing_step(
            'test_data_lgbm',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'
                ),
                ProcessingInput(
                    input_name='trial_data_train',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data',
                    destination=f'{self.output_source}trial_data_train/'
                )
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/',
                '--s3_input_trial_data_train_folder', f'{self.output_source}trial_data_train/'
            ])

    def compute_train_data_scored_agg(self):
        self.train_data_scored_agg_step = self.create_python_processing_step(
            'train_data_scored_agg',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm_1',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm_1/data/train_data_lgbm_1',
                    destination=f'{self.output_source}train_data_lgbm_1/'
                ),
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'
                )
            ],
            job_arguments=[
                '--s3_input_train_data_1_lgbm_folder', f'{self.output_source}train_data_lgbm_1/',
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/'
            ])

    def compute_test_data_scored_agg(self):
        self.test_data_scored_agg_step = self.create_python_processing_step(
            'test_data_scored_agg',
            job_inputs=[
                ProcessingInput(
                    input_name='test_data_lgbm',
                    source=f'{self.output_destination_placement}compute_test_data_lgbm/data/test_data_lgbm',
                    destination=f'{self.output_source}test_data_lgbm/'
                ),
                ProcessingInput(
                    input_name='train_data_lgbm_folder',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm_folder/'
                )
            ],
            job_arguments=[
                '--s3_input_test_data_lgbm_folder', f'{self.output_source}test_data_lgbm/',
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm_folder/'
            ])

    def compute_grid_classification(self):
        self.grid_classification_step = self.create_python_processing_step(
            name='grid_classification',
            job_inputs=[
                ProcessingInput(
                    input_name='Grid_10km_RM_Density_Final.csv',
                    source=f'{self.output_destination_placement}compute_grid_classification/data',
                    destination=f'{self.output_source}input_grid_classification/'),
                ProcessingInput(
                    input_name='trial_data_all_years',
                    source=f'{self.output_destination_placement}compute_trial_data_all_years/data/trial_data_all_years',
                    destination=f'{self.output_source}trial_data_all_years/')
            ],
            job_outputs=[
                # Mount the src dir with your code here.
                ProcessingOutput(
                    output_name='output_grid_classification',
                    source='/opt/ml/processing/data/grid_classification/',
                    destination=f'{self.output_destination_placement}compute_grid_classification/data/grid_classification')
            ],
            job_arguments=[
                '--s3_input_grid_10km_folder', f'{self.output_source}input_grid_classification/',
                '--s3_input_trial_data_all_years_folder', f'{self.output_source}trial_data_all_years/'
            ])

    def compute_cropFact_grid_for_model(self):
        self.cropFact_grid_for_model_step = self.create_python_processing_step(
            name='cropFact_grid_for_model',
            job_inputs=[
                ProcessingInput(
                    input_name='cropFact_api_output_all_years',
                    source=f'{self.output_destination_placement}compute_trial_feature_keep_array/data',
                    destination=f'{self.output_source}trial_feature_keep_array/'),
                ProcessingInput(
                    input_name='grid_classification',
                    source=f'{self.output_destination_placement}compute_grid_classification/data',
                    destination=f'{self.output_source}grid_classification/'),
                ProcessingInput(
                    input_name='soy_cropfact_grid',
                    source=f'{self.output_destination_placement}compute_soy_cropfact_grid_prep/data/soy_cropfact_grid_prep',
                    destination=f'{self.output_source}soy_cropfact_grid_prep/'),
                ProcessingInput(
                    input_name='corn_cropfact_grid',
                    source=f'{self.output_destination_placement}compute_corn_cropfact_grid_prep/data/corn_cropfact_grid_prep',
                    destination=f'{self.output_source}corn_cropfact_grid_prep/'),
                ProcessingInput(
                    input_name='trial_data_all_years',
                    source=f'{self.output_destination_placement}compute_trial_data_all_years/data/trial_data_all_years',
                    destination=f'{self.output_source}trial_data_all_years/')

            ],
            job_arguments=[
                '--s3_input_grid_classification_folder', f'{self.output_source}grid_classification/',
                '--s3_input_trial_feature_keep_array_folder', f'{self.output_source}trial_feature_keep_array/',
                '--s3_input_soy_cropfact_grid_folder', f'{self.output_source}soy_cropfact_grid_prep/',
                '--s3_input_corn_cropfact_grid_folder', f'{self.output_source}corn_cropfact_grid_prep/',
                '--s3_input_trial_data_all_years_folder', f'{self.output_source}trial_data_all_years/'
            ],
            python_processor=self.python_processor_large)

    def compute_shap_ui_output(self):
        self.shap_ui_output_step = self.create_python_processing_step(
            'shap_ui_output',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'),
                ProcessingInput(
                    input_name='grid_classification',
                    source=f'{self.output_destination_placement}compute_grid_classification/data/grid_classification',
                    destination=f'{self.output_source}grid_classification/'),
                ProcessingInput(
                    input_name='hetpool1_pca',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool1/data/pca_output_hetpool1',
                    destination=f'{self.output_source}pca_output_hetpool1/'),
                ProcessingInput(
                    input_name='hetpool2_pca',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool2/data/pca_output_hetpool2',
                    destination=f'{self.output_source}pca_output_hetpool2/'),
                ProcessingInput(
                    input_name='test_data_lgbm',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data/test_data',
                    destination=f'{self.output_source}test_data/')
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/',
                '--s3_input_grid_classification_folder', f'{self.output_source}grid_classification/',
                '--s3_input_hetpool1_pca_folder', f'{self.output_source}pca_output_hetpool1/',
                '--s3_input_hetpool2_pca_folder', f'{self.output_source}pca_output_hetpool2/',
                '--s3_input_test_data_folder', f'{self.output_source}test_data/'
            ],
            python_processor=self.python_processor_large)

    def compute_grid_performance(self):
        self.grid_performance_step = self.create_python_processing_step(
            'grid_performance',
            job_inputs=[
                ProcessingInput(
                    input_name='train_data_lgbm',
                    source=f'{self.output_destination_placement}compute_train_data_lgbm/data/train_data_lgbm',
                    destination=f'{self.output_source}train_data_lgbm/'),
                ProcessingInput(
                    input_name='grid_classification',
                    source=f'{self.output_destination_placement}compute_grid_classification/data/grid_classification',
                    destination=f'{self.output_source}grid_classification/'),
                ProcessingInput(
                    input_name='hetpool1_pca',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool1/data/pca_output_hetpool1',
                    destination=f'{self.output_source}pca_output_hetpool1/'),
                ProcessingInput(
                    input_name='hetpool2_pca',
                    source=f'{self.output_destination_placement}compute_pca_output_hetpool2/data/pca_output_hetpool2',
                    destination=f'{self.output_source}pca_output_hetpool2/'),
                ProcessingInput(
                    input_name='test_data_lgbm',
                    source=f'{self.output_destination_placement}compute_trial_data_train/data/test_data',
                    destination=f'{self.output_source}test_data/')
            ],
            job_arguments=[
                '--s3_input_train_data_lgbm_folder', f'{self.output_source}train_data_lgbm/',
                '--s3_input_grid_classification_folder', f'{self.output_source}grid_classification/',
                '--s3_input_hetpool1_pca_folder', f'{self.output_source}pca_output_hetpool1/',
                '--s3_input_hetpool2_pca_folder', f'{self.output_source}pca_output_hetpool2/',
                '--s3_input_test_data_folder', f'{self.output_source}test_data/'
            ],
            python_processor=self.python_processor_large)

    def compute_regional_aggregate_query(self):
        self.regional_aggregate_query_step = self.create_spark_processing_step(
            'regional_aggregate_query',
            arguments=[
                '--s3_input_grid_performance',
                f'{self.spark_output_placement}compute_grid_performance/data/grid_performance/',
                '--s3_input_grid_classification',
                f'{self.spark_output_placement}compute_grid_classification/data/grid_classification/',
                '--s3_output_regional_aggregate_query',
                f'{self.spark_output_placement}compute_regional_aggregate_query/data/regional_aggregate_query/'
            ]
        )

    def compute_regional_aggregate(self):
        self.regional_aggregate_step = self.create_python_processing_step(
            'regional_aggregate',
            job_inputs=[
                ProcessingInput(
                    input_name='regional_aggregate_query',
                    source=f'{self.output_destination_placement}compute_regional_aggregate_query/data/regional_aggregate_query',
                    destination=f'{self.output_source}regional_aggregate_query/')
            ],
            job_arguments=[
                '--s3_input_regional_aggregate_query_folder', f'{self.output_source}regional_aggregate_query/'
            ],
            python_processor=self.python_processor_large)

    def compute_temporal_aggregate_query(self):
        self.temporal_aggregate_query_step = self.create_spark_processing_step(
            'temporal_aggregate_query',
            python_processor=self.python_processor_large,
            spark_processor=self.spark_processor_large,
            arguments=[
                '--s3_input_grid_performance',
                f'{self.spark_output_placement}compute_grid_performance/data/grid_performance/',
                '--s3_input_grid_classification',
                f'{self.spark_output_placement}compute_grid_classification/data/grid_classification/',
                '--s3_output_temporal_aggregate_query',
                f'{self.spark_output_placement}compute_temporal_aggregate_query/data/temporal_aggregate_query/'
            ]
        )

    def compute_temporal_aggregate(self):
        self.temporal_aggregate_step = self.create_python_processing_step(
            'temporal_aggregate',
            job_inputs=[
                ProcessingInput(
                    input_name='temporal_aggregate_query',
                    source=f'{self.output_destination_placement}compute_temporal_aggregate_query/data/temporal_aggregate_query',
                    destination=f'{self.output_source}temporal_aggregate_query/')
            ],
            job_arguments=[
                '--s3_input_temporal_aggregate_query_folder', f'{self.output_source}temporal_aggregate_query/'
            ],
            python_processor=self.python_processor_large)

    def compute_regional_aggregate_with_ranking(self):
        self.regional_aggregate_with_ranking_step = self.create_python_processing_step(
            'regional_aggregate_with_ranking',
            job_inputs=[
                ProcessingInput(
                    input_name='regional_aggregate',
                    source=f'{self.output_destination_placement}compute_regional_aggregate/data/regional_aggregate',
                    destination=f'{self.output_source}regional_aggregate/'),
                ProcessingInput(
                    input_name='shap_ui_output',
                    source=f'{self.output_destination_placement}compute_shap_ui_output/data/shap_ui_output',
                    destination=f'{self.output_source}shap_ui_output/')
            ],
            job_arguments=[
                '--s3_input_regional_aggregate_folder', f'{self.output_source}regional_aggregate/',
                '--s3_input_shap_ui_output_folder', f'{self.output_source}shap_ui_output/'
            ])

    def compute_recommended_acres(self):
        self.recommended_acres_step = self.create_python_processing_step(
            'recommended_acres',
            job_inputs=[
                ProcessingInput(
                    input_name='grid_classification',
                    source=f'{self.output_destination_placement}compute_grid_classification/data/grid_classification',
                    destination=f'{self.output_source}grid_classification/'),
                ProcessingInput(
                    input_name='regional_aggregate_with_ranking',
                    source=f'{self.output_destination_placement}compute_regional_aggregate/data/regional_aggregate',
                    destination=f'{self.output_source}regional_aggregate/'),
                ProcessingInput(
                    input_name='temporal_aggregate',
                    source=f'{self.output_destination_placement}compute_temporal_aggregate/data/temporal_aggregate',
                    destination=f'{self.output_source}temporal_aggregate/')
            ],
            job_arguments=[
                '--s3_input_grid_classification_folder', f'{self.output_source}grid_classification/',
                '--s3_input_regional_aggregate_folder', f'{self.output_source}regional_aggregate/',
                '--s3_input_temporal_aggregate_folder', f'{self.output_source}temporal_aggregate/'
            ])

    def compute_tops_data_query(self):
        self.tops_data_query_step = self.create_spark_processing_step(
            'tops_data_query',
            arguments=[
                '--s3_input_tops_data',
                f's3://{self.bucket}/{ENVIRONMENT}/data/tops_pheno/ap_data_sector_name=CORN_NA_SUMMER/year=2023/',
                '--s3_output_tops_data_query',
                f'{self.spark_output_placement}compute_tops_data_query/data/tops_data_query/'
            ]
        )

    def compute_tops_data_query_2(self):
        self.tops_data_query_2_step = self.create_python_processing_step(
            'tops_data_query_2',
            job_inputs=[
                ProcessingInput(
                    input_name='tops_data_query',
                    source=f'{self.output_destination_placement}compute_tops_data_query/data/tops_data_query',
                    destination=f'{self.output_source}tops_data_query/'),
                ProcessingInput(
                    input_name='tops_data',
                    source=f's3://{self.bucket}/{ENVIRONMENT}/data/tops_pheno/ap_data_sector_name=CORN_NA_SUMMER/year=2023/',
                    destination=f'{self.output_source}tops_data/')
            ],
            job_arguments=[
                '--s3_input_tops_data_query_folder', f'{self.output_source}tops_data_query/',
                '--s3_input_tops_data_folder', f'{self.output_source}tops_data/'
            ])

    def compute_tops_data_query_3(self):
        self.tops_data_query_3_step = self.create_spark_processing_step(
            'tops_data_query_3',
            arguments=[
                '--s3_input_tops_data_2',
                f'{self.spark_output_placement}compute_tops_data_query_2/data/tops_data_query_2/',
                '--s3_output_tops_data_query_3',
                f'{self.spark_output_placement}compute_tops_data_query_3/data/tops_data_query_3/'
            ]
        )

    def pipeline_preparation(self):
        # self.comparison_set_step.add_depends_on([self.init_script_step])
        # self.comparison_set_v1_step.add_depends_on([self.init_script_step])
        # self.comparison_set_step.add_depends_on([self.comparison_set_v1_step])
        # self.giga_distance_step.add_depends_on([self.init_script_step])
        # self.sample_geno_output_giga_step.add_depends_on([self.init_script_step])
        # self.tops_data_query_step.add_depends_on([self.init_script_step])
        # self.tops_data_query_2_step.add_depends_on([self.tops_data_query_step])
        # self.tops_data_query_3_step.add_depends_on([self.tops_data_query_2_step])
        # self.trial_data_by_year_step.add_depends_on([self.init_script_step])
        # self.trial_data_all_years_step.add_depends_on([self.trial_data_by_year_step])
        # self.cropFact_api_output_by_year_step.add_depends_on([self.trial_data_by_year_step])
        # self.trial_feature_keep_array_step.add_depends_on([self.cropFact_api_output_by_year_step])
        # self.trial_feature_keep_array_step.add_depends_on([self.init_script_step])
        # self.sample_comps_for_grm_step.add_depends_on([self.comparison_set_step])
        # self.sample_geno_output_step.add_depends_on([self.sample_comps_for_grm_step])
        # self.sample_geno_output_step.add_depends_on([self.init_script_step])
        # self.hetpool1_all_years_step.add_depends_on([self.sample_geno_output_step])
        # self.hetpool2_all_years_step.add_depends_on([self.sample_geno_output_step])
        # self.pca_output_hetpool1_step.add_depends_on([self.hetpool1_all_years_step])
        # self.pca_output_hetpool2_step.add_depends_on([self.hetpool2_all_years_step])
        # self.hetpool1_all_years_step.add_depends_on([self.init_script_step])
        # self.hetpool2_all_years_step.add_depends_on([self.init_script_step])
        # self.pca_output_hetpool1_step.add_depends_on([self.hetpool1_all_years_step])
        # self.pca_output_hetpool2_step.add_depends_on([self.hetpool2_all_years_step])
        # self.trial_data_train_step.add_depends_on([self.trial_feature_keep_array_step, self.pca_output_hetpool1_step,self.pca_output_hetpool2_step])
        # self.trial_data_train_step.add_depends_on([self.init_script_step])
        # self.trial_data_train_step.add_depends_on([self.trial_feature_keep_array_step])
        # self.train_data_lgbm_step.add_depends_on([self.trial_data_train_step])
        # self.train_data_lgbm_1_step.add_depends_on([self.train_data_lgbm_step])
        # self.test_data_lgbm_step.add_depends_on([self.train_data_lgbm_step])
        # self.train_data_scored_agg_step.add_depends_on([self.train_data_lgbm_1_step])
        # self.test_data_scored_agg_step.add_depends_on([self.test_data_lgbm_step])
        # # self.soy_cropfact_grid_prep_step.add_depends_on([self.init_script_step])
        # self.corn_cropfact_grid_prep_step.add_depends_on([self.init_script_step])
        self.grid_classification_step.add_depends_on([self.init_script_step])
        # self.grid_classification_step.add_depends_on([self.test_data_scored_agg_step, self.train_data_scored_agg_step])
        self.cropFact_grid_for_model_step.add_depends_on([self.grid_classification_step])
        # self.cropFact_grid_for_model_step.add_depends_on([self.init_script_step])
        self.shap_ui_output_step.add_depends_on([self.cropFact_grid_for_model_step])
        # self.shap_ui_output_step.add_depends_on([self.init_script_step])
        # self.grid_performance_step.add_depends_on([self.init_script_step])
        self.grid_performance_step.add_depends_on([self.cropFact_grid_for_model_step])
        # self.temporal_aggregate_query_step.add_depends_on([self.init_script_step])
        # self.regional_aggregate_query_step.add_depends_on([self.init_script_step])
        self.temporal_aggregate_query_step.add_depends_on([self.grid_performance_step])
        # self.temporal_aggregate_step.add_depends_on([self.init_script_step])

        self.temporal_aggregate_step.add_depends_on([self.temporal_aggregate_query_step])
        self.regional_aggregate_query_step.add_depends_on([self.grid_performance_step])
        self.regional_aggregate_step.add_depends_on([self.regional_aggregate_query_step])
        self.recommended_acres_step.add_depends_on([self.regional_aggregate_step, self.temporal_aggregate_step])
        self.regional_aggregate_with_ranking_step.add_depends_on([self.recommended_acres_step, self.shap_ui_output_step])
        # self.regional_aggregate_with_ranking_step.add_depends_on([self.recommended_acres_step])
        # self.materials_without_geno_step.add_depends_on([self.init_script_step])

        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[self.param_ap_data_sector,
                        self.param_target_pipeline_runid,
                        self.param_analysis_year,
                        self.param_historical_build
                        ],
            steps=self.steps,
            sagemaker_session=self.sagemaker_session
        )
        self.pipeline.upsert(role_arn=self.role, tags=self.tags)

    def create_event(self, status, message):
        inputs = [
            # Mount the libs dir with your code here.
            ProcessingInput(
                input_name='libraries',
                source=self.libs_uri,
                destination=self.input_libs)
        ]
        step = ProcessingStep(name=f'ap-{ENVIRONMENT}-send_{status.lower()}_event',
                              processor=self.python_processor_default,
                              code=f'{self.libs_uri}/placement_send_event.py',
                              inputs=inputs,
                              job_arguments=['--event_bus', self.event_bus,
                                             '--ap_data_sector', self.param_ap_data_sector,
                                             '--target_pipeline_runid', self.param_target_pipeline_runid,
                                             '--analysis_year', self.param_analysis_year,
                                             '--status', status,
                                             '--message', message],
                              kms_key=self.output_kms_key)
        self.steps.append(step)
        return step

    def create_start_event(self):
        self.start_event_step = self.create_event('START', f'Start Placement DME pipeline: {self.pipeline_name}')

    def create_end_event(self):
        self.end_event_step = self.create_event('END', f'Finish Sagemaker Placement DME pipeline: {self.pipeline_name}')

    def create_pipeline(self):
        self.create_sagemaker_session()
        self.set_parameters()
        self.make_processors()
        self.init_script()
        # self.create_start_event()
        # self.compute_materials_without_geno()
        # self.compute_pvs_input_step()
        # self.compute_soy_comparison_set()
        # self.compute_comparison_set_v1()
        # self.compute_comparison_set()
        # self.compute_comparison_set_v1()
        # self.compute_giga_distance()
        # self.compute_sample_geno_output_giga()
        # self.compute_sample_comps_for_grm()
        # self.compute_sample_geno_output()
        # self.compute_hetpool1_all_years()
        # self.compute_hetpool2_all_years()
        # self.compute_pca_output_hetpool1()
        # self.compute_pca_output_hetpool2()
        # self.compute_tops_data_query()
        # self.compute_tops_data_query_2()
        # self.compute_tops_data_query_3()
        # self.compute_trial_data_by_year()
        # self.compute_trial_data_all_years()
        # self.compute_cropFact_api_output_by_year()
        # self.compute_trial_feature_keep_array()
        # self.compute_trial_data_train()
        # self.compute_train_data_lgbm()
        # self.compute_train_data_lgbm_1()
        # self.compute_test_data_lgbm()
        # self.compute_train_data_scored_agg()
        # self.compute_test_data_scored_agg()
        # self.compute_soy_cropfact_grid_prep()
        # self.compute_corn_cropfact_grid_prep()
        self.compute_grid_classification()
        self.compute_cropFact_grid_for_model()
        self.compute_shap_ui_output()
        self.compute_grid_performance()
        self.compute_temporal_aggregate_query()
        self.compute_temporal_aggregate()
        self.compute_regional_aggregate_query()
        self.compute_regional_aggregate()
        self.compute_recommended_acres()
        self.compute_regional_aggregate_with_ranking()
        # self.create_end_event()
        self.pipeline_preparation()


if __name__ == '__main__':
    print("start main in placement Pipeline pipeline. ")
    sp = PlacementPipeline()
    sp.create_pipeline()
    print("end main in placement Pipeline pipeline.")