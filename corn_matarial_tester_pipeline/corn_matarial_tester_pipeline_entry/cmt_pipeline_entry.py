import sys

from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput, NetworkConfig
from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.steps import CacheConfig
from sagemaker.workflow.functions import Join, JsonGet
from sagemaker.workflow.properties import PropertyFile

from libs.config.config_vars import CONFIG, ENVIRONMENT, DME_PROJECT, S3_PREFIX, ENVIRONMENT_VARS, S3_DATA_PREFIX, \
    S3_BUCKET

class CMTPipeline:
    def __init__(self):
        print("start init in CMT Pipeline pipeline!")
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
        self.tags = CONFIG['tags']
        self.input_libs = CONFIG.get('input_libs')
        self.input_variables = CONFIG.get('input_variables')
        self.output_source = CONFIG.get('output_source')
        self.output_destination = CONFIG.get('output_destination')
        self.spark_output = CONFIG.get('spark_output')
        self.output_prefix = CONFIG.get('output_prefix')
        self.extra_args = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': self.output_kms_key}
        self.event_bus = CONFIG.get('event_bus')
        self.init_script_step = None

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
            #env={'DME_ENV': ENVIRONMENT}
            env=ENVIRONMENT_VARS
        )

        return python_processor

    def create_sagemaker_session(self):
        self.sagemaker_session = PipelineSession(default_bucket=self.bucket)
        network_config = NetworkConfig(enable_network_isolation=False,
                                       subnets=self.subnets,
                                       security_group_ids=self.security_group_ids)

        #make default processor based on config_vars
        self.python_processor_default = self.make_python_processor(
            network_config=network_config,
            instance_type=self.instance_type,
            instance_count=1
        )

        self.python_processor_large = self.make_python_processor(
            network_config=network_config,
            instance_type='ml.m5.4xlarge',
            instance_count=1
        )

        self.python_processor_xlarge = self.make_python_processor(
            network_config=network_config,
            instance_type='ml.m5.12xlarge',
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
        
        self.data_uri = S3Uploader.upload(
            local_path='../data',
            desired_s3_uri=f'{self.output_destination}data',
            kms_key=self.output_kms_key
        )

        self.egg_uri = S3Uploader.upload(
            local_path=f'../../dist/sagemaker_dme_lib-1.0.0-py3.{sys.version_info.minor}.egg',
            desired_s3_uri=f'{self.output_destination}egg',
            kms_key=self.output_kms_key
        )

    def set_parameters(self):
        self.param_target_pipeline_runid = ParameterString(name='target_pipeline_runid')
        self.param_force_refresh = ParameterString(name='force_refresh', default_value='True')
        self.param_analysis_type = 'cmt'

    def create_python_processing_step(self,
                                      name,
                                      job_inputs=None,
                                      job_outputs=None,
                                      job_arguments=None,
                                      python_processor=None,
                                      property_files=None):

        """
        Python processing step.
        name : name of step
        job_inputs : inputs to download from S3 to a local directory
        job_outputs : files outputted by script that should be pushed to an S3 bucket.
        job_arguments : args structure inputs to script. This allows parameters/file locations to be sent to the script
        python_processor : select processor to use for each step.
            See make_python_processor() to make a non-default processor.
            Uses python_processor_default by default (if nothing is provided).
        """

        # set base_python_processor as default
        if python_processor is None:
            python_processor=self.python_processor_default

        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination}compute_{name}/src',
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
                source=self.get_destination(),
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
                    destination=self.get_destination()),
            ]
        step = ProcessingStep(name=f'ap-{ENVIRONMENT}-compute_{name}_step',
                              processor=python_processor,
                              code=f'{input_src_uri}/processing.py',
                              inputs=inputs,
                              outputs=outputs,
                              job_arguments=job_arguments,
                              kms_key=self.output_kms_key,
                              property_files=property_files)
        return step

    def create_spark_processing_step(self, 
                                     name, 
                                     arguments=None,
                                     python_processor=None,
                                     spark_processor=None):
        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination}compute_{name}/src',
            kms_key=self.output_kms_key
        )

        # set base_python_processor as default
        if python_processor is None:
            python_processor=self.python_processor_default

        if spark_processor is None:
            spark_processor=self.spark_processor_default

        inputs = [
            # Mount the libs dir with your code here.
            ProcessingInput(
                input_name='libraries',
                source=self.libs_uri,
                destination=self.input_libs),
            ProcessingInput(
                input_name='input_variables',
                source=self.get_destination(),
                destination='/opt/ml/processing/input/input_variables')
        ]
        step = ProcessingStep(
            name=f'ap-{ENVIRONMENT}-spark-{name}_step',
            step_args=spark_processor.run(
                submit_app=f'{input_src_uri}/processing.py',
                submit_py_files=[self.egg_uri],
                submit_jars=[self.postgres_driver],
                arguments=arguments,
                inputs=inputs,
                kms_key=self.output_kms_key,
                spark_event_logs_s3_uri=f'{self.output_destination}compute_{name}/spark_event_logs',
                logs=False)
        )
        return step

    def init_script(self):
        self.query_variables = PropertyFile(
            name='query_variables',
            output_name='output',
            path='query_variables.json',
        )
        self.init_script_step = self.create_python_processing_step(
            'init_script',
            job_arguments=[
               '--target_pipeline_runid', self.param_target_pipeline_runid,
               '--force_refresh', self.param_force_refresh,
               '--analysis_type', self.param_analysis_type
            ],
            property_files=[self.query_variables]
        )

    def compute_geno_sample_list(self):
        self.geno_sample_list_step = self.create_python_processing_step(
            name='geno_sample_list',
            job_inputs=[],
            job_outputs=[
                ProcessingOutput(
                    output_name='output',
                    source=self.output_source,
                    destination=self.get_destination())
            ]

        )

    def compute_material_info_sp(self):
        self.material_info_sp_step = self.create_python_processing_step(
            name='material_info_sp',
            python_processor=self.python_processor_xlarge,
            job_inputs=[],
            job_outputs=[
                ProcessingOutput(
                    output_name='output',
                    source=self.output_source,
                    destination=self.get_destination())
            ]

        )

    def compute_genetic_group_classification(self):
        self.genetic_group_classification_step = self.create_python_processing_step(
            name='genetic_group_classification',
            job_inputs=[],
            job_outputs=[
                ProcessingOutput(
                    output_name='output',
                    source=self.output_source,
                    destination=self.get_destination())
            ]

        )

    def compute_material_prepared_joined(self):
        s3_input_material_info_sp = self.get_destination('material_info_sp.parquet')
        s3_output_material_prepared_joined = self.get_destination('material_prepared_joined', True)
        self.material_prepared_joined_step = self.create_spark_processing_step(
            'material_prepared_joined',
            python_processor=self.python_processor_large,
            arguments=[
                '--s3_input_material_info_sp', s3_input_material_info_sp,
                '--s3_output_material_prepared_joined', s3_output_material_prepared_joined

            ]
        )

    def compute_geno_sample_info(self):
        s3_input_material_prepared_joined = self.get_destination('material_prepared_joined')
        s3_input_geno_sample_list = self.get_destination('geno_sample_list.csv')
        s3_output_geno_sample_info = self.get_destination('geno_sample_info', True)
        self.geno_sample_info_step = self.create_spark_processing_step(
            'geno_sample_info',
            arguments=[
                '--s3_input_material_prepared_joined',s3_input_material_prepared_joined,
                '--s3_input_geno_sample_list', s3_input_geno_sample_list,
                '--s3_output_geno_sample_info', s3_output_geno_sample_info
            ]
        )

    def compute_fada_groups_joined(self):
        self.sagemaker_session.upload_data(
            '../data',
            key_prefix=f'{self.output_prefix}/data',
            extra_args=self.extra_args
        )

        s3_input_material_prepared_joined = self.get_destination('material_prepared_joined')
        s3_input_genetic_group_classification = self.get_destination('genetic_group_classification.csv')
        s3_output_fada_groups_joined = self.get_destination('fada_groups_joined', True)

        self.fada_groups_joined_step = self.create_spark_processing_step(
            'fada_groups_joined',
            arguments=[
                '--s3_input_material_prepared_joined', s3_input_material_prepared_joined,
                '--s3_input_fada_ss_nss',
                f'{self.spark_output}data/fada_ss_nss.csv',
                '--s3_input_genetic_group_classification', s3_input_genetic_group_classification,
                '--s3_output_fada_groups_joined', s3_output_fada_groups_joined
            ]
        )

    def compute_filled_material_tester_calculated_rough(self):
        
        s3_input_material_prepared_joined = self.get_destination('material_prepared_joined')
        s3_input_geno_sample_info = self.get_destination('geno_sample_info')
        s3_input_fada_groups_joined = self.get_destination('fada_groups_joined')
        s3_output_filled_material_tester_calculated_rough = self.get_destination('filled_material_tester_calculated_rough', True)

        self.filled_material_tester_calculated_rough_step = self.create_spark_processing_step(
            'filled_material_tester_calculated_rough',
            python_processor=self.python_processor_xlarge,
            spark_processor=self.spark_processor_large,
            arguments=[
                '--s3_input_fada_groups_joined', s3_input_fada_groups_joined,
                '--s3_input_material_prepared_joined', s3_input_material_prepared_joined,
                '--s3_input_geno_sample_info', s3_input_geno_sample_info,
                '--s3_output_filled_material_tester_calculated_rough', s3_output_filled_material_tester_calculated_rough
            ]
        )


    def compute_filled_mtc_final_bid2(self):

        s3_input_filled_material_tester_calculated_rough = self.get_destination('filled_material_tester_calculated_rough')
        s3_output_filled_mtc_final_bid2 = self.get_destination('filled_mtc_final_bid2', True)

        self.filled_mtc_final_bid2_step = self.create_spark_processing_step(
            'filled_mtc_final_bid2',
            python_processor=self.python_processor_large,
            spark_processor=self.spark_processor_large,
            arguments=[
                '--s3_input_filled_material_tester_calculated_rough', s3_input_filled_material_tester_calculated_rough,
                '--s3_output_filled_mtc_final_bid2', s3_output_filled_mtc_final_bid2
            ]
        )

    def pipeline_preparation(self):
        self.geno_sample_list_step.add_depends_on([self.init_script_step])
        self.material_info_sp_step.add_depends_on([self.init_script_step])
        self.genetic_group_classification_step.add_depends_on([self.init_script_step])
        self.material_prepared_joined_step.add_depends_on([self.material_info_sp_step])

        self.geno_sample_info_step.add_depends_on([
            self.geno_sample_list_step,
            self.material_prepared_joined_step
        ])

        self.fada_groups_joined_step.add_depends_on([
            self.material_prepared_joined_step,
            self.genetic_group_classification_step
        ])

        self.filled_material_tester_calculated_rough_step.add_depends_on([
            self.geno_sample_info_step,
            self.fada_groups_joined_step,
            self.material_prepared_joined_step
        ])

        self.filled_mtc_final_bid2_step.add_depends_on([
            self.filled_material_tester_calculated_rough_step
        ])

        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[self.param_target_pipeline_runid,
                        self.param_force_refresh,
                        self.param_analysis_type],
            steps=[self.init_script_step, self.geno_sample_list_step,
                   self.genetic_group_classification_step,
                   self.material_info_sp_step,
                   self.material_prepared_joined_step,
                   self.geno_sample_info_step,
                   self.fada_groups_joined_step,
                   self.filled_material_tester_calculated_rough_step,
                   self.filled_mtc_final_bid2_step
                   ],
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
                              code=f'{self.libs_uri}/send_event.py',
                              inputs=inputs,
                              job_arguments=['--event_bus', self.event_bus,
                                             '--ap_data_sector', 'cmt',
                                             '--analysis_year', 'cmt',
                                             '--analysis_type', 'cmt',
                                             '--target_pipeline_runid', self.param_target_pipeline_runid,
                                             '--stages', 'cmt',
                                             '--breakout_level', 'cmt',
                                             '--analysis_run_group', 'cmt',
                                             '--status', status,
                                             '--message', message],
                              kms_key=self.output_kms_key)
        return step

    def create_start_event(self):
        self.start_event_step = self.create_event('START', f'Start CMT pipeline: {self.pipeline_name}')

    def create_end_event(self):
        self.end_event_step = self.create_event('END', f'Finish CMT pipeline: {self.pipeline_name}')

    def create_pipeline(self):
        self.create_sagemaker_session()
        self.set_parameters()
        self.init_script()
        self.compute_geno_sample_list()
        self.compute_material_info_sp()
        self.compute_material_prepared_joined()
        self.compute_genetic_group_classification()
        self.compute_geno_sample_info()
        self.compute_fada_groups_joined()
        self.compute_filled_material_tester_calculated_rough()
        self.compute_filled_mtc_final_bid2()
        self.pipeline_preparation()

    def get_destination(self, sub_folder='', is_spark=False):
        protocol = 's3a:/' if is_spark else 's3:/'
        return Join(on='/', values=[protocol, S3_BUCKET, S3_DATA_PREFIX, self.param_target_pipeline_runid,
                                    self.param_analysis_type, sub_folder])

    def get_static_data(self, file_name):
        return f'{self.data_uri}/{file_name}'

if __name__ == '__main__':
    print("start main in CMTPipeline pipeline in environment." + ENVIRONMENT)
    sp = CMTPipeline()
    sp.create_pipeline()
    print("end main in CMTPipeline pipeline.")
