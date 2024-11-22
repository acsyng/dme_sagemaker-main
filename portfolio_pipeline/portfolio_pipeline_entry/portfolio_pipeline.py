import sys

from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput, NetworkConfig
from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.steps import CacheConfig
from sagemaker.workflow.functions import Join

from libs.config.config_vars import CONFIG, ENVIRONMENT, DME_PROJECT, \
    S3_PREFIX, ENVIRONMENT_VARS, S3_DATA_PREFIX, S3_BUCKET


class PerformanceIngestionPipeline:
    def __init__(self):
        print("start init in Portfolio Pipeline!")
        self.bucket = CONFIG.get('bucket')
        self.pipeline_name = f'ap-{ENVIRONMENT}-portfolio'
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
        self.extra_args = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': self.output_kms_key}
        self.event_bus = CONFIG.get('event_bus')
        self.environment = ENVIRONMENT
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

        self.portfolio_libs_uri = S3Uploader.upload(
            local_path='../portfolio_libs',
            desired_s3_uri=f'{self.output_destination}portfolio_libs',
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

        self.python_processor_small = self.make_python_processor(
            network_config=network_config,
            instance_type='ml.m5.large',
            instance_count=1
        )

        self.python_processor_large = self.make_python_processor(
            network_config=network_config,
            instance_type='ml.m5.4xlarge',
            instance_count=1
        )

        self.spark_processor = PySparkProcessor(framework_version='3.1',
                                                base_job_name=self.spark_base_job_name,
                                                sagemaker_session=self.sagemaker_session,
                                                role=self.role,
                                                instance_count=self.spark_instance_count,
                                                instance_type=self.instance_type,
                                                network_config=network_config,
                                                output_kms_key=self.output_kms_key,
                                                tags=self.tags,
                                                image_uri=self.image_uri,
                                                env=ENVIRONMENT_VARS
                                                )

    def set_parameters(self):
        """
        Set UI input parameters of this pipeline.

        When multiple years are provided, should be comma separated.
        Flags are either '1' or '0'
        """
        self.param_ap_data_sector = ParameterString(name='ap_data_sector')
        self.param_analysis_year = ParameterString(name='analysis_year')
        self.param_target_pipeline_runid = ParameterString(name='target_pipeline_runid')
        self.param_force_refresh = ParameterString(name='force_refresh', default_value='True')


    def create_python_processing_step(self,
                                      name,
                                      job_inputs=None,
                                      job_outputs=None,
                                      job_arguments=None,
                                      python_processor=None):

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
            python_processor = self.python_processor_default

        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination}compute_{name}/src',
            kms_key=self.output_kms_key
        )
        inputs = [
            # Mount the portfolio_libs dir with your code here.
            ProcessingInput(
                input_name='libraries',
                source=self.libs_uri,
                destination=self.input_libs
            ),
            ProcessingInput(
                input_name='portfolio_libraries',
                source=self.portfolio_libs_uri,
                destination='/opt/ml/processing/input/code/portfolio_libs'
            )
        ]
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
                    destination=self.get_destination(sub_folder='',pipeline_name='')
                )
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

    def create_spark_processing_step(self, name, arguments=None):
        input_src_uri = S3Uploader.upload(
            local_path=f'../compute_{name}/src',
            desired_s3_uri=f'{self.output_destination}compute_{name}/src',
            kms_key=self.output_kms_key
        )
        inputs = [ProcessingInput(
            input_name='input_variables',
            source=
            self.init_script_step.properties.ProcessingOutputConfig.Outputs[
                'output'].S3Output.S3Uri,
            destination='/opt/ml/processing/input/input_variables')]
        step = ProcessingStep(
            name=f'ap-{ENVIRONMENT}-{name}_step',
            step_args=self.spark_processor.run(
                submit_app=f'{input_src_uri}/processing.py',
                submit_py_files=[self.egg_uri],
                submit_jars=[self.postgres_driver],
                arguments=arguments,
                inputs=inputs,
                kms_key=self.output_kms_key,
                spark_event_logs_s3_uri=f'{self.output_destination}compute_{name}/spark_event_logs',
                logs=False)
        )
        self.steps.append(step)
        return step

    def compute_data_ingestion(self):
        self.data_ingestion_step = self.create_python_processing_step(
            name='data_ingestion',
            job_outputs=[
                ProcessingOutput(
                    output_name='output_data_ingestion',
                    source=f'{self.output_source}data_ingestion/',
                    destination=self.get_destination(
                        sub_folder='data_ingestion',
                        include_pipeline_runid=False,
                        pipeline_name=''
                    )
                )
            ]
        )

    def compute_product_lifecycle(self):
        self.product_lifecycle_step = self.create_python_processing_step(
            name='product_lifecycle',
            job_inputs=[
                ProcessingInput(
                    input_name='ingested_data',
                    source=self.get_destination(
                        sub_folder='data_ingestion',
                        include_pipeline_runid=False,
                        pipeline_name=''
                    ),
                    destination=f'{self.output_source}data_ingestion/'
                ),
            ],
            job_outputs=[
                ProcessingOutput(
                    output_name='output_lifecycle',
                    source=f'{self.output_source}product_lifecycle/',
                    destination=self.get_destination(
                        sub_folder='data_ingestion',
                        include_pipeline_runid=False,
                        pipeline_name=''
                    )
                )
            ],
            job_arguments=['--s3_input_data_ingestion_folder', f'{self.output_source}data_ingestion/']
        )

    def compute_nondominated_fronts(self):
        self.nondominated_fronts_step = self.create_python_processing_step(
            name='nondominated_fronts',
            python_processor=self.python_processor_large,
            job_inputs=[
                ProcessingInput(
                    input_name='ingested_data',
                    source=self.get_destination(
                        sub_folder='data_ingestion',
                        include_pipeline_runid=False,
                        pipeline_name=''
                    ),
                    destination=f'{self.output_source}data_ingestion/'
                ),
            ],
            job_outputs=[
                ProcessingOutput(
                    output_name='output_lifecycle',
                    source=f'{self.output_source}nondominated_fronts/',
                    destination=self.get_destination(
                        sub_folder='nondominated_fronts',
                        include_pipeline_runid=False,
                        pipeline_name=''
                    )
                )
            ],
            job_arguments=['--s3_input_data_ingestion_folder', f'{self.output_source}data_ingestion/']
        )

    def init_script(self):
        """
        store initial parameters. These parameters are loaded at the beginning of each script.
        All parameters start as strings, compute_init_script_infer performs some processing
            Like splitting comma-separated strings into lists (for years to load). Each year within the list is a string
        Boolean flags should be strings ('1' or '0')
        """
        self.init_script_step = self.create_python_processing_step(
            'init_script_portfolio',
            job_arguments=[
                '--ap_data_sector', self.param_ap_data_sector,
                '--analysis_year', self.param_analysis_year,
                '--target_pipeline_runid', self.param_target_pipeline_runid,
                '--force_refresh', self.param_force_refresh
            ])

    def pipeline_preparation(self):
        #self.data_ingestion_step.add_depends_on([self.init_script_step])
        #self.product_lifecycle_step.add_depends_on([self.init_script_step])
        self.nondominated_fronts_step.add_depends_on([self.init_script_step])

        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[self.param_ap_data_sector,
                        self.param_analysis_year,
                        self.param_target_pipeline_runid,
                        self.param_force_refresh
                        ],

            steps=self.steps,
            sagemaker_session=self.sagemaker_session
        )
        self.pipeline.upsert(role_arn=self.role, tags=self.tags)

    def create_event(self, status, message):
        inputs = [
            # Mount the portfolio_libs dir with your code here.
            ProcessingInput(
                input_name='libraries',
                source=self.libs_uri,
                destination=self.input_libs)
        ]
        step = ProcessingStep(name=f'ap-{ENVIRONMENT}-send_{status.lower()}_event',
                              code=f'{self.libs_uri}/portfolio_send_event.py',
                              processor=self.python_processor_default,
                              inputs=inputs,
                              job_arguments=[
                                  '--event_bus', self.event_bus,
                                  '--ap_data_sector', self.param_ap_data_sector,
                                  '--target_pipeline_runid', self.param_target_pipeline_runid,
                                  '--status', status,
                                  '--message', message
                              ],
                              kms_key=self.output_kms_key)
        self.steps.append(step)
        return step

    def create_start_event(self):
        self.start_event_step = self.create_event('START', f'Start Performance DME pipeline: {self.pipeline_name}')

    def create_end_event(self):
        self.end_event_step = self.create_event('END', f'Finish Sagemaker DME pipeline: {self.pipeline_name}')

    def create_pipeline(self):
        self.create_sagemaker_session()
        self.set_parameters()
        self.make_processors()
        self.init_script()
        #self.compute_data_ingestion()
        #self.compute_product_lifecycle()
        self.compute_nondominated_fronts()
        self.pipeline_preparation()

    def get_destination(self, sub_folder='', include_pipeline_runid=True, pipeline_name='portfolio', is_spark=False):
        # set output folder destination
        # automatically append data sector and forward model year to folder structure

        protocol = 's3a:/' if is_spark else 's3:/'
        # apparently join will join empty strings. Fun. Set list, check if empty before joining
        values_list = []
        if include_pipeline_runid:
            possible_values = [protocol, S3_BUCKET, S3_DATA_PREFIX,
                                    self.param_ap_data_sector, pipeline_name,
                                    self.param_target_pipeline_runid, sub_folder]
        else:
            possible_values = [protocol, S3_BUCKET, S3_DATA_PREFIX,
                                    self.param_ap_data_sector, pipeline_name,
                                    sub_folder]

        for v in possible_values:
            if v != '':
                values_list.append(v)

        return Join(on='/', values=values_list)


if __name__ == '__main__':
    print("start main in Portfolio Pipeline.")
    sp = PerformanceIngestionPipeline()
    sp.create_pipeline()
    print("end main in Portfolio Pipeline.")
