import sys

from sagemaker.processing import (
    ScriptProcessor,
    ProcessingInput,
    ProcessingOutput,
    NetworkConfig,
)
from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow import ParameterString
from sagemaker.workflow.functions import Join, JsonGet
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import ProcessingStep

from libs.config.config_vars import CONFIG, DME_PROJECT, ENVIRONMENT_VARS
from libs.config.config_vars import ENVIRONMENT, S3_DATA_PREFIX, S3_BUCKET


class BasePipeline:
    def __init__(self):
        self.bucket = CONFIG.get("bucket")
        self.pipeline_name = f"ap-{ENVIRONMENT}-{DME_PROJECT}"
        self.role = CONFIG.get("role")
        self.subnets = CONFIG.get("subnets")
        self.security_group_ids = CONFIG.get("security_group_ids")
        self.script_base_job_name = CONFIG.get("script_base_job_name")
        self.spark_base_job_name = CONFIG.get("spark_base_job_name")
        self.image_uri = CONFIG.get("image_uri")
        self.script_instance_count = CONFIG.get("script_instance_count")
        self.spark_instance_count = CONFIG.get("spark_instance_count")
        self.instance_type = CONFIG.get("instance_type")
        self.output_kms_key = CONFIG.get("output_kms_key")
        self.tags = CONFIG["tags"]
        self.input_libs = CONFIG.get("input_libs")
        self.input_variables = CONFIG.get("input_variables")
        self.output_source = CONFIG.get("output_source")
        self.output_destination = CONFIG.get("output_destination")
        self.spark_output = CONFIG.get("spark_output")
        self.extra_args = {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": self.output_kms_key,
        }
        self.event_bus = CONFIG.get("event_bus")
        self.init_script_step = None

    def create_sagemaker_session(self):
        self.sagemaker_session = PipelineSession(default_bucket=self.bucket)
        network_config = NetworkConfig(
            enable_network_isolation=False,
            subnets=self.subnets,
            security_group_ids=self.security_group_ids,
        )

        self.python_processor = ScriptProcessor(
            command=["python3"],
            base_job_name=self.script_base_job_name,
            image_uri=self.image_uri,
            role=self.role,
            instance_count=self.script_instance_count,
            instance_type=self.instance_type,
            network_config=network_config,
            output_kms_key=self.output_kms_key,
            tags=self.tags,
            env=ENVIRONMENT_VARS,
        )

        self.spark_processor = PySparkProcessor(
            framework_version="3.5",
            base_job_name=self.spark_base_job_name,
            sagemaker_session=self.sagemaker_session,
            role=self.role,
            instance_count=self.spark_instance_count,
            instance_type=self.instance_type,
            network_config=network_config,
            output_kms_key=self.output_kms_key,
            tags=self.tags,
            image_uri=self.image_uri,
            env=ENVIRONMENT_VARS,
        )
        self.postgres_driver = S3Uploader.upload(
            local_path="../../driver/postgresql-42.5.4.jar",
            desired_s3_uri=f"{self.output_destination}driver",
            kms_key=self.output_kms_key,
        )

        self.libs_uri = S3Uploader.upload(
            local_path="../../libs",
            desired_s3_uri=f"{self.output_destination}libs",
            kms_key=self.output_kms_key,
        )

        self.data_uri = S3Uploader.upload(
            local_path="../data",
            desired_s3_uri=f"{self.output_destination}data",
            kms_key=self.output_kms_key,
        )

        self.egg_uri = S3Uploader.upload(
            local_path=f"../../dist/sagemaker_dme_lib-1.0.0-py3.{sys.version_info.minor}.egg",
            desired_s3_uri=f"{self.output_destination}egg",
            kms_key=self.output_kms_key,
        )

    def create_python_processing_step(
        self,
        name,
        job_inputs=None,
        job_outputs=None,
        job_arguments=None,
        property_files=None,
    ):
        input_src_uri = S3Uploader.upload(
            local_path=f"../compute_{name}/src",
            desired_s3_uri=f"{self.output_destination}compute_{name}/src",
            kms_key=self.output_kms_key,
        )
        inputs = [
            # Mount the libs dir with your code here.
            ProcessingInput(
                input_name="libraries",
                source=self.libs_uri,
                destination=self.input_libs,
            )
        ]
        if self.init_script_step:
            inputs += [
                ProcessingInput(
                    input_name="input_variables",
                    source=self.get_destination(),
                    destination=self.input_variables,
                )
            ]
        if job_inputs:
            inputs += job_inputs

        if job_outputs:
            outputs = job_outputs
        else:
            outputs = [
                # Mount the src dir with your code here.
                ProcessingOutput(
                    output_name="output",
                    source=self.output_source,
                    destination=self.get_destination(),
                ),
            ]

        step = ProcessingStep(
            name=f"ap-{ENVIRONMENT}-python_{name}_step",
            processor=self.python_processor,
            code=f"{input_src_uri}/processing.py",
            inputs=inputs,
            outputs=outputs,
            job_arguments=job_arguments,
            kms_key=self.output_kms_key,
            property_files=property_files,
        )
        return step

    def create_spark_processing_step(self, name, arguments=None):
        input_src_uri = S3Uploader.upload(
            local_path=f"../compute_{name}/src",
            desired_s3_uri=f"{self.output_destination}compute_{name}/src",
            kms_key=self.output_kms_key,
        )
        inputs = [
            ProcessingInput(
                input_name="input_variables",
                source=self.get_destination(),
                destination="/opt/ml/processing/input/input_variables",
            )
        ]
        step = ProcessingStep(
            name=f"ap-{ENVIRONMENT}-spark-{name}_step",
            step_args=self.spark_processor.run(
                submit_app=f"{input_src_uri}/processing.py",
                submit_py_files=[self.egg_uri],
                submit_jars=[self.postgres_driver],
                arguments=arguments,
                inputs=inputs,
                kms_key=self.output_kms_key,
                spark_event_logs_s3_uri=f"{self.output_destination}compute_{name}/spark_event_logs",
                logs=False,
            ),
        )
        return step

    def set_parameters(self):
        self.param_ap_data_sector = ParameterString(name="ap_data_sector")
        self.param_analysis_year = ParameterString(name="analysis_year")
        self.param_target_pipeline_runid = ParameterString(name="target_pipeline_runid")
        self.param_analysis_run_group = ParameterString(name="analysis_run_group")
        self.param_breakout_level = ParameterString(name="breakout_level")
        self.param_force_refresh = ParameterString(
            name="force_refresh", default_value="False"
        )

    def get_destination(self, sub_folder="", is_spark=False):
        protocol = "s3a:/" if is_spark else "s3:/"
        return Join(
            on="/",
            values=[
                protocol,
                S3_BUCKET,
                S3_DATA_PREFIX,
                "archive",
                self.param_target_pipeline_runid,
                self.param_analysis_run_group,
                sub_folder,
            ],
        )

    def get_dme_output(self, table_name):
        protocol = "s3:/"
        return Join(
            on="/",
            values=[
                protocol,
                S3_BUCKET,
                S3_DATA_PREFIX,
                "output",
                table_name,
                Join(
                    on="_",
                    values=[
                        self.param_analysis_year,
                        self.param_ap_data_sector,
                        self.param_analysis_run_group
                    ]),
            ],
        )

    def get_static_data(self, file_name):
        return f"{self.data_uri}/{file_name}"

    def init_script(self):
        self.query_variables = PropertyFile(
            name="query_variables",
            output_name="output",
            path="query_variables.json",
        )
        self.init_script_step = self.create_python_processing_step(
            "init_script",
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
            ],
            property_files=[self.query_variables],
        )
