import os


ENVIRONMENT = os.getenv("DME_ENV", "dev")
DME_PROJECT = os.getenv("DME_PROJECT")
S3_PREFIX = f"{ENVIRONMENT}/source/dme/{DME_PROJECT}/"
S3_DATA_PREFIX = f"{ENVIRONMENT}/data/dme/{DME_PROJECT}"

S3_PREFIX_PLACEMENT = f"{ENVIRONMENT}/dme/{DME_PROJECT}/"

S3_BUCKETS = {
    "dev": "us.com.syngenta.ap.nonprod",
    "uat": "us.com.syngenta.ap.nonprod",
    "prod": "us.com.syngenta.ap.prod",
}
S3_BUCKET = S3_BUCKETS[ENVIRONMENT]
CLOUD_WATCH_GROUP = f"/aws/sagemaker/{ENVIRONMENT}/dme/{DME_PROJECT}"

ENVIRONMENT_VARS = {
    "DME_PROJECT": DME_PROJECT,
    "ENVIRONMENT": ENVIRONMENT,
    "DME_ENV": ENVIRONMENT,  # environment variable DME_ENV defaults to 'dev' if not provided.
    "CLOUD_WATCH_GROUP": CLOUD_WATCH_GROUP,
}

AWS_TAGS = [
    {"Key": "Application", "Value": "Advancement placement"},
    {"Key": "Platform", "Value": "ADAPT"},
    {"Key": "Name", "Value": "ADAPT-dme-sagemaker-job"},
    {"Key": "CostCenter", "Value": "rdit3000"},
    {
        "Key": "Purpose",
        "Value": "Advancement placement decision metric and machine learning model jobs to support breeder decisions",
    },
    {"Key": "ContactEmail", "Value": "keri.rehm@syngenta.com"},
    {"Key": "OwnerEmail", "Value": "clay.cole@syngenta.com"},
    {"Key": "Namespace", "Value": "ap"},
    {"Key": "BusinessFunction", "Value": "Seeds R+D"},
    {"Key": "Environment", "Value": ENVIRONMENT},
    {"Key": "ProjectIONumber", "Value": "-"},
]

COMMON_CONFIG = {
    "cost_center": "RDIT3000",
    "input_libs": "/opt/ml/processing/input/code/libs",
    "input_variables": "/opt/ml/processing/input/input_variables",
    "send_from": "Adapt.Teams@syngenta.com",
    "output_source": "/opt/ml/processing/data/",
    "tags": AWS_TAGS,
}

MAIN_CONFIG = {
    "dev": {
        "bucket": S3_BUCKET,
        "role": "arn:aws:iam::809800841141:role/ap_nonprod_sagemaker_role",
        "subnets": ["subnet-0fb710a91cc2c3a6d", "subnet-0aa1dc140fbb37b31"],
        "security_group_ids": ["sg-00634d30526efcdec", "sg-0405f346fdd704d24"],
        "script_base_job_name": "ap-nonrod-custom-dockercontainer-test",
        "spark_base_job_name": "ap-nonprod-train-sparkprocessor",
        "image_uri": "809800841141.dkr.ecr.us-east-1.amazonaws.com/ap-dme-sagemaker:latest",
        "script_instance_count": 1,
        "spark_instance_count": 2,
        "instance_type": "ml.m5.2xlarge",
        "output_kms_key": "arn:aws:kms:us-east-1:809800841141:key/353d6263-d454-444f-ac60-41afe025b445",
        "send_to": "ea74c0ac.Syngenta.onmicrosoft.com@emea.teams.ms",
        "output_destination": f"s3://{S3_BUCKET}/{S3_PREFIX}",
        "spark_output": f"s3a://{S3_BUCKET}/{S3_PREFIX}",
        "output_destination_placement": f"s3://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "spark_output_placement": f"s3a://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "event_bus": "ap_single_eventbus_dev",
        "performance_pipeline_arn": "arn:aws:sagemaker:us-east-1:809800841141:pipeline/ap-dev-performance-infer",
        "dme_output_metrics_s3_path": f"s3://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}dme_output_metrics",
        "performance_ingestion_arn": "arn:aws:sagemaker:us-east-1:809800841141:pipeline/ap-dev-performance-ingestion"
    },
    "uat": {
        "bucket": S3_BUCKET,
        "role": "arn:aws:iam::809800841141:role/ap_nonprod_sagemaker_role",
        "subnets": ["subnet-0fb710a91cc2c3a6d", "subnet-0aa1dc140fbb37b31"],
        "security_group_ids": ["sg-00634d30526efcdec", "sg-0405f346fdd704d24"],
        "script_base_job_name": "ap-nonrod-custom-dockercontainer-test",
        "spark_base_job_name": "ap-nonprod-train-sparkprocessor",
        "image_uri": "809800841141.dkr.ecr.us-east-1.amazonaws.com/ap-dme-sagemaker:latest",
        "script_instance_count": 1,
        "spark_instance_count": 2,
        "instance_type": "ml.m5.2xlarge",
        "output_kms_key": "arn:aws:kms:us-east-1:809800841141:key/353d6263-d454-444f-ac60-41afe025b445",
        "send_to": "57a7b081.Syngenta.onmicrosoft.com@emea.teams.ms",
        "output_destination": f"s3://{S3_BUCKET}/{S3_PREFIX}",
        "spark_output": f"s3a://{S3_BUCKET}/{S3_PREFIX}",
        "output_destination_placement": f"s3://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "spark_output_placement": f"s3a://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "event_bus": "ap_single_eventbus_uat",
        "performance_pipeline_arn": "arn:aws:sagemaker:us-east-1:809800841141:pipeline/ap-uat-performance-infer",
        "performance_ingestion_arn": "arn:aws:sagemaker:us-east-1:809800841141:pipeline/ap-uat-performance-ingestion",
    },
    "prod": {
        "bucket": S3_BUCKET,
        "role": "arn:aws:iam::750606694809:role/ap_prod_sagemaker_role",
        "subnets": ["subnet-0ee7fb35ce98c32f3", "subnet-00ea5be020b94dced"],
        "security_group_ids": ["sg-03eada0fa38ae0cdc", "sg-04174dd6282c1055b"],
        "script_base_job_name": "ap-prod-custom-dockercontainer-test",
        "spark_base_job_name": "ap-prod-train-sparkprocessor",
        "image_uri": "750606694809.dkr.ecr.us-east-1.amazonaws.com/ap-dme-sagemaker:latest",
        "script_instance_count": 1,
        "spark_instance_count": 2,
        "instance_type": "ml.m5.4xlarge",
        "output_kms_key": "arn:aws:kms:us-east-1:750606694809:key/e86be4b3-6b15-4fee-9ef4-9c862bb2d690",
        "send_to": "2556f33e.Syngenta.onmicrosoft.com@emea.teams.ms",
        "output_destination": f"s3://{S3_BUCKET}/{S3_PREFIX}",
        "spark_output": f"s3a://{S3_BUCKET}/{S3_PREFIX}",
        "output_destination_placement": f"s3://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "spark_output_placement": f"s3a://{S3_BUCKET}/{S3_PREFIX_PLACEMENT}",
        "event_bus": "ap_single_eventbus_prod",
        "performance_pipeline_arn": "arn:aws:sagemaker:us-east-1:750606694809:pipeline/ap-prod-performance-infer",
        "performance_ingestion_arn": "arn:aws:sagemaker:us-east-1:750606694809:pipeline/ap-prod-performance-ingestion",
    },
}

CONFIG = MAIN_CONFIG[ENVIRONMENT] | COMMON_CONFIG
