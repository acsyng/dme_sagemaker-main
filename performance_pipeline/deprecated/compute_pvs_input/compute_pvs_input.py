#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sagemaker
from sagemaker.processing import ProcessingInput, ProcessingOutput, NetworkConfig
from sagemaker.sklearn.processing import SKLearnProcessor

bucket = "us.com.syngenta.ap.nonprod"


sagemaker_session = sagemaker.Session(default_bucket=bucket)
role = sagemaker.get_execution_role()


# In[3]:


network_config = NetworkConfig(enable_network_isolation=False,
                               subnets = ["subnet-0327a8fda4133f6a9","subnet-0a9f652afd14455bb"],
                               security_group_ids = ["sg-00634d30526efcdec","sg-0405f346fdd704d24"])


# In[4]:


processor = SKLearnProcessor(framework_version='1.0-1',
                              base_job_name="ap-nonrod-compute-pvs-input",
                              sagemaker_session=sagemaker_session,
                              role=role,
                              instance_count=1,
                              instance_type="ml.t3.large",
                              max_runtime_in_seconds=1200,
                              network_config = network_config,
                              output_kms_key = "arn:aws:kms:us-east-1:809800841141:key/353d6263-d454-444f-ac60-41afe025b445",
                              tags = [{'Key': 'Application', 'Value': 'Advancement placement'},{'Key': 'Cost Center', 'Value': 'RDIT3000'}]
)

inputs = [
    # Mount the src dir with your code here.
    ProcessingInput(
        source="./src/libs",
        destination="/opt/ml/processing/input/code/libs"
    ),
]

outputs = [
    # Mount the src dir with your code here.
    ProcessingOutput(
        destination="s3://us.com.syngenta.ap.nonprod/poc/compute_pvs_input/data",
        source="/opt/ml/processing/data/"
    ),
]


# In[5]:


query_variables = """{
  "stages": [
    "5"
  ],
  "analysis_run_group": "phenohybrid1yr",
  "analysis_year": "2023",
  "pipeline_runid": "20230213_12_03_01",
  "ap_data_sector": "CORN_NA_SUMMER",
  "current_analysis_year": "2021",
  "analysis_type": "SingleExp",
  "current_source_ids": "('21SUWEYG55V','21SUABYG601', '21SUABYG611', '21SUABYG621', '21SUABYG631', '21SUABYG641', '21SUABYG661', '21SUABYG671', '21SUABYG6DS')",
  "breakout_level": "tpp_region"
}"""


# In[ ]:


import json
data = json.loads(query_variables)


# In[ ]:


processor.run(
    code="./src/chk_data_for_ingestion.py",
    inputs=inputs,
    outputs=outputs,
    arguments=["--ap_data_sector", data['ap_data_sector'],
              "--current_analysis_year", data['current_analysis_year'],
              "--analysis_type", data['analysis_type'],
              "--current_source_ids", data['current_source_ids'],
              "--pipeline_runid", data['pipeline_runid'],
              ],
    kms_key = "arn:aws:kms:us-east-1:809800841141:key/353d6263-d454-444f-ac60-41afe025b445",
)


# In[ ]:




