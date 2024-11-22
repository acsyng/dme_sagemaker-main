FROM 173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py39-v1.0

RUN pip3 install pandas==2.2 matplotlib==3.5.2
RUN pip3 install psycopg2-binary==2.9.9
RUN pip3 install scikit-learn==1.4.2 statsmodels==0.13.2 scipy==1.7.3
RUN pip3 install symbulate==0.5.7 pyspark==3.5.0 seaborn==0.11.2
RUN pip3 install tenacity==8.0.1 watchtower==3.0.1 SQLAlchemy==2.0.23
RUN pip3 install xgboost==2.0.2 pyarrow==16.1.0
RUN pip3 install asyncio==3.4.3 nest-asyncio==1.5.8 aiohttp==3.9.5
RUN pip3 install boto3==1.34.131 aiobotocore==2.13.1
RUN pip3 install shap==0.43.0
RUN pip3 install fsspec==2023.12.1 fastparquet==2024.5.0
RUN pip3 install s3fs==2024.6.1 scikit-optimize==0.9.0

RUN pip3 install lightgbm==4.1.0 arm-mango==1.4.0 fasttreeshap pynndescent polars==1.3.0

RUN pip3 install redshift_connector snowflake-connector-python

RUN pip3 install pareto

ENTRYPOINT ["smspark-submit"]
