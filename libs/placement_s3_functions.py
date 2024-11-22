import boto3
import os
from libs.config.config_vars import S3_BUCKET


def check_if_file_exists_s3(fpath, bucket=S3_BUCKET):
    s3_client = boto3.client('s3')
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=fpath, MaxKeys=1)
    return 'Contents' in res


def get_s3_prefix(env):
    return os.path.join(env, 'dme', 'placement')

