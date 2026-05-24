import boto3
from botocore.client import Config
from botocore import UNSIGNED


def create_s3_client(aws_region):
    return boto3.client('s3', region_name=aws_region, config=Config(signature_version=UNSIGNED))