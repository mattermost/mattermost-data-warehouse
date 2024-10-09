import os

import boto3
import pytest

# Safeguard for moto tests
os.environ['AWS_ACCESS_KEY_ID'] = 'test-key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test-secret'


@pytest.fixture
def s3_boto():
    """Create an S3 boto3 client and return the client object"""
    return boto3.client('s3', region_name='us-east-1')
