from io import BytesIO
from typing import Generator, Tuple

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')


def bucket_exists(bucket: str) -> bool:
    """
    Check if a bucket exists.
    """
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def object_iter(bucket: str, prefix: str) -> Generator[Tuple[str, BytesIO], None, None]:
    """
    Iterates over all items from a bucket with a given prefix, return .

    :bucket: The bucket to search in.
    :prefix: The prefix to search for.
    """
    paginator = s3.get_paginator('list_objects_v2')

    # Iterate
    for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
        # Download each file individually
        for key in result.get('Contents', []):
            yield key['Key'], BytesIO(s3.get_object(Bucket=bucket, Key=key['Key'])["Body"].read())
