"""Utilities for working with S3."""
from S3MP.global_config import S3MPConfig
from S3MP.types import S3Bucket, S3Client, S3ListObjectV2Output
from botocore.exceptions import ClientError

def s3_list_single_key(
    key: str, bucket: S3Bucket = None, client: S3Client = None
) -> S3ListObjectV2Output:
    """List details of a single key on S3."""
    if bucket is None:
        bucket = S3MPConfig.bucket
    if client is None:
        client = S3MPConfig.s3_client
    
    return client.list_objects_v2(
        Bucket=bucket.name, Prefix=key, Delimiter="/", MaxKeys=1
    )

def key_exists_on_s3(
    key: str, bucket: S3Bucket = None, client: S3Client = None
) -> bool:
    """Check if a key exists on S3."""
    res = s3_list_single_key(key, bucket, client)
    return "Contents" in res or "CommonPrefixes" in res


def key_is_file_on_s3(
    key: str, bucket: S3Bucket = None, client: S3Client = None
) -> bool:
    """Check if a key is a file on S3, returns false if it is a folder. Raises an error if the key does not exist."""
    if not key_exists_on_s3(key, bucket, client):
        raise ValueError("Key does not exist on S3")    
    res = s3_list_single_key(key, bucket, client)
    return "Contents" in res
    

    
