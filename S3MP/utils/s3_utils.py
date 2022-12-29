"""Utilities for working with S3."""
from S3MP.global_config import S3MPConfig
from S3MP.types import S3Bucket, S3Client, S3ListObjectV2Output
from botocore.exceptions import ClientError


def s3_list_single_key(
    key: str,
    bucket: S3Bucket = S3MPConfig.bucket,
    client: S3Client = S3MPConfig.s3_client,
) -> S3ListObjectV2Output:
    """List details of a single key on S3."""
    return client.list_objects_v2(
        Bucket=bucket.name, Prefix=key, Delimiter="/", MaxKeys=1
    )


def key_exists_on_s3(
    key: str, bucket: S3Bucket = None, client: S3Client = None
) -> bool:
    """Check if a key exists on S3."""
    res = s3_list_single_key(key, bucket, client)
    return "Contents" in res or "CommonPrefixes" in res


# @functools.cache(maxsize=1000)
def key_is_file_on_s3(
    key: str, 
    bucket: S3Bucket = S3MPConfig.bucket, 
    client: S3Client = S3MPConfig.s3_client,
) -> bool:
    """Check if a key is a file on S3, returns false if it is a folder. Raises an error if the key does not exist."""
    if not key_exists_on_s3(key, bucket, client):
        raise ValueError("Key does not exist on S3")
    res = s3_list_single_key(key, bucket, client)
    return "Contents" in res


def key_size_on_s3(key: str, bucket: S3Bucket = None, client: S3Client = None) -> int:
    """Get the size of a key on S3. Raises an error if the key does not exist."""
    if not key_exists_on_s3(key, bucket, client):
        raise ValueError("Key does not exist on S3")
    res = s3_list_single_key(key, bucket, client)
    return res["Contents"]["Size"] if "Contents" in res else 0


def delete_child_keys_on_s3(
    key: str,
    bucket: S3Bucket = S3MPConfig.bucket,
    client: S3Client = S3MPConfig.s3_client,
) -> None:
    """Delete all keys that are children of a key on S3."""
    for obj in client.list_objects_v2(Bucket=bucket.name, Prefix=key)["Contents"]:
        client.delete_object(Bucket=bucket.name, Key=obj["Key"])


def delete_key_on_s3(
    key: str,
    bucket: S3Bucket = S3MPConfig.bucket,  # TODO decide if we want to use the global config here like this
    client: S3Client = S3MPConfig.s3_client,
) -> None:
    """Delete a key on S3."""
    if not key_exists_on_s3(key, bucket, client):
        return
    if key_is_file_on_s3(key, bucket, client):
        client.delete_object(Bucket=bucket.name, Key=key)
    else:
        delete_child_keys_on_s3(key, bucket, client)
    