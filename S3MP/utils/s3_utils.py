"""Utilities for working with S3."""
import warnings
from pathlib import Path
from S3MP.global_config import S3MPConfig
from S3MP.types import S3Bucket, S3Client, S3ListObjectV2Output
from typing import List
from botocore.exceptions import ClientError

def s3_list_single_key(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> S3ListObjectV2Output:
    """List details of a single key on S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    return client.list_objects_v2(
        Bucket=bucket.name, Prefix=key, Delimiter="/", MaxKeys=1
    )

def s3_list_child_keys(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> List[str]:
    """List details of all child keys on S3."""
    if not key.endswith("/"):
        warnings.warn(f"Listing child keys of {key} - key does not end with '/'")
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    params = {
        "Bucket": bucket.name,
        "Prefix": key,
        "Delimiter": "/",
    }
    child_s3_keys: List[str] = []
    continuation_token: str = None
    while True:
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        resp = client.list_objects_v2(**params)

        # Collect keys from the current response
        if "Contents" in resp:
            child_s3_keys.extend(
                obj["Key"] for obj in resp["Contents"] if obj["Key"] != key
            )
        if "CommonPrefixes" in resp:
            child_s3_keys.extend(obj["Prefix"] for obj in resp["CommonPrefixes"])

        # Check if there are more pages to fetch
        if "NextContinuationToken" in resp:
            continuation_token = resp["NextContinuationToken"]
        else:
            break

    return child_s3_keys

def download_key(
    key: str,
    local_path: Path,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> None:
    """Download a key from S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    print(f"{key} is file: {key_is_file_on_s3(key, bucket, client)}")
    if key_is_file_on_s3(key, bucket, client):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket.name, key, str(local_path), Callback=S3MPConfig.callback, Config=S3MPConfig.transfer_config)
    else:
        for child_key in s3_list_child_keys(key, bucket, client):
            download_key(child_key, local_path / child_key.replace(key, ""))
    
def upload_to_key(
    key: str,
    local_path: Path,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> None:
    """Upload a file or folder to a key on S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    if local_path.is_file():
        client.upload_file(str(local_path), bucket.name, key, Callback=S3MPConfig.callback, Config=S3MPConfig.transfer_config)
    else:
        for child in local_path.iterdir():
            upload_to_key(f"{key}/{child.name}", child, bucket, client)

def key_exists_on_s3(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> bool:
    """Check if a key exists on S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    res = s3_list_single_key(key, bucket, client)
    return "Contents" in res or "CommonPrefixes" in res


def key_is_file_on_s3(
    key: str, 
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> bool:
    """Check if a key is a file on S3, returns false if it is a folder. Raises an error if the key does not exist."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise ValueError(f"Key {key} does not exist on S3")
        elif e.response['Error']['Code'] == '403':
            raise ValueError(f"Access denied for key {key} on S3")
        else:
            return False


def key_size_on_s3(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
    ) -> int:
    """Get the size of a key on S3. Raises an error if the key does not exist."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    if not key_exists_on_s3(key, bucket, client):
        raise ValueError(f"Key {key} does not exist on S3")
    res = s3_list_single_key(key, bucket, client)
    return res["Contents"]["Size"] if "Contents" in res else 0


def delete_child_keys_on_s3(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> None:
    """Delete all keys that are children of a key on S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    for child_key in s3_list_child_keys(key, bucket, client):
        client.delete_object(Bucket=bucket.name, Key=child_key)


def delete_key_on_s3(
    key: str,
    bucket: S3Bucket = None,
    client: S3Client = None,
) -> None:
    """Delete a key on S3."""
    bucket = bucket or S3MPConfig.bucket
    client = client or S3MPConfig.s3_client
    if not key_exists_on_s3(key, bucket, client):
        return
    if key_is_file_on_s3(key, bucket, client):
        client.delete_object(Bucket=bucket.name, Key=key)
    else:
        delete_child_keys_on_s3(key, bucket, client)
    