"""Set global values for S3MP module."""
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
import boto3 
from S3MP.types import S3Client, S3Resource, S3Bucket, S3TransferConfig


class Singleton(type):
    # Singleton metaclass 
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

@dataclass
class S3MPConfig(metaclass=Singleton):
    """Singleton class for S3MP globals."""
    s3_client: S3Client = boto3.client("s3")
    s3_resource: S3Resource = boto3.resource("s3")
    mirror_root: Path = None 
    default_bucket_key: str = None
    default_bucket: S3Bucket = None 
    transfer_config: S3TransferConfig = None
    callback: Callable = None
    use_async_global_thread_queue: bool = True

    def get_bucket(self, bucket_key: str = None) -> S3Bucket:
        if bucket_key:
            return self.s3_resource.Bucket(bucket_key)
        elif self.default_bucket is None:
            if self.default_bucket_key is None:
                raise ValueError("No default bucket key set.")
            self.default_bucket = self.s3_resource.Bucket(self.default_bucket_key)
        return self.default_bucket

S3MPConfig = S3MPConfig() 
