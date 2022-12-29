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
    _s3_client: S3Client = None
    _s3_resource: S3Resource = None
    _bucket: S3Bucket = None

    default_bucket_key: str = None
    mirror_root: Path = None
    transfer_config: S3TransferConfig = None
    callback: Callable = None
    use_async_global_thread_queue: bool = True

    @property
    def s3_client(self) -> S3Client:
        """Get S3 client."""
        if not self._s3_client:
            self._s3_client = boto3.client("s3")
        return self._s3_client
    
    @property
    def s3_resource(self) -> S3Resource:
        """Get S3 resource."""
        if not self._s3_resource:
            self._s3_resource = boto3.resource("s3")
        return self._s3_resource
    
    @property
    def bucket(self, bucket_key: str = None) -> S3Bucket:
        """Get bucket."""
        if bucket_key:
            return self.s3_resource.Bucket(bucket_key)
        elif self._bucket is None:
            if self.default_bucket_key is None:
                raise ValueError("No default bucket key set.")
            self._bucket = self.s3_resource.Bucket(self.default_bucket_key)
        return self._bucket


S3MPConfig = S3MPConfig() 
