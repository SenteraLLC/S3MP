"""Set global values for S3MP module."""
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
import boto3 
from mypy_boto3_s3 import S3Client, S3ServiceResource
from boto3.s3.transfer import TransferConfig as S3TransferConfig


class Singleton(type):
    # Singleton metaclass 
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

@dataclass
class S3MPGlobals(metaclass=Singleton):
    """Singleton class for S3MP globals."""
    s3_client: S3Client = boto3.client("s3")
    s3_resource: S3ServiceResource = boto3.resource("s3")
    mirror_root: Path = None 
    default_bucket: str = None
    transfer_config: S3TransferConfig = S3TransferConfig(
        multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=True
    )
    callback: Callable = None