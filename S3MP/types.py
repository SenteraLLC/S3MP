"""Types, mostly from mypy_boto3 (boto3-stubs)."""

from pathlib import Path
from typing import TypeAlias

from mypy_boto3_s3 import S3Client, S3ServiceResource
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef
from s3transfer.manager import TransferConfig

S3Resource = S3ServiceResource
S3Bucket = Bucket
S3TransferConfig = TransferConfig
S3ListObjectV2Output = ListObjectsV2OutputTypeDef

PathSList: TypeAlias = list[Path] | Path
StrSSlist: TypeAlias = list[str] | str

__all__ = [
    "S3Client",
    "S3Resource",
    "S3Bucket",
    "S3TransferConfig",
    "S3ListObjectV2Output",
    "PathSList",
    "StrSSlist",
]
