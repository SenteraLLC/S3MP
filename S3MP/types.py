"""Types, mostly from mypy_boto3 (boto3-stubs)."""
from pathlib import Path
from typing import List, TypeVar, Union

from mypy_boto3_s3 import S3Client
from mypy_boto3_s3 import S3ServiceResource as S3Resource
from mypy_boto3_s3.service_resource import Bucket as S3Bucket
from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef as S3ListObjectV2Output
from s3transfer.manager import TransferConfig as S3TransferConfig

T = TypeVar('T')
SList = Union[List[T], T]
PathSList = SList[Path]
StrSSlist = SList[str]