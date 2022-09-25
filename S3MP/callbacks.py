"""
S3 callbacks to be used for boto3 transfers (uploads, downloads, and copies).
"""
from pathlib import Path
from S3MP.globals import S3MPConfig
import os
import tqdm

from S3MP.mirror_path import MirrorPath
from S3MP.types import SList


class FileSizeTQDMCallback(tqdm.tqdm):
    """File transfer tracker scaled to the size of the file(s). Multiple files can be tracked at once."""

    def __init__(
        self,
        transfer_objs: SList[Path | str | MirrorPath],
        resource=None,
        bucket_key=None,
        is_download: bool = True,
    ):
        """
        Construct download object and printout total file size.

        :param transfer_mappings: List of files to be transferred.
        :param resource: AWS Resource to access object with.
        :param bucket: Bucket to locate resource within.
        :param is_download: Marker for upload/download transfer.
        """
        if resource is None:
            resource = S3MPConfig.s3_resource
        if bucket_key is None:
            bucket_key = S3MPConfig.default_bucket_key
        if not isinstance(transfer_objs, list):
            transfer_objs = [transfer_objs]
        self._total_bytes = sum(
            MirrorPath.get_transfer_size_bytes(transfer_mapping, is_download)
            for transfer_mapping in transfer_objs
        )

        transfer_str = "Download" if is_download else "Upload"
        super().__init__(
            self,
            total=self._total_bytes,
            unit="B",
            unit_scale=True,
            desc=f"{transfer_str} progress",
        )
        self._transfer_objs = transfer_objs

    def __enter__(self):
        """Enter context, set self as global callback."""
        S3MPConfig.callback = self
        return self

    def __call__(self, bytes_progress):
        """
        Update tracking and progress bar accordingly.

        :param bytes_progress: Number of bytes downloaded since last call.
        """
        self.update(bytes_progress)
