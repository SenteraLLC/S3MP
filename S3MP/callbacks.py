"""
S3 callbacks to be used for boto3 transfers (uploads, downloads, and copies).
"""
from S3MP.globals import S3MPConfig
import os
from typing import Union, List
import tqdm


class FileSizeTQDMCallback(tqdm.tqdm):
    """File transfer tracker scaled to the size of the file(s). Multiple files can be tracked at once."""

    def __init__(
        self,
        path_or_key_list: Union[List[str], str],
        resource=None,
        bucket=None,
        is_download: bool = True,
    ):
        """
        Construct download object and printout total file size.

        :param path_or_key_list: Path or list of paths to files to be downloaded.
        :param resource: AWS Resource to access object with.
        :param bucket: Bucket to locate resource within.
        :param is_download: Marker for upload/download transfer.
        """
        if resource is None:
            resource = S3MPConfig.s3_resource
        if bucket is None:
            bucket = S3MPConfig.default_bucket
        if not isinstance(path_or_key_list, list):
            path_or_key_list = [path_or_key_list]

        if is_download:
            self._total_bytes = sum(
                resource.Object(bucket, key).content_length for key in path_or_key_list
            )
        else:  # Upload
            self._total_bytes = sum(os.path.getsize(path) for path in path_or_key_list)
        
        transfer_str = "Download" if is_download else "Upload"
        super().__init__(self, total=self._total_bytes, unit="B", unit_scale=True, desc=f"{transfer_str} progress")

    def __call__(self, bytes_progress):
        """
        Update tracking and progress bar accordingly.

        :param bytes_progress: Number of bytes downloaded since last call.
        """
        self.update(bytes_progress)