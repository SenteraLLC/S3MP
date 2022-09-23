"""
S3 callbacks to be used for boto3 transfers (uploads, downloads, and copies).
"""
from S3MP.globals import S3MPGlobals
import os
from typing import Union, List
from tqdm import tqdm


def byte_sizeof_fmt(num: float, suffix: str = "B"):
    """
    Format byte size to human-readable format.
    https://web.archive.org/web/20111010015624/http://blogmag.net/blog/read/38/Print_human_readable_file_size

    :param num: Number of bytes to translate.
    :param suffix: Suffix to display in output string.
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


class FileSizeTQDMCallback(object):
    """File transfer tracker scaled to the size of the file(s). Multiple files can be tracked at once."""

    def __init__(
        self, path_or_key_list: Union[List[str], str], resource=None, bucket=None, is_download: bool = True
    ):
        """
        Construct download object and printout total file size.

        :param resource: AWS Resource to access object with.
        :param bucket: Bucket to locate resource within.
        :param filename: File to track from bucket.
        """
        if resource is None:
            resource = S3MPGlobals.s3_resource
        if bucket is None:
            bucket = S3MPGlobals.default_bucket
        if not isinstance(path_or_key_list, list):
            path_or_key_list = [path_or_key_list]

        if is_download:
            self._total_bytes = sum(
                resource.Object(bucket, key).content_length for key in path_or_key_list
            )
        else:  # Upload
            self._total_bytes = sum(os.path.getsize(key) for key in path_or_key_list)

        transfer_type_str = "download" if is_download else "upload"

        print(
            f"Starting {transfer_type_str} of size {byte_sizeof_fmt(self._total_bytes)}."
        )

        self._bytes_seen_so_far = 0
        self.pbar = tqdm(total=100)
        self._most_recent_percent = 0

    def __enter__(self):
        """
        Enter context manager.
        """
        return self

    def __exit__(self, *_):
        """
        Exit context manager.
        """
        self.pbar.close()

    def __call__(self, bytes_progress):
        """
        Update tracking and progress bar accordingly.

        :param bytes_progress: Number of bytes downloaded since last call.
        """
        self._bytes_seen_so_far += bytes_progress
        percentage = round((self._bytes_seen_so_far / self._total_bytes) * 100)
        percentage = min(percentage, 100)
        if percentage > self._most_recent_percent:
            self.pbar.update(percentage - self._most_recent_percent)
            self._most_recent_percent = percentage
