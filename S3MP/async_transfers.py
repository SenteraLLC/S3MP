"""Asynchronous transfer utilities."""
from pathlib import Path
from S3MP.globals import S3Resource, S3MPGlobals, Singleton
import aioboto3
import asyncio
from typing import Coroutine, List

def upload_file_thread(
    local_path: Path,
    s3_key: str,
    bucket_key: str = None,
    callback = None,
    config = None
) -> Coroutine:
    """Upload a file on a separate thread."""
    bucket = S3MPGlobals.get_bucket(bucket_key)
    if callback is None:
        callback = S3MPGlobals.callback
    if config is None:
        config = S3MPGlobals.transfer_config
    return asyncio.to_thread(
        bucket.upload_file, 
        str(local_path),
        s3_key, 
        Callback=callback,
        Config=config
    )

def upload_files_in_threads(
    local_paths: List[Path],
    s3_keys: List[str],
    bucket_key: str = None,
    callback = None,
    config = None
) -> List[Coroutine]:
    """Upload files in threads."""
    return [
        upload_file_thread(
            local_path, 
            s3_key, 
            bucket_key=bucket_key, 
            callback=callback, 
            config=config
        )
        for local_path, s3_key in zip(local_paths, s3_keys)
    ]
