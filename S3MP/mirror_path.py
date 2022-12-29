"""S3 Mirror pathing management."""
from __future__ import annotations
import functools
import botocore.exceptions
import cv2
import json
import os
from mypy_boto3_s3 import S3Client
import numpy as np
from typing import Callable, Dict, List, Tuple
from pathlib import Path
from S3MP.global_config import S3MPConfig
from S3MP.keys import (
    KeySegment,
    get_matching_s3_keys,
    replace_key_segments,
    replace_key_segments_at_relative_depth,
)
from S3MP.types import S3Bucket, S3Resource
import tempfile
from S3MP.utils.local_file_utils import (
    DEFAULT_LOAD_LEDGER,
    DEFAULT_SAVE_LEDGER,
    delete_local_path,
)

from S3MP.utils.s3_utils import delete_key_on_s3, download_key, key_exists_on_s3, key_is_file_on_s3, upload_to_key


def get_env_file_path() -> Path:
    """Get the mirror root from .env file."""
    root_module_folder = Path(__file__).parent.parent.resolve()
    env_file = root_module_folder / ".env"
    if not os.path.exists(f"{env_file}"):
        raise FileNotFoundError("No .env file found.")

    return env_file


def set_env_mirror_root(mirror_root: Path) -> None:
    """Set the mirror root in the .env file."""
    env_file = get_env_file_path()
    with open(f"{env_file}", "w") as f:
        f.write(f"MIRROR_ROOT={mirror_root}")


def get_env_mirror_root() -> Path:
    """Get the mirror root from .env file."""
    if S3MPConfig.mirror_root is not None:
        return Path(S3MPConfig.mirror_root)
    env_file = get_env_file_path()
    with open(f"{env_file}", "r") as f:
        mirror_root = f.read().strip().replace("MIRROR_ROOT=", "")

    return Path(mirror_root)


class MirrorPath:
    """A path representing an S3 file and its local mirror."""

    def __init__(
        self, 
        key_segments: List[KeySegment],
        mirror_root: Path = None,
    ):
        """Init."""
        self.key_segments = key_segments

        if mirror_root is None:
            mirror_root = get_env_mirror_root()
    
    @property
    def s3_key(self) -> str:
        """Get s3 key."""
        ret_key = "/".join([str(s) for s in self.key_segments])
        # We'll infer folder/file based on extension
        return ret_key if '.' in self.key_segments[-1] else f"{ret_key}/"
    
    @property
    def local_path(self) -> Path:
        """Get local path."""
        return Path(S3MPConfig.mirror_root) / self.s3_key

    @staticmethod
    def from_s3_key(s3_key: str, **kwargs: Dict) -> MirrorPath:
        """Create a MirrorPath from an s3 key."""
        key_segments = [KeySegment(idx, s) for idx, s in enumerate(s3_key.split('/'))]
        return MirrorPath(key_segments, **kwargs)
    
    @staticmethod
    def from_local_path(local_path: Path, mirror_root: Path = None, **kwargs: Dict) -> MirrorPath:
        """Create a MirrorPath from a local path."""
        if not mirror_root:
            mirror_root = get_env_mirror_root()
        s3_key = local_path.relative_to(mirror_root).as_posix()
        return MirrorPath.from_s3_key(s3_key, **kwargs)
    
    def __copy__(self):
        """Copy."""
        return MirrorPath( 
            [seg.__copy__() for seg in self.key_segments],
            **self.__dict__
        )

    def __repr__(self):
        """Class representation."""
        return f"{self.__class__.__name__}({self.s3_key})"
    

    def exists_in_mirror(self) -> bool:
        """Check if file exists in mirror."""
        return self.local_path.exists()

    def exists_on_s3(self) -> bool:
        """Check if file exists on S3."""
        return key_exists_on_s3(self.s3_key)

    def is_file_on_s3(self) -> bool:
        """Check if is a file on s3."""
        return key_is_file_on_s3(self.s3_key)

    def download_to_mirror(self, overwrite: bool = False):
        """Download S3 file to mirror."""
        if not overwrite and self.exists_in_mirror():
            return
        download_key(self.s3_key, self.local_path)

    def download_to_mirror_if_not_present(self):
        """Download to mirror if not present in mirror."""
        self.download_to_mirror(overwrite=False)

    def upload_from_mirror(self, overwrite: bool = False):
        """Upload local file to S3."""
        if not overwrite and self.exists_on_s3():
            return
        upload_to_key(self.s3_key, self.local_path)

    def upload_from_mirror_if_not_present(self):
        """Upload from mirror if not present on S3."""
        self.upload_from_mirror(overwrite=False)

    def trim(self, max_depth) -> MirrorPath:
        """Trim key from s3 key."""
        segments = self.s3_key.split("/")
        if len(segments) > max_depth:
            segments = segments[:max_depth]
        trimmed_key = "/".join(segments)
        return MirrorPath.from_s3_key(trimmed_key, **self._get_env_dict())

    def get_key_segment(self, index: int) -> str:
        """Get key segment."""
        segments = self.s3_key.split("/")
        return segments[index]

    def replace_key_segments(self, segments: List[KeySegment]) -> MirrorPath:
        """Replace key segments."""
        # TODO decide if this is the best way to handle this.
        new_key = replace_key_segments(self.s3_key, segments)
        return MirrorPath.from_s3_key(new_key, **self._get_env_dict())

    def replace_key_segments_at_relative_depth(
        self, segments: List[KeySegment]
    ) -> MirrorPath:
        """Replace key segments at relative depth."""
        new_key = replace_key_segments_at_relative_depth(self.s3_key, segments)
        return MirrorPath.from_s3_key(new_key, **self._get_env_dict())

    def get_sibling(self, sibling_name: str) -> MirrorPath:
        """Get a file with the same parent as this file."""
        return self.replace_key_segments_at_relative_depth(
            [KeySegment(0, sibling_name)]
        )

    def get_child(self, child_name: str) -> MirrorPath:
        """Get a file with the same parent as this file."""
        return self.replace_key_segments_at_relative_depth([KeySegment(1, child_name)])

    def get_children_on_s3(self) -> List[MirrorPath]:
        """Get all children on s3."""
        self._guarantee_trailing_slash()
        bucket = self._get_bucket()
        objects = bucket.objects.filter(Prefix=self.s3_key, Delimiter="/")
        return [MirrorPath.from_s3_key(obj.key) for obj in objects]

    def get_parent(self) -> MirrorPath:
        """Get the parent of this file."""
        stripped_key = "/".join([seg for seg in self.s3_key.split("/") if seg][:-1])
        return MirrorPath.from_s3_key(stripped_key)

    def delete_local(self):
        """Delete local file."""
        delete_local_path(self.local_path)

    def delete_s3(self):
        """Delete s3 file."""
        delete_key_on_s3(self.s3_key, self._get_bucket(), self._get_client())

    def delete_all(self):
        """Delete all files."""
        self.delete_local()
        self.delete_s3()

    def load_local(
        self, download: bool = True, load_fn: Callable = None, overwrite: bool = False
    ):
        """
        Load local file, infer file type and load.
        Setting download to false will still download if the file is not present.
        """
        if download or overwrite or not self.exists_in_mirror():
            self.download_to_mirror(overwrite)
        if load_fn is None:
            suffix = self.local_path.suffix[1:].lower()
            load_fn = DEFAULT_LOAD_LEDGER[suffix]

        return load_fn(str(self.local_path))

    def save_local(
        self,
        data,
        upload: bool = True,
        save_fn: Callable = None,
        overwrite: bool = False,
    ):
        """Save local file, infer file type and upload."""
        if not self.local_path.parent.exists():
            self.local_path.parent.mkdir(parents=True)
        if save_fn is None:
            suffix = self.local_path.suffix[1:].lower()
            save_fn = DEFAULT_SAVE_LEDGER[suffix]

        save_fn(data)
        if upload:
            self.upload_from_mirror(overwrite)



# # TODO find better spot for this.
# def get_matching_s3_mirror_paths(segments: List[KeySegment]) -> List[MirrorPath]:
#     """Case get_matching_s3_keys to MirrorPath."""
#     return [MirrorPath.from_s3_key(key) for key in get_matching_s3_keys(segments)]
