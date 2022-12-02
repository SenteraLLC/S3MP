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

    def __init__(self, s3_key: str, local_path: Path, **kwargs):
        """Init."""
        self._mirror_root = kwargs.get("_mirror_root", get_env_mirror_root())
        self.s3_key = s3_key
        self.local_path = local_path

        # Configurable members
        self.s3_bucket_key = kwargs.get("s3_bucket_key")
        self.s3_resource = kwargs.get("s3_resource")
        self.s3_client = kwargs.get("s3_client")
        self.s3_bucket = kwargs.get("s3_bucket")
    
    def _get_env_dict(self) -> Dict:
        """Get dictionary detailing S3 environment, usually for constructing a relative MP."""
        return { 
            "_mirror_root": self._mirror_root,
            "s3_bucket_key": self._get_bucket_key(),
            "s3_resource": self._get_resource(),
            "s3_client": self._get_client(),
            "s3_bucket": self._get_bucket(),
        }
    
    def _get_bucket_key(self) -> str:
        """Get s3 bucket key, create from defaults if not present."""
        if self.s3_bucket_key is None:
            self.s3_bucket_key = S3MPConfig.default_bucket_key
        return self.s3_bucket_key
    
    def _get_resource(self) -> S3Resource:
        """Get s3 resource, create from defaults if not present."""
        if self.s3_resource is None:
            self.s3_resource = S3MPConfig.s3_resource
        return self.s3_resource
    
    def _get_client(self) -> S3Client:
        """Get s3 client."""
        if self.s3_client is None:
            self.s3_client = S3MPConfig.s3_client
        return self.s3_client

    def _get_bucket(self) -> S3Bucket:
        """Get bucket, create from defaults if not present."""
        if self.s3_bucket is None:
            s3_resource = self._get_resource()
            bucket_key = self._get_bucket_key()
            self.s3_bucket = s3_resource.Bucket(bucket_key)
        return self.s3_bucket
    
    def _guarantee_trailing_slash(self):
        """Guarantee trailing slash."""
        if self.s3_key[-1] != "/":
            self.s3_key += "/"

    @staticmethod
    def from_s3_key(s3_key: str, **kwargs: Dict) -> "MirrorPath":
        """Create a MirrorPath from an s3 key."""
        mirror_root = get_env_mirror_root()
        local_path = mirror_root / s3_key
        return MirrorPath(s3_key, local_path, **kwargs)

    @staticmethod
    def from_local_path(local_path: Path, **kwargs: Dict) -> "MirrorPath":
        """Create a MirrorPath from a local path."""
        mirror_root = get_env_mirror_root()
        s3_key = local_path.relative_to(mirror_root).as_posix()
        return MirrorPath(s3_key, local_path, **kwargs)

    def exists_in_mirror(self) -> bool:
        """Check if file exists in mirror."""
        return self.local_path.exists()

    def exists_on_s3(self) -> bool:
        """Check if file exists on S3."""
        bucket = self._get_bucket()
        results = bucket.objects.filter(Prefix=self.s3_key)
        return len(list(results)) > 0
    
    def is_file_on_s3(self) -> bool:
        """Check if is a file on s3."""
        bucket = self._get_bucket()
        s3_obj = bucket.Object(self.s3_key)

        try:
            return s3_obj.content_type != "application/x-directory"
        except botocore.exceptions.ClientError as e:
            if self.exists_on_s3():
                return False 
            else:
                # We should really never hit this point.
                raise FileNotFoundError(f"File {self.s3_key} not found on S3.") from e

    def download_to_mirror(self, overwrite: bool = False):
        """Download S3 file to mirror."""
        if not overwrite and self.exists_in_mirror():
            self.update_current_callback_on_skipped_transfer(True)
            return
        local_folder = self.local_path.parent
        local_folder.mkdir(parents=True, exist_ok=True)

        bucket = self._get_bucket()
        if self.is_file_on_s3():
            bucket.download_file(
                self.s3_key,
                str(self.local_path),
                Callback=S3MPConfig.callback,
                Config=S3MPConfig.transfer_config,
            )
        else:  # Folder, so download all.
            objects = bucket.objects.filter(Prefix=self.s3_key)
            for obj in objects:
                obj_mp = MirrorPath.from_s3_key(obj.key)
                obj_mp.download_to_mirror(overwrite=overwrite)


    def download_to_mirror_if_not_present(self):
        """Download to mirror if not present in mirror."""
        self.download_to_mirror(overwrite=False)

    def upload_from_mirror(self, overwrite: bool = False):
        """Upload local file to S3."""
        if not overwrite and self.exists_on_s3():
            self.update_current_callback_on_skipped_transfer(False)
            return
        bucket = self._get_bucket()
        bucket.upload_file(
            str(self.local_path),
            self.s3_key,
            Callback=S3MPConfig.callback,
            Config=S3MPConfig.transfer_config,
        )

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
    
    def delete_self_on_s3(self):
        """Delete self on s3."""
        bucket = self._get_bucket()
        s3_obj = bucket.Object(key=self.s3_key)
        s3_obj.delete()
    
    def delete_children_on_s3(self):
        """Delete all children on s3."""
        bucket = self._get_bucket()
        ## TODO centralized trailing slashes and the like.
        # if not self.is_file_on_s3() and self.s3_key[-1] != "/":
        #     self.s3_key += "/"
        bucket.objects.filter(Prefix=self.s3_key).delete()
    
    def delete_local(self):
        """Delete local file."""
        if not self.local_path.exists():
            return 
        if self.local_path.is_dir():
            for path in self.local_path.iterdir():
                path.unlink()
            self.local_path.rmdir()
        else:
            self.local_path.unlink()
    
    def delete_s3(self):
        """Delete s3 file."""
        if not self.exists_on_s3():
            return
        if self.is_file_on_s3():
            self.delete_self_on_s3()
        else:
            self.delete_children_on_s3()
    
    def delete_all(self):
        """Delete all files."""
        self.delete_local()
        self.delete_s3()

    def load_local(self, download: bool = True, load_fn: Callable = None, overwrite: bool = False):
        """
        Load local file, infer file type and load.
        Setting download to false will still download if the file is not present.
        """
        if download or not self.exists_in_mirror():
            self.download_to_mirror(overwrite)
        if load_fn is None:
            match (self.local_path.suffix):
                case ".json":

                    def _load_fn(path):
                        with open(path, "r") as fd:
                            return json.load(fd)

                    load_fn = _load_fn
                case ".npy":
                    load_fn = np.load
                case ".jpg" | ".jpeg" | ".png":
                    load_fn = cv2.imread

        data = load_fn(str(self.local_path))
        return data

    def save_local(self, data, upload: bool = True, save_fn: Callable = None, overwrite: bool = False):
        """Save local file, infer file type and upload."""
        if not self.local_path.parent.exists():
            self.local_path.parent.mkdir(parents=True)
        if save_fn is None:
            match (self.local_path.suffix):
                case ".json":
                    def _save_fn(_data):
                        with open(str(self.local_path), "w") as fd:
                            json.dump(_data, fd)

                    save_fn = _save_fn
                case ".npy":
                    save_fn = lambda _data: np.save(str(self.local_path), _data)
                case ".jpg" | ".jpeg" | ".png":
                    def _save_fn(_data):
                        cv2.imwrite(str(self.local_path), _data)
                    save_fn = _save_fn
        save_fn(data)
        if upload:
            self.upload_from_mirror(overwrite)

    @staticmethod
    def get_s3_key_size_bytes(s3_key: str) -> int:
        """Get the size of an S3 key in bytes."""
        s3_resource = S3MPConfig.s3_resource
        bucket_key = S3MPConfig.default_bucket_key
        return s3_resource.Object(bucket_key, s3_key).content_length

    @staticmethod
    def get_local_file_size_bytes(local_path: Path) -> int:
        """Get the size of a local file in bytes."""
        return local_path.stat().st_size

    @staticmethod
    def get_transfer_size_bytes(transfer_obj, is_download: bool):
        """Get the size of a transfer in bytes."""
        if isinstance(transfer_obj, MirrorPath):
            return transfer_obj.get_size_bytes(is_download)
        if is_download:
            MirrorPath.get_s3_key_size_bytes(transfer_obj)
        return MirrorPath.get_local_file_size_bytes(transfer_obj)

    def get_size_bytes(self, on_s3: bool = True) -> int:
        """Get size of self in bytes."""
        if on_s3 and self.exists_on_s3():
            return MirrorPath.get_s3_key_size_bytes(self.s3_key)
        elif self.exists_in_mirror():
            return MirrorPath.get_local_file_size_bytes(self.local_path)
        raise FileNotFoundError(f"File {self} not found.")

    def update_current_callback_on_skipped_transfer(self, is_download: bool):
        """Update the current callback if a transfer gets skipped."""
        if S3MPConfig.callback and self in S3MPConfig.callback._transfer_objs:
            S3MPConfig.callback(self.get_size_bytes(is_download))

    def __repr__(self):
        """Repr."""
        return f"{self.__class__.__name__}({self.s3_key}, {self.local_path}, {self.s3_bucket_key})"

# TODO find better spot for this.
def get_matching_s3_mirror_paths(segments: List[KeySegment]) -> List[MirrorPath]:
    """Case get_matching_s3_keys to MirrorPath."""
    return [MirrorPath.from_s3_key(key) for key in get_matching_s3_keys(segments)]