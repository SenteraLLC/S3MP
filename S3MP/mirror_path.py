"""S3 Mirror pathing management."""
import os
from typing import Dict
import boto3
from pathlib import Path
from S3MP.globals import S3MPGlobals


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
    if S3MPGlobals.mirror_root is not None:
        return S3MPGlobals.mirror_root
    env_file = get_env_file_path()
    with open(f"{env_file}", "r") as f:
        mirror_root = f.read().strip().replace("MIRROR_ROOT=", "")

    return Path(mirror_root)


class MirrorPath:
    """A path representing an S3 file and it's local mirror."""

    def __init__(
        self, s3_key: str, local_path: Path, s3_bucket: str = S3MPGlobals.default_bucket
    ):
        """Init."""
        self._mirror_root = get_env_mirror_root()
        self.s3_key = s3_key
        self.local_path = local_path
        self.s3_bucket = s3_bucket

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
        s3_client = S3MPGlobals.s3_client
        results = s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_key)
        return "Contents" in results

    def download_to_mirror(self, overwrite: bool = False):
        """Download S3 file to mirror."""
        local_folder = self.local_path.parent
        local_folder.mkdir(parents=True, exist_ok=True)

        # TODO move downloads to more central spot
        s3_resource = S3MPGlobals.s3_resource
        bucket = s3_resource.Bucket(self.s3_bucket)
        if not overwrite and self.exists_in_mirror():
            return
        bucket.download_file(
            self.s3_key,
            self.local_path,
            Callback=S3MPGlobals.callback,
            Config=S3MPGlobals.transfer_config,
        )

    def upload_from_mirror(self):
        """Upload local file to S3."""
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(self.s3_bucket)
        # TODO put configs in a more central spot
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=20,
            multipart_chunksize=1024 * 25,
            use_threads=True,
        )
        bucket.upload_file(
            self.local_path,
            self.s3_key,
            Callback=S3MPGlobals.callback,
            Config=transfer_config,
        )
