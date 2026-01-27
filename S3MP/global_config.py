"""Set global values for S3MP module."""

import tempfile
from collections.abc import Callable
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path

import boto3

from S3MP.types import S3Bucket, S3Client, S3Resource, S3TransferConfig


def get_config_file_path() -> Path:
    """Get the location of the config file."""
    root_module_folder = Path(__file__).parent.resolve()
    return root_module_folder / "config.ini"


class Singleton(type):
    # Singleton metaclass
    _instances: dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass
class _S3MPConfigClass(metaclass=Singleton):
    """Singleton class for S3MP globals."""

    # Boto3 Objects
    _s3_client: S3Client | None = None
    _s3_resource: S3Resource | None = None
    _bucket: S3Bucket | None = None

    # Config Items
    _default_bucket_key: str | None = None
    _mirror_root: Path | None = None

    # Other Items
    transfer_config: S3TransferConfig | None = None
    callback: Callable | None = None
    use_async_global_thread_queue: bool = True

    @property
    def default_bucket_key(self) -> str:
        """Get default bucket key."""
        if self._default_bucket_key is None:
            raise ValueError(
                "No default bucket key set. Use the 'set_default_bucket_key' method or set `default_bucket_key` in the config."
            )
        return self._default_bucket_key

    def set_default_bucket_key(self, bucket_key: str) -> None:
        """Set default bucket key."""
        self._default_bucket_key = bucket_key

    @property
    def s3_client(self) -> S3Client:
        """Get S3 client."""
        if not self._s3_client:
            self._s3_client = boto3.client("s3")
        return self._s3_client

    @property
    def s3_resource(self) -> S3Resource:
        """Get S3 resource."""
        if not self._s3_resource:
            self._s3_resource = boto3.resource("s3")
        return self._s3_resource

    def get_bucket(self, bucket_key: str | None = None) -> S3Bucket:
        """Get bucket."""
        if bucket_key:
            return self.s3_resource.Bucket(bucket_key)
        elif self._bucket is None:
            self._bucket = self.s3_resource.Bucket(self.default_bucket_key)
        return self._bucket

    @property
    def bucket(self) -> S3Bucket:
        """Get bucket using default key."""
        return self.get_bucket()

    @property
    def mirror_root(self) -> Path:
        """Get mirror root."""
        if self._mirror_root is None:
            print("Mirror Root not set, a temporary directory will be used as the mirror root.")
            self._mirror_root = Path(tempfile.gettempdir())
        return self._mirror_root

    def load_config(self, config_file_path: Path | None = None):
        """Load the config file."""
        config_file_path = config_file_path or get_config_file_path()
        config = ConfigParser()
        config.read(config_file_path)

        if "DEFAULT" not in config:
            return

        if "default_bucket_key" in config["DEFAULT"]:
            self.set_default_bucket_key(config["DEFAULT"]["default_bucket_key"])

        if "mirror_root" in config["DEFAULT"]:
            self._mirror_root = Path(config["DEFAULT"]["mirror_root"])

    def save_config(self, config_file_path: Path | None = None):
        """Write config file."""
        config_file_path = config_file_path or get_config_file_path()
        config = ConfigParser()
        config["DEFAULT"] = {}
        config["DEFAULT"]["default_bucket_key"] = self.default_bucket_key
        if self._mirror_root:
            config["DEFAULT"]["mirror_root"] = str(self._mirror_root)
        with open(config_file_path, "w") as configfile:
            config.write(configfile)


# Create the singleton instance and export it as S3MPConfig
S3MPConfig = _S3MPConfigClass()
S3MPConfig.load_config()
