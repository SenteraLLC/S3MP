"""Set global values for S3MP module."""

import tempfile
from collections.abc import Callable
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from sys import platform

import boto3
from botocore.config import Config

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
    _boto3_config: Config | None = None

    # Config Items
    _default_bucket_key: str | None = None
    _mirror_root: Path | None = None
    _iam_role_arn: str | None = None
    _max_pool_connections: int | None = None

    # Other Items
    transfer_config: S3TransferConfig | None = None
    callback: Callable | None = None
    use_async_global_thread_queue: bool = True

    def assume_role(self, role_arn: str) -> None:
        """Assume an IAM role and update the S3 client and resource with the new credentials."""
        sts_client = boto3.client("sts")
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="S3MPAssumeRoleSession"
        )
        credentials = assumed_role["Credentials"]

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            config=self.boto3_config,
        )
        self._s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            config=self.boto3_config,
        )
        self._iam_role_arn = role_arn

        # Clear cached bucket
        self._bucket = None

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
        # Clear cached bucket
        self._bucket = None

    def clear_boto3_cache(self) -> None:
        """Clear cached boto3 client and resource."""
        self._s3_client = None
        self._s3_resource = None
        self._bucket = None
        self._boto3_config = None

    @property
    def max_pool_connections(self) -> int | None:
        """Get max pool connections."""
        return self._max_pool_connections

    def set_max_pool_connections(self, max_connections: int) -> None:
        """Set max pool connections."""
        self._max_pool_connections = max_connections
        # Clear cached boto3 config and clients to apply new max pool connections
        self.clear_boto3_cache()

    @property
    def boto3_config(self) -> Config:
        """Get boto3 config parameters."""
        if self._boto3_config is None:
            if self._max_pool_connections is not None:
                self._boto3_config = Config(
                    max_pool_connections=self._max_pool_connections,
                )
            else:
                self._boto3_config = Config()
        return self._boto3_config

    @property
    def s3_client(self) -> S3Client:
        """Get S3 client."""
        if not self._s3_client and self._iam_role_arn:
            self.assume_role(self._iam_role_arn)

        if not self._s3_client:
            self._s3_client = boto3.client("s3", config=self.boto3_config)

        return self._s3_client

    @property
    def s3_resource(self) -> S3Resource:
        """Get S3 resource."""
        if not self._s3_resource and self._iam_role_arn:
            self.assume_role(self._iam_role_arn)

        if not self._s3_resource:
            self._s3_resource = boto3.resource("s3", config=self.boto3_config)

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

    def set_mirror_root(self, mirror_root: Path | str) -> None:
        """Set mirror root. If a relative path is provided, it will be prefixed with the OS-specific root (e.g., C:\\ on Windows or / otherwise)."""
        mirror_path = Path(mirror_root)

        if mirror_path.is_absolute():
            # If it's an absolute path, use it as-is
            self._mirror_root = mirror_path
        else:
            # If it's a relative path, prefix with OS-specific root
            if platform == "win32":
                self._mirror_root = Path(f"C:\\{mirror_path}")
            else:
                self._mirror_root = Path(f"/{mirror_path}")

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

        if "max_pool_connections" in config["DEFAULT"]:
            self.set_max_pool_connections(int(config["DEFAULT"]["max_pool_connections"]))

        if "iam_role_arn" in config["DEFAULT"]:
            self.assume_role(config["DEFAULT"]["iam_role_arn"])

    def save_config(self, config_file_path: Path | None = None):
        """Write config file."""
        config_file_path = config_file_path or get_config_file_path()
        config = ConfigParser()
        config["DEFAULT"] = {}
        if self._default_bucket_key:
            config["DEFAULT"]["default_bucket_key"] = self._default_bucket_key
        if self._mirror_root:
            config["DEFAULT"]["mirror_root"] = str(self._mirror_root)
        if self._iam_role_arn:
            config["DEFAULT"]["iam_role_arn"] = self._iam_role_arn
        if self._max_pool_connections:
            config["DEFAULT"]["max_pool_connections"] = str(self._max_pool_connections)
        with open(config_file_path, "w") as configfile:
            config.write(configfile)


# Create the singleton instance and export it as S3MPConfig
S3MPConfig = _S3MPConfigClass()
S3MPConfig.load_config()
