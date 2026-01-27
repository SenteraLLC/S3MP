"""Test s3_utils."""

import contextlib

import boto3
import pytest

from S3MP.types import S3Client
from S3MP.utils import s3_utils
from S3MP.utils.s3_utils import key_exists_on_s3, key_is_file_on_s3, s3_list_child_keys

TESTING_BUCKET_NAME = "s3mp-testing"


def list_assert(actual: list, expected: list, order_matters: bool = True):
    """Assert that two lists have the same contents, in the same order."""
    assert len(actual) == len(expected)
    if order_matters:
        assert all(a == b for a, b in zip(actual, expected, strict=True))
    else:
        assert all(a in expected for a in actual)


def _make_test_file_structure(
    bucket_key: str,
    prefix: str,
    current_data: dict,
    client: S3Client | None = None,
):
    """
    Make a test file structure, recursively.

    current_data is a Dictionary:
        - files: Dict[str, bytes] -> filename: file data
        - folders: Dict[str, Dict] -> folder name: folder data
    """
    if not client:
        client = boto3.client("s3")
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"
    if "files" in current_data:
        for fn, data in current_data["files"].items():
            client.put_object(Bucket=TESTING_BUCKET_NAME, Key=f"{prefix}{fn}", Body=data)
    if "folders" in current_data:
        for folder_name, folder_data in current_data["folders"].items():
            client.put_object(Bucket=TESTING_BUCKET_NAME, Key=f"{prefix}{folder_name}/")
            _make_test_file_structure(bucket_key, f"{prefix}{folder_name}", folder_data)


def test_folders_files():
    """Test folder/file creation and detection."""
    # Setup
    client: S3Client = boto3.client("s3")
    with contextlib.suppress(client.exceptions.BucketAlreadyOwnedByYou):
        client.create_bucket(Bucket=TESTING_BUCKET_NAME)

    setup_structure = {
        "folders": {
            "test_folder": {
                "files": {"test_file_1": b"test_file_1", "test_file_2": b"test_file_2"},
                "folders": {
                    "empty_subfolder": {},
                    "nonempty_subfolder": {"files": {"test_file_3": b"test_file_3"}},
                },
            }
        }
    }
    _make_test_file_structure(TESTING_BUCKET_NAME, "", setup_structure, client)

    bucket = boto3.resource("s3").Bucket(TESTING_BUCKET_NAME)

    # Test folder listing
    with pytest.warns(Warning):
        s3_list_child_keys("test_folder", bucket, client)
    # Use s3_list_single_key to get the full response dict for testing
    response = s3_utils.s3_list_single_key("test_folder/", bucket, client)
    child_files = [obj["Key"] for obj in response.get("Contents", [])]
    child_folders = [obj["Prefix"] for obj in response.get("CommonPrefixes", [])]
    list_assert(
        child_files,
        [
            "test_folder/",
            "test_folder/test_file_1",
            "test_folder/test_file_2",
            "test_folder/empty_subfolder/",
            "test_folder/nonempty_subfolder/",
        ],
    )
    list_assert(
        child_folders,
        ["test_folder/empty_subfolder/", "test_folder/nonempty_subfolder/"],
    )

    # Test folder and file existences
    keys_that_should_exist = [
        "test_folder/test_file_1",
        "test_folder/test_file_2",
        "test_folder",
        "test_folder/",
        "test_folder/nonempty_subfolder",
        "test_folder/nonempty_subfolder/",
        "test_folder/nonempty_subfolder/test_file_3",
        "test_folder/empty_subfolder",
        "test_folder/empty_subfolder/",
    ]
    for key in keys_that_should_exist:
        assert key_exists_on_s3(key, bucket, client)

    keys_that_should_not_exist = [
        "test_folder/test_file_1/",
        "test_folder/test_file_2/",
        "test_folder/nonempty_subfolder/test_file_3/",
    ]
    for key in keys_that_should_not_exist:
        assert not key_exists_on_s3(key, bucket, client)

    keys_that_are_files = [
        "test_folder/test_file_1",
        "test_folder/test_file_2",
        "test_folder/nonempty_subfolder/test_file_3",
    ]
    for key in keys_that_are_files:
        assert key_is_file_on_s3(key, bucket, client)

    keys_that_are_folders = [
        "test_folder",
        "test_folder/",
        "test_folder/nonempty_subfolder",
        "test_folder/nonempty_subfolder/",
        "test_folder/empty_subfolder",
        "test_folder/empty_subfolder/",
    ]
    for key in keys_that_are_folders:
        assert not key_is_file_on_s3(key, bucket, client)


if __name__ == "__main__":
    test_folders_files()
