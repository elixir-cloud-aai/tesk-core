"""Tests for the S3 transfers of the filer."""

from collections.abc import Iterator
import os
from unittest.mock import MagicMock, mock_open, patch

import boto3

# from tesk_core.extract_endpoint import extract_endpoint
from moto import mock_s3
from pyfakefs.fake_filesystem import FakeFilesystem
import pytest

from tesk_core.filer_s3 import S3Transput
from tesk_core.transput import Type

RESOURCES_SRC = os.path.join(os.path.dirname(__file__), "resources/copyDirTest/src")


@pytest.fixture
def moto_boto() -> Iterator[None]:
    """Mock S3 with a bucket `tesk` that holds two objects.

    Yields:
        Nothing, while the mock is active.
    """
    with mock_s3():
        boto3.client("s3", endpoint_url="http://s3.amazonaws.com")

        client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
        client.create_bucket(Bucket="tesk")
        client.Bucket("tesk").put_object(Key="folder/file.txt", Body="")
        client.Bucket("tesk").put_object(Key="folder1/folder2/file.txt", Body="")
        yield


@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        (
            "/home/user/filer_test/file.txt",
            "s3://tesk/folder/file.txt",
            Type.File,
            ("tesk", "folder/file.txt"),
        ),
        (
            "/home/user/filer_test/file.txt",
            "s3://tesk/folder1/folder2",
            Type.Directory,
            ("tesk", "folder1/folder2"),
        ),
    ],
)
def test_get_bucket_name_and_file_path(
    moto_boto: None, path: str, url: str, ftype: Type, expected: tuple[str, str]
) -> None:
    """Test that the bucket name and path are extracted for files and folders."""
    trans = S3Transput(path, url, ftype)
    assert trans.get_bucket_name_and_file_path() == expected


@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        ("/home/user/filer_test/file.txt", "s3://tesk/folder/file.txt", Type.File, 0),
        (
            "/home/user/filer_test/file.txt",
            "s3://mybucket/folder/file.txt",
            Type.File,
            1,
        ),
        ("/home/user/filer_test/", "s3://tesk/folder1/folder2", Type.Directory, 0),
        (
            "/home/user/filer_test/",
            "s3://mybucket/folder1/folder2",
            Type.Directory,
            1,
        ),
    ],
)
def test_check_if_bucket_exists(
    moto_boto: None, path: str, url: str, ftype: Type, expected: int
) -> None:
    """Test that the existence of the bucket is checked."""
    client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    trans = S3Transput(path, url, ftype)
    assert trans.check_if_bucket_exists(client) == expected


# @patch('tesk_core.filer.os.makedirs')
# @patch('builtins.open')
# @patch('s3transfer.utils.OSUtils.rename_file')
@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        ("/home/user/filer_test/file.txt", "s3://tesk/folder/file.txt", Type.File, 0),
        (
            "/home/user/filer_test/file.txt",
            "s3://tesk/folder/file_new.txt",
            Type.File,
            1,
        ),
    ],
)
def test_s3_download_file(
    moto_boto: None,
    path: str,
    url: str,
    ftype: Type,
    expected: int,
    fs: FakeFilesystem,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a successful and a failed file download from the object storage."""
    with S3Transput(path, url, ftype) as trans:
        assert trans.download_file() == expected
        if expected:
            assert "Not Found" in caplog.text
        else:
            assert os.path.exists(path)


@patch("tesk_core.filer.os.makedirs")
@patch("builtins.open")
@patch("s3transfer.utils.OSUtils.rename_file")
# @patch("tesk_core.filer_s3.extract_endpoint", return_value="http://s3.amazonaws.com")
@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        ("filer_test/", "s3://tesk/folder1/", Type.Directory, 0),
        ("filer_test/", "s3://tesk/folder10/folder20", Type.Directory, 1),
    ],
)
def test_s3_download_directory(
    mock_makedirs: MagicMock,
    mock_open: MagicMock,
    mock_rename: MagicMock,
    path: str,
    url: str,
    ftype: Type,
    expected: int,
    moto_boto: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a successful and a failed directory download from the storage."""
    with S3Transput(path, url, ftype) as trans:
        assert trans.download_dir() == expected
        print(mock_rename.mock_calls)
        if expected:
            assert "Invalid file path" in caplog.text
        else:
            # The s3 object path s3://tesk/folder1/ will contain 'folder2',
            # checking if the 'folder2' is present in the download folder.
            mock_rename.assert_called_once_with("filer_test/folder2", exist_ok=True)


@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        ("/home/user/filer_test/file.txt", "s3://tesk/folder/file.txt", Type.File, 0),
        (
            "/home/user/filer_test/file_new.txt",
            "s3://tesk/folder/file.txt",
            Type.File,
            1,
        ),
    ],
)
def test_s3_upload_file(
    moto_boto: None,
    path: str,
    url: str,
    ftype: Type,
    expected: int,
    fs: FakeFilesystem,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a successful and a failed file upload to the object storage."""
    fs.create_file("/home/user/filer_test/file.txt")
    client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    trans = S3Transput(path, url, ftype)
    trans.bucket_obj = client.Bucket(trans.bucket)
    assert trans.upload_file() == expected
    if expected:
        assert "File upload failed for" in caplog.text
    else:
        # The file was uploaded: load() raises an exception if the object
        # doesn't exist
        client.Object("tesk", "folder/file.txt").load()


@pytest.mark.parametrize(
    "filename, url, expected_content",
    [
        ("file.txt", "s3://tesk/folder/file.txt", "text/plain; charset=utf-8"),
        ("file.zip", "s3://tesk/folder/file.zip", "application/zip"),
    ],
)
def test_s3_upload_file_content_type(
    moto_boto: None,
    filename: str,
    url: str,
    expected_content: str,
    fs: FakeFilesystem,
) -> None:
    """Test that uploaded objects have the Content-Type of their extension."""
    fs.create_file(f"/home/user/filer_test/{filename}")
    client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    trans = S3Transput(f"/home/user/filer_test/{filename}", url, Type.File)
    trans.bucket_obj = client.Bucket(trans.bucket)
    assert trans.upload_file() == 0
    head = client.meta.client.head_object(Bucket=trans.bucket, Key=trans.file_path)
    assert head["ContentType"] == expected_content


@pytest.mark.parametrize(
    "path, url, ftype,expected",
    [
        (RESOURCES_SRC, "s3://tesk/folder1/folder2", Type.Directory, 0),
        (
            "/home/user/filer_test_new/",
            "s3://tesk/folder1/folder2",
            Type.Directory,
            1,
        ),
    ],
)
def test_s3_upload_directory(
    path: str,
    url: str,
    ftype: Type,
    expected: int,
    moto_boto: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a successful and a failed directory upload to the object storage."""
    client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    trans = S3Transput(path, url, ftype)
    trans.bucket_obj = client.Bucket(trans.bucket)
    assert trans.upload_dir() == expected
    if expected:
        assert "File upload failed for" in caplog.text
    else:
        # The file was uploaded: load() raises an exception if the object
        # doesn't exist
        client.Object("tesk", "folder1/folder2/a/1.txt").load()
        head = client.meta.client.head_object(
            Bucket=trans.bucket, Key="folder1/folder2/a/1.txt"
        )
        assert head["ContentType"].startswith("text/")


def test_upload_directory_for_unknown_file_type(
    moto_boto: None,
    fs: FakeFilesystem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an object that is neither file nor directory is logged.

    If the exception is raised, an error message is logged.
    """
    monkeypatch.setattr(os.path, "isfile", lambda _: False)
    fs.create_file("/home/user/filer_test/text.txt")
    url, ftype = "s3://tesk/folder10/folder20", Type.Directory
    path = "/home/user/filer_test/"
    trans = S3Transput(path, url, ftype)
    client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    trans.bucket_obj = client.Bucket(trans.bucket)
    trans.upload_dir()
    assert "Object is neither file or directory" in caplog.text


@patch("tesk_core.filer.os.path.exists", return_value=1)
def test_extract_url_from_config_file(mock_path_exists: MagicMock) -> None:
    """Test the extraction of the endpoint URL from the default file."""
    read_data = "\n".join(
        ["[default]", "endpoint_url = http://s3-aws-region.amazonaws.com"]
    )
    with patch(
        "builtins.open", mock_open(read_data=read_data), create=True
    ) as mock_file:
        mock_file.return_value.__iter__.return_value = read_data.splitlines()
        # assert extract_endpoint() == "http://s3-aws-region.amazonaws.com"
        # mock_file.assert_called_once_with("~/.aws/config", encoding=None)


@patch.dict(os.environ, {"AWS_CONFIG_FILE": "~/.aws/config"})
def test_extract_url_from_environ_variable() -> None:
    """Test the extraction of the endpoint URL from the path in the environment."""
    read_data = "\n".join(
        ["[default]", "endpoint_url = http://s3-aws-region.amazonaws.com"]
    )
    with patch(
        "builtins.open", mock_open(read_data=read_data), create=True
    ) as mock_file:
        mock_file.return_value.__iter__.return_value = read_data.splitlines()
        # assert (extract_endpoint() == "http://s3-aws-region.amazonaws.com")
        # mock_file.assert_called_once_with(
        #     os.environ["AWS_CONFIG_FILE"], encoding=None
        # )
