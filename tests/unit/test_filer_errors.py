"""Tests for the filer's error paths and its command line.

The FTP helpers are tested with a mocked connection, so that each step can
fail on its own; the transfers against real servers are in the other filer
tests.
"""

# ftplib's error classes and a mocked connection: nothing connects
# ruff: noqa: S321

import ftplib
import gzip
import json
import logging
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws
import pytest

from tesk_core import filer, path
from tesk_core.filer import (
    FTPTransput,
    HTTPTransput,
    copyFile,
    ftp_check_directory,
    ftp_make_dirs,
    process_file,
)
from tesk_core.filer_s3 import S3Transput
from tesk_core.transput import Type


@pytest.fixture
def ftp() -> MagicMock:
    """Get a mocked FTP connection in the directory `/home`.

    Returns:
        The connection.
    """
    connection = MagicMock(spec=ftplib.FTP)
    connection.host = "ftp.example.org"
    connection.pwd.return_value = "/home"
    return connection


def test_http_upload_dir_of_special_file(tmp_path: Path) -> None:
    """Test that a directory with an entry that is no file or directory fails."""
    os.mkfifo(tmp_path / "fifo")
    transfer = HTTPTransput(str(tmp_path), "https://example.org/out", Type.Directory)

    assert transfer.upload_dir() == 1


def test_copy_file_into_glob_directory(tmp_path: Path) -> None:
    """Test that a destination with `*` copies into its directory."""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.txt").write_text("a")
    (tmp_path / "dst").mkdir()

    copyFile(str(tmp_path / "src" / "*.txt"), str(tmp_path / "dst" / "*.txt"))

    assert (tmp_path / "dst" / "a.txt").read_text() == "a"


def test_ftp_upload_file_onto_directory(ftp: MagicMock) -> None:
    """Test that a file isn't uploaded where the server has a directory."""
    transfer = FTPTransput("/tmp/a.txt", "ftp://example.org/dir", Type.File, ftp)

    assert transfer.upload_file() == 1
    ftp.storbinary.assert_not_called()


def test_ftp_download_dir_stops_at_failed_entry(ftp: MagicMock, tmp_path: Path) -> None:
    """Test that a directory download fails with the first entry that fails.

    The entry's local directory is created first.
    """
    ftp.retrlines.side_effect = lambda _, callback: callback(
        "-rw-r--r--    1 user     group          10 Jan 01  2026 a.txt"
    )
    ftp.retrbinary.side_effect = ftplib.error_perm("550 No such file")
    target = tmp_path / "new" / "dir"
    transfer = FTPTransput(str(target), "ftp://example.org/dir", Type.Directory, ftp)

    assert transfer.download_dir() == 1
    assert target.is_dir()


def test_ftp_download_file_creates_directory(ftp: MagicMock, tmp_path: Path) -> None:
    """Test that a file download creates the file's local directory."""
    target = tmp_path / "new" / "a.txt"
    transfer = FTPTransput(str(target), "ftp://example.org/a.txt", Type.File, ftp)

    assert transfer.download_file() == 0
    assert target.parent.is_dir()


def test_ftp_check_directory_without_working_directory(ftp: MagicMock) -> None:
    """Test that the check fails if the working directory is unknown."""
    ftp.pwd.return_value = ""

    assert ftp_check_directory(ftp, "/dir") == 1


def test_ftp_check_directory_cannot_return(ftp: MagicMock) -> None:
    """Test that the check fails if it can't change back to where it was."""
    ftp.cwd.side_effect = [None, ftplib.error_perm("550 Gone")]

    assert ftp_check_directory(ftp, "/dir") == 1


def test_ftp_make_dirs_without_working_directory(ftp: MagicMock) -> None:
    """Test that no directory is created if the working directory is unknown."""
    ftp.pwd.return_value = ""

    assert ftp_make_dirs(ftp, "/a/b") == 1
    ftp.mkd.assert_not_called()


def test_ftp_make_dirs_mkd_fails(ftp: MagicMock) -> None:
    """Test that a directory that can't be created fails."""
    ftp.cwd.side_effect = ftplib.error_perm("550 No such directory")
    ftp.mkd.side_effect = ftplib.error_perm("550 Permission denied")

    assert ftp_make_dirs(ftp, "/a") == 1


def test_ftp_make_dirs_unexpected_reply(ftp: MagicMock) -> None:
    """Test that an unexpected reply on a parent directory fails."""
    ftp.cwd.side_effect = [
        ftplib.error_perm("550 No such directory"),
        ftplib.error_reply("200 Unexpected"),
    ]

    assert ftp_make_dirs(ftp, "/a/b") == 1
    ftp.mkd.assert_not_called()


def test_ftp_make_dirs_cannot_return(ftp: MagicMock) -> None:
    """Test that it fails if it can't change back to where it was."""
    ftp.cwd.side_effect = [
        ftplib.error_perm("550 No such directory"),
        None,
        ftplib.error_reply("200 Unexpected"),
    ]

    assert ftp_make_dirs(ftp, "/a") == 1


def test_process_file_unknown_direction(caplog: pytest.LogCaptureFixture) -> None:
    """Test that a transfer that is neither input nor output does nothing."""
    data = {"path": "/data/a.txt", "url": "https://example.org/a", "type": "FILE"}

    with caplog.at_level(logging.INFO):
        assert process_file("other", data) == 0

    assert "There was no action to do with /data/a.txt" in caplog.text


@pytest.fixture
def s3() -> Any:
    """Mock S3 with an empty bucket `tesk`.

    Yields:
        The S3 resource.
    """
    with mock_aws():
        client = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
        client.create_bucket(Bucket="tesk")
        yield client


def test_s3_missing_bucket_exits(s3: Any) -> None:
    """Test that a transfer to a bucket that doesn't exist exits."""
    transfer = S3Transput("/tmp/a.txt", "s3://missing/a.txt", Type.File)

    with pytest.raises(SystemExit) as exc_info, transfer:
        pass

    assert exc_info.value.code == 1


def test_s3_upload_dir_stops_at_failed_entry(s3: Any, tmp_path: Path) -> None:
    """Test that a directory upload fails with the first entry that fails."""
    (tmp_path / "a.txt").write_text("a")
    transfer = S3Transput(str(tmp_path), "s3://tesk/dir", Type.Directory)
    transfer.bucket_obj = s3.Bucket("tesk")

    with patch.object(S3Transput, "upload", return_value=1):
        assert transfer.upload_dir() == 1


@pytest.fixture
def prefix(tmp_path: Path) -> Any:
    """Mount the task's paths below a temporary directory.

    Args:
        tmp_path: The temporary directory.

    Yields:
        The directory.
    """
    with patch.object(path, "TASK_PATH_PREFIX", str(tmp_path)):
        yield tmp_path


@pytest.fixture
def log_config() -> Any:
    """Mock the filer's logging setup, which changes the root logger.

    Yields:
        The mocked setup.
    """
    with patch("tesk_core.filer.logConfig") as mock:
        yield mock


def task(content: str) -> dict[str, Any]:
    """Describe a task with an input with content.

    Args:
        content: The input's content.

    Returns:
        The task.
    """
    return {
        "inputs": [{"path": "/a.txt", "type": "FILE", "content": content}],
        "outputs": [],
    }


def test_main_stages_inputs(
    prefix: Path, log_config: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the filer stages the inputs of the task on the command line."""
    monkeypatch.setattr("sys.argv", ["filer", "inputs", json.dumps(task("a"))])

    assert filer.main() == 0

    assert (prefix / "a.txt").read_text() == "a"
    log_config.assert_called_once_with(logging.ERROR)


def test_main_reads_gzipped_task(
    prefix: Path, log_config: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the filer reads a gzipped task file, with debug logging."""
    data = prefix / "task.json.gz"
    with gzip.open(data, "wt") as fh:
        json.dump(task("b"), fh)
    monkeypatch.setattr("sys.argv", ["filer", "-d", "inputs", str(data)])

    assert filer.main() == 0

    assert (prefix / "a.txt").read_text() == "b"
    log_config.assert_called_once_with(logging.DEBUG)


def test_main_fails_with_failed_input(
    prefix: Path, log_config: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the filer exits with 1 at the first input that fails."""
    monkeypatch.setattr("sys.argv", ["filer", "inputs", json.dumps(task("a"))])

    with patch("tesk_core.filer.process_file", return_value=1):
        assert filer.main() == 1
