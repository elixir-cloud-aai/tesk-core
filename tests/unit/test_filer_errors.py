"""Tests for the filer's error paths.

The FTP helpers are tested with a mocked connection, so that each step can
fail on its own; the transfers against real servers are in the other filer
tests.
"""

# ftplib's error classes and a mocked connection: nothing connects
# ruff: noqa: S321

import ftplib
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tesk_core.filer import (
    FTPTransput,
    HTTPTransput,
    copyFile,
    ftp_check_directory,
    ftp_make_dirs,
    process_file,
)
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
    ftp.cwd.assert_not_called()


def test_ftp_check_directory_cannot_return(ftp: MagicMock) -> None:
    """Test that the check fails if it can't change back to where it was."""
    ftp.cwd.side_effect = [None, ftplib.error_perm("550 Gone")]

    assert ftp_check_directory(ftp, "/dir") == 1


def test_ftp_make_dirs_without_working_directory(ftp: MagicMock) -> None:
    """Test that no directory is created if the working directory is unknown."""
    ftp.pwd.return_value = ""

    assert ftp_make_dirs(ftp, "/a/b") == 1
    ftp.mkd.assert_not_called()


def test_ftp_make_dirs_mkd_fails(
    ftp: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a directory that can't be created fails."""
    ftp.cwd.side_effect = ftplib.error_perm("550 No such directory")
    ftp.mkd.side_effect = ftplib.error_perm("550 Permission denied")

    assert ftp_make_dirs(ftp, "/a") == 1
    ftp.mkd.assert_called_once_with("/a")
    assert 'Unable to create directory "/a"' in caplog.text


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
