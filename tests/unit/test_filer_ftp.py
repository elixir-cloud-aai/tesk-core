"""Tests for 'filer.py' FTP functionalities using 'pytest'."""

import ftplib
import os
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import (
    ftp_check_directory,
    ftp_download_file,
    ftp_login,
    ftp_make_dirs,
    ftp_upload_file,
)


def test_ftp_login(mocker: MockerFixture) -> None:
    """Test that ftp_login detects credentials and calls ftplib.FTP.login."""
    conn = mocker.patch("ftplib.FTP")
    mock_login = mocker.patch("ftplib.FTP.login")
    with mock.patch.dict(
        "os.environ",
        {
            "TESK_FTP_USERNAME": "test",
            "TESK_FTP_PASSWORD": "test_pass",
        },
    ):
        ftp_login(conn, "", None)
        mock_login.assert_called_with("test", "test_pass")


def test_ftp_upload_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_upload_file behaves correctly upon upload error."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.storbinary", side_effect=ftplib.error_reply)
    assert ftp_upload_file(conn, __file__, "/home/tesk/test_copy.py") == 1
    assert "Unable to upload file" in caplog.text


def test_ftp_download_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_download_file behaves correctly upon download error."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.retrbinary", side_effect=ftplib.error_perm)
    with mock.patch("builtins.open", mock.mock_open(), create=False):
        assert ftp_download_file(conn, "remote.txt", "test_copy.py") == 1
        assert "Unable to download file" in caplog.text


def test_ftp_download_file_success(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a successful download creates the local destination file."""
    conn = mocker.patch("ftplib.FTP")
    mock_retrbin = mocker.patch("ftplib.FTP.retrbinary")
    with mock.patch("builtins.open", mock.mock_open(), create=False) as m:
        assert ftp_download_file(conn, "remote.txt", "test_copy.py") == 0

        mock_retrbin.assert_called_with("RETR remote.txt", mock.ANY)

        m.assert_called_with("test_copy.py", "w+b")

        # Since we want to avoid file creation in testing and we're using
        # 'create=False', we cannot check whether a file exists or not (but
        # it's not really necessary since we can assert that the necessary
        # functions have been invoked.
        # assert os.path.exists('test_copy.py')


def test_ftp_check_directory_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_check_directory logs the proper error message."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.cwd", side_effect=ftplib.error_reply)
    assert ftp_check_directory(conn, "/folder/file") == 1
    assert "Could not check if path" in caplog.text


def test_ftp_make_dirs(mocker: MockerFixture) -> None:
    """Test that ftp_make_dirs exits with 0 for an existing directory."""
    conn = mocker.patch("ftplib.FTP")
    assert ftp_make_dirs(conn, os.curdir) == 0
