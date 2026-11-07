"""Tests for 'filer.py' FTP functionalities using 'pytest'."""

import ftplib
import os
from unittest import mock

from tesk_core.filer import (
    ftp_check_directory,
    ftp_download_file,
    ftp_login,
    ftp_make_dirs,
    ftp_upload_file,
)


def test_ftp_login(mocker):
    """Ensure ftp_login detects ftp credentials and properly calls
    ftplib.FTP.login."""

    conn = mocker.patch("ftplib.FTP")
    mock_login = mocker.patch("ftplib.FTP.login")
    with mock.patch.dict(
        "os.environ",
        {
            "TESK_FTP_USERNAME": "test",
            "TESK_FTP_PASSWORD": "test_pass",
        },
    ):
        ftp_login(conn, None, None)
        mock_login.assert_called_with("test", "test_pass")


def test_ftp_upload_file_error(mocker, caplog):
    """Ensure that upon upload error, ftp_upload_file behaves correctly."""

    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.storbinary", side_effect=ftplib.error_reply)
    assert 1 == ftp_upload_file(conn, __file__, "/home/tesk/test_copy.py")
    assert "Unable to upload file" in caplog.text


def test_ftp_download_file_error(mocker, caplog):
    """Ensure that upon download error, ftp_download_file behaves correctly."""

    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.retrbinary", side_effect=ftplib.error_perm)
    with mock.patch("builtins.open", mock.mock_open(), create=False):
        assert 1 == ftp_download_file(conn, "test_filer_ftp_pytest.py", "test_copy.py")
        assert "Unable to download file" in caplog.text


def test_ftp_download_file_success(mocker, caplog):
    """Ensure that upon successful download, the local destination file has
    been created."""

    conn = mocker.patch("ftplib.FTP")
    mock_retrbin = mocker.patch("ftplib.FTP.retrbinary")
    with mock.patch("builtins.open", mock.mock_open(), create=False) as m:
        assert 0 == ftp_download_file(conn, "test_filer_ftp_pytest.py", "test_copy.py")

        mock_retrbin.assert_called_with("RETR " + "test_filer_ftp_pytest.py", mock.ANY)

        m.assert_called_with("test_copy.py", "w+b")

        # Since we want to avoid file creation in testing and we're using
        # 'create=False', we cannot check whether a file exists or not (but
        # it's not really necessary since we can assert that the necessary
        # functions have been invoked.
        # assert os.path.exists('test_copy.py')


def test_ftp_check_directory_error(mocker, caplog):
    """Ensure ftp_check_directory_error creates the proper error log
    message in case of error."""

    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.cwd", side_effect=ftplib.error_reply)
    assert 1 == ftp_check_directory(conn, "/folder/file")
    assert "Could not check if path" in caplog.text


def test_ftp_make_dirs(mocker):
    """In case of existing directory, exit with 0."""

    conn = mocker.patch("ftplib.FTP")
    assert ftp_make_dirs(conn, os.curdir) == 0
