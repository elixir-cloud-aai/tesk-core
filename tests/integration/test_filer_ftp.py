"""Integration tests of the filer's FTP transfers, against a local FTP server."""

import ftplib
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import (
    FTPTransput,
    Type,
    ftp_make_dirs,
)


def test_ftp_upload_dir(mocker: MockerFixture, tmp_path: Path, ftpserver: Any) -> None:
    """Test that the upload of a directory through FTP completes successfully.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        ftpserver: The local FTP server, from `pytest-localftpserver`, which
            ships no type information.
    """
    # Local nested directories with files, on the real file system and with
    # an absolute path: the FTP server runs in a thread of this process, so a
    # fake file system (pyfakefs) would hide its home directory from it, and
    # it changes the working directory of the process
    (tmp_path / "dir1" / "dir2").mkdir(parents=True)
    (tmp_path / "dir1" / "file1").write_text("this is random")
    (tmp_path / "dir1" / "dir2" / "file2").write_text("not really")
    (tmp_path / "dir1" / "dir2" / "file4.txt").write_text("took me a while")

    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()  # noqa: S321  # plain FTP to a local test server

    mocker.patch(
        "ftplib.FTP.connect",
        side_effect=conn.connect(host=login_dict["host"], port=login_dict["port"]),
    )
    mocker.patch(
        "ftplib.FTP.login",
        side_effect=conn.login(login_dict["user"], login_dict["passwd"]),
    )
    mocker.patch("ftplib.FTP.pwd", side_effect=conn.pwd)
    mocker.patch("ftplib.FTP.cwd", side_effect=conn.cwd)
    mocker.patch("ftplib.FTP.mkd", side_effect=conn.mkd)
    mock_storbinary = mocker.patch("ftplib.FTP.storbinary", side_effect=conn.storbinary)

    ftp_obj = FTPTransput(
        str(tmp_path / "dir1"),
        "ftp://" + login_dict["host"] + "/dir1",
        Type.Directory,
        ftp_conn=conn,
    )

    ftp_obj.upload_dir()

    # We use mock.ANY since the 2nd argument of the 'ftplib.FTP.storbinary' is
    # a file object and we can't have the same between the original and the
    # mock calls
    assert sorted(mock_storbinary.mock_calls) == sorted(
        [
            mock.call("STOR /" + "/dir1/file1", mock.ANY),
            mock.call("STOR /" + "/dir1/dir2/file2", mock.ANY),
            mock.call("STOR /" + "/dir1/dir2/file4.txt", mock.ANY),
        ]
    )
    home = Path(ftpserver.server_home)
    assert (home / "dir1" / "file1").read_text() == "this is random"
    assert (home / "dir1" / "dir2" / "file2").read_text() == "not really"
    assert (home / "dir1" / "dir2" / "file4.txt").read_text() == "took me a while"


def test_ftp_download_dir(
    mocker: MockerFixture, tmp_path: Path, ftpserver: Any
) -> None:
    """Test that the download of a directory through FTP completes successfully.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        ftpserver: The local FTP server, from `pytest-localftpserver`, which
            ships no type information.
    """
    # Temporary nested directories with files
    (tmp_path / "dir1" / "dir2").mkdir(parents=True)
    (tmp_path / "dir1" / "file1").write_text("this is random")
    (tmp_path / "dir1" / "dir2" / "file2").write_text("not really")
    (tmp_path / "dir1" / "dir2" / "file3").write_text("took me a while")

    # Temporary folder for download
    (tmp_path / "downloads").mkdir()

    # Populate the server with the above files to later download
    ftpserver.put_files({"src": str(tmp_path / "dir1/file1"), "dest": "remote1/file1"})
    ftpserver.put_files(
        {"src": str(tmp_path / "dir1/dir2/file2"), "dest": "remote1/remote2/file2"}
    )
    ftpserver.put_files(
        {"src": str(tmp_path / "dir1/dir2/file3"), "dest": "remote1/remote2/file3"}
    )

    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()  # noqa: S321  # plain FTP to a local test server
    conn.connect(host=login_dict["host"], port=login_dict["port"])
    conn.login(login_dict["user"], login_dict["passwd"])

    mock_retrbinary = mocker.patch("ftplib.FTP.retrbinary", side_effect=conn.retrbinary)

    ftp_obj = FTPTransput(
        str(tmp_path / "downloads"),
        "ftp://" + login_dict["host"] + "/remote1",
        Type.Directory,
        ftp_conn=conn,
    )

    ftp_obj.download_dir()

    # We use mock.ANY since the 2nd argument of the 'ftplib.FTP.storbinary' is
    # a file object and we can't have the same between the original and the
    # mock calls
    assert sorted(mock_retrbinary.mock_calls) == sorted(
        [
            mock.call("RETR " + "/remote1/file1", mock.ANY),
            mock.call("RETR " + "/remote1/remote2/file2", mock.ANY),
            mock.call("RETR " + "/remote1/remote2/file3", mock.ANY),
        ]
    )

    downloads = tmp_path / "downloads"
    assert (downloads / "file1").read_text() == "this is random"
    assert (downloads / "remote2/file2").read_text() == "not really"
    assert (downloads / "remote2/file3").read_text() == "took me a while"


def test_ftp_make_dirs_error(
    mocker: MockerFixture, ftpserver: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Test the return value and the message for an `ftplib.error_reply`.

    Args:
        mocker: The mocker fixture.
        ftpserver: The local FTP server, from `pytest-localftpserver`, which
            ships no type information.
        caplog: The fixture that captures the log.
    """
    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()  # noqa: S321  # plain FTP to a local test server
    conn.connect(host=login_dict["host"], port=login_dict["port"])
    conn.login(login_dict["user"], login_dict["passwd"])

    mocker.patch("ftplib.FTP.cwd", side_effect=ftplib.error_reply)

    assert ftp_make_dirs(conn, "dir1") == 1
    assert "Unable to create directory" in caplog.text


@pytest.mark.parametrize("user_info", ["", "someone@"])
def test_ftp_download_file_with_port(
    tmp_path: Path,
    ftpserver: Any,
    monkeypatch: pytest.MonkeyPatch,
    user_info: str,
) -> None:
    """Test that a file is downloaded from a URL with a port.

    With and without user information in the URL; the credentials come from
    the environment.

    Args:
        tmp_path: The temporary directory of the test.
        ftpserver: The local FTP server, from `pytest-localftpserver`, which
            ships no type information.
        monkeypatch: The fixture that sets the environment.
        user_info: User information in the URL.
    """
    login = ftpserver.get_login_data()
    monkeypatch.setenv("HOME", str(tmp_path))  # No `.netrc`
    monkeypatch.setenv("TESK_FTP_USERNAME", login["user"])
    monkeypatch.setenv("TESK_FTP_PASSWORD", login["passwd"])
    (Path(ftpserver.server_home) / "port.txt").write_text("content")
    url = f"ftp://{user_info}{login['host']}:{login['port']}/port.txt"

    with FTPTransput(str(tmp_path / "port.txt"), url, Type.File) as transfer:
        assert transfer.download_file() == 0

    assert (tmp_path / "port.txt").read_text() == "content"
