"""Integration tests of the filer's FTP transfers, against a local FTP server."""

import ftplib
import os
from unittest import mock

from tesk_core.filer import (
    FTPTransput,
    Type,
    ftp_make_dirs,
)


def test_ftp_upload_dir(mocker, tmp_path, ftpserver):
    """Check whether the upload of a directory through FTP completes
    successfully."""

    # Local nested directories with files, on the real file system and with
    # an absolute path: the FTP server runs in a thread of this process, so a
    # fake file system (pyfakefs) would hide its home directory from it, and
    # it changes the working directory of the process
    (tmp_path / "dir1" / "dir2").mkdir(parents=True)
    (tmp_path / "dir1" / "file1").write_text("this is random")
    (tmp_path / "dir1" / "dir2" / "file2").write_text("not really")
    (tmp_path / "dir1" / "dir2" / "file4.txt").write_text("took me a while")

    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()

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
    mock_storbinary = mocker.patch("ftplib.FTP.storbinary")

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


def test_ftp_download_dir(mocker, tmpdir, tmp_path, ftpserver):
    """Check whether the download of a directory through FTP completes
    successfully."""

    # Temporary nested directories with files
    file1 = tmpdir.mkdir("dir1").join("file1")
    file1.write("this is random")
    file2 = tmpdir.mkdir("dir1/dir2").join("file2")
    file2.write("not really")
    file3 = tmpdir.join("dir1/dir2/file3")
    file3.write("took me a while")

    # Temporary folder for download
    tmpdir.mkdir("downloads")

    # Populate the server with the above files to later download
    ftpserver.put_files({"src": str(tmp_path) + "/dir1/file1", "dest": "remote1/file1"})
    ftpserver.put_files(
        {"src": str(tmp_path) + "/dir1/dir2/file2", "dest": "remote1/remote2/file2"}
    )
    ftpserver.put_files(
        {"src": str(tmp_path) + "/dir1/dir2/file3", "dest": "remote1/remote2/file3"}
    )

    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()
    conn.connect(host=login_dict["host"], port=login_dict["port"])
    conn.login(login_dict["user"], login_dict["passwd"])

    mock_retrbinary = mocker.patch("ftplib.FTP.retrbinary", side_effect=conn.retrbinary)

    ftp_obj = FTPTransput(
        str(tmp_path) + "downloads",
        "ftp://" + login_dict["host"],
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

    assert os.path.exists(str(tmp_path) + "downloads/remote1/file1")
    assert os.path.exists(str(tmp_path) + "downloads/remote1/remote2/file2")
    assert os.path.exists(str(tmp_path) + "downloads/remote1/remote2/file3")


def test_ftp_make_dirs_error(mocker, ftpserver, caplog):
    """Ensure in case of 'ftplib.error_reply', both the return value
    and the error message are correct."""

    login_dict = ftpserver.get_login_data()

    conn = ftplib.FTP()
    conn.connect(host=login_dict["host"], port=login_dict["port"])
    conn.login(login_dict["user"], login_dict["passwd"])

    mocker.patch("ftplib.FTP.cwd", side_effect=ftplib.error_reply)

    assert ftp_make_dirs(conn, "dir1") == 1
    assert "Unable to create directory" in caplog.text
