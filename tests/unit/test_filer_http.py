"""Tests for 'filer.py' HTTP functionalities using 'pytest'."""

import os
from pathlib import Path
from typing import IO
from unittest import mock

from pyfakefs.fake_filesystem import FakeFilesystem
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from tesk_core.filer import HTTPTransput, Type, http_join

PATH_DOWN = "test_download_file.txt"
PATH_UP = __file__
SUCCESS = 200
FAIL = 300
URL = "http://www.foo.bar"
FTYPE = Type.File

resp = Response()
resp._content = b'{ "foo" : "bar" }'


def test_download_file(mocker: MockerFixture) -> None:
    """Test that a file gets properly downloaded."""
    resp.status_code = SUCCESS
    http_obj = HTTPTransput(PATH_DOWN, URL, FTYPE)
    mocker.patch("requests.get", return_value=resp)

    with mock.patch(
        "builtins.open", mock.mock_open(read_data=resp._content), create=False
    ):
        assert http_obj.download_file() == 0
        with open(PATH_DOWN, "rb") as fh:
            assert fh.read() == resp._content


def test_download_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a download error returns the correct value and message."""
    resp.status_code = FAIL
    http_obj = HTTPTransput(PATH_DOWN, URL, FTYPE)
    mocker.patch("requests.get", return_value=resp)

    assert http_obj.download_file() == 1
    assert f"Got status code: {FAIL}" in caplog.text


def test_upload_file(mocker: MockerFixture) -> None:
    """Test that a file gets properly uploaded."""
    resp.status_code = SUCCESS
    http_obj = HTTPTransput(PATH_UP, URL, FTYPE)
    mocker.patch("requests.put", return_value=resp)

    assert http_obj.upload_file() == 0


def test_upload_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that an upload error returns the correct value and message."""
    resp.status_code = FAIL
    http_obj = HTTPTransput(PATH_UP, URL, FTYPE)
    mocker.patch("requests.put", return_value=resp)

    assert http_obj.upload_file() == 1
    assert f"Got status code: {FAIL}" in caplog.text


def test_upload_dir(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """Test that each file inside nested directories gets uploaded."""
    # Tele2 Speedtest Service, free upload /download test server
    endpoint = "http://speedtest.tele2.net/upload.php"
    resp.status_code = 200

    fs.create_dir("dir1")
    fs.create_dir("dir1/dir2")
    fs.create_file("dir1/file1", contents="this is random")
    fs.create_file("dir1/dir2/file2", contents="not really")
    fs.create_file("dir1/dir2/file4.txt", contents="took me a while")

    # Each file is sent as an open file object, so its content is read
    # during the call.
    uploads = {}

    def mock_put(url: str, data: bytes | IO[bytes]) -> Response:
        uploads[url] = data if isinstance(data, bytes) else data.read()
        return resp

    mocker.patch("requests.put", side_effect=mock_put)

    http_obj = HTTPTransput("dir1", endpoint + "/dir1", Type.Directory)

    assert http_obj.upload_dir() == 0

    assert uploads == {
        endpoint + "/dir1/dir2/file2": b"not really",
        endpoint + "/dir1/dir2/file4.txt": b"took me a while",
        endpoint + "/dir1/file1": b"this is random",
    }


def test_upload_dir_error(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """Test that a directory upload fails if the upload of a file fails."""
    fs.create_dir("dir1")
    fs.create_file("dir1/file1", contents="this is random")
    mocker.patch("requests.put", side_effect=requests.ConnectionError)

    http_obj = HTTPTransput("dir1", "http://example.org/dir1", Type.Directory)

    assert http_obj.upload_dir() == 1


@pytest.mark.parametrize(
    "url, name, expected",
    [
        ("http://foo.bar/dir", "file.txt", "http://foo.bar/dir/file.txt"),
        ("http://foo.bar/dir/", "file.txt", "http://foo.bar/dir/file.txt"),
        ("http://foo.bar/dir", "result#1.txt", "http://foo.bar/dir/result%231.txt"),
        ("http://foo.bar/dir", "a?b.txt", "http://foo.bar/dir/a%3Fb.txt"),
        ("http://foo.bar/dir", "50%25.txt", "http://foo.bar/dir/50%2525.txt"),
        ("http://foo.bar/dir", "caf\u00e9.txt", "http://foo.bar/dir/caf%C3%A9.txt"),
        # A name that isn't valid UTF-8, as os.listdir returns it
        (
            "http://foo.bar/dir",
            os.fsdecode(b"caf\xe9.txt"),
            "http://foo.bar/dir/caf%E9.txt",
        ),
        (
            "http://foo.bar/dir?token=a%2Bb",
            "file.txt",
            "http://foo.bar/dir/file.txt?token=a%2Bb",
        ),
    ],
)
def test_http_join(url: str, name: str, expected: str) -> None:
    """Test that a file name is escaped and the URL's query is kept."""
    assert http_join(url, name) == expected


def test_upload_dir_stops_at_first_failure(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    """Test that a directory upload stops at the first file that fails."""
    (tmp_path / "a.txt").write_bytes(b"a")
    (tmp_path / "b.txt").write_bytes(b"b")
    resp_fail = Response()
    resp_fail.status_code = 500
    put_mock = mocker.patch("requests.put", return_value=resp_fail)

    http_obj = HTTPTransput(str(tmp_path), URL, Type.Directory)

    assert http_obj.upload_dir() == 1
    assert put_mock.call_count == 1
