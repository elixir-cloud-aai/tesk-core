"""Tests of names that the URL of a directory entry can't carry (#146).

S3 and FTP directory uploads build each entry's URL by appending its name
to the directory's URL, and take the URL's path as it is (`urlparse`), which
ends at `#` and `?` (and, for FTP, at `;`). Such names fail the output
before anything is uploaded, as for the matches of wildcard outputs.
"""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_aws
import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import process_file
from tesk_core.transput import Type, unsafe_entry


@pytest.fixture
def bucket() -> Iterator[None]:
    """Mock S3 with an empty bucket `tesk`.

    Yields:
        Nothing, while the mock is active.
    """
    with mock_aws():
        boto3.resource("s3", endpoint_url="http://s3.amazonaws.com").create_bucket(
            Bucket="tesk"
        )
        yield


def keys() -> list[str]:
    """List the keys in the bucket `tesk`.

    Returns:
        The keys, sorted.
    """
    s3 = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    return sorted(obj.key for obj in s3.Bucket("tesk").objects.all())


def write(path: Path, text: str = "x") -> None:
    """Write a file, creating its directory.

    Args:
        path: The file.
        text: Its content.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def directory(path: Path, url: str) -> dict[str, str]:
    """Describe a DIRECTORY output.

    Args:
        path: Its path.
        url: Its URL.

    Returns:
        The output, as the filer gets it.
    """
    return {"path": str(path), "url": url, "type": Type.Directory.value}


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
@pytest.mark.parametrize(
    "name, refused",
    [
        ("a#1", "a#1"),
        ("a?1", "a?1"),
        ("a\nb", "a\nb"),
        ("a\tb", "a\tb"),
        ("a\rb", "a\rb"),
        ("sub/c#d.txt", "sub/c#d.txt"),
        ("d#2/x.txt", "d#2"),
    ],
)
def test_s3_directory_entry_names_are_refused(
    tmp_path: Path, name: str, refused: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that an S3 directory output with such a name uploads nothing.

    Before the fix, `a#1` was uploaded to the key `out/a`, and `a<LF>b` to
    `out/ab`, and the output succeeded. Entries that sort before the
    refused one aren't uploaded either, and the error names the file or
    directory.
    """
    write(tmp_path / "0.txt")
    write(tmp_path / name)

    assert process_file("outputs", directory(tmp_path, "s3://tesk/out")) == 1
    assert keys() == []
    assert repr(str(tmp_path / refused)) in caplog.text


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.parametrize(
    "name, refused",
    [
        ("a#1", "a#1"),
        ("a?1", "a?1"),
        ("a;1", "a;1"),
        ("a\nb", "a\nb"),
        ("sub/b;x.txt", "sub/b;x.txt"),
        ("d;2/x.txt", "d;2"),
    ],
)
def test_ftp_directory_entry_names_are_refused(
    tmp_path: Path,
    name: str,
    refused: str,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an FTP directory output with such a name uploads nothing.

    FTP URLs also end their last path segment at `;` (parameters).
    """
    ftp = mocker.patch("tesk_core.filer.FTP")
    mocker.patch("tesk_core.filer.ftp_login")
    upload = mocker.patch("tesk_core.filer.ftp_upload_file", return_value=0)
    make_dirs = mocker.patch("tesk_core.filer.ftp_make_dirs", return_value=0)
    write(tmp_path / "0.txt")
    write(tmp_path / name)

    filedata = directory(tmp_path, "ftp://ftp.example.org/out")
    assert process_file("outputs", filedata) == 1
    upload.assert_not_called()
    make_dirs.assert_not_called()
    ftp.return_value.storbinary.assert_not_called()
    assert repr(str(tmp_path / refused)) in caplog.text


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_s3_directory_with_semicolon_is_uploaded(tmp_path: Path) -> None:
    """Test that S3 keys keep `;`, which only FTP URLs end at."""
    write(tmp_path / "a;1")

    assert process_file("outputs", directory(tmp_path, "s3://tesk/out")) == 0
    assert keys() == ["out/a;1"]


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_s3_wildcard_match_with_semicolon_is_uploaded(tmp_path: Path) -> None:
    """Test that S3 wildcard matches keep `;` too."""
    write(tmp_path / "res;1" / "a;1.txt")

    filedata = {
        "path": f"{tmp_path}/res*",
        "path_prefix": str(tmp_path),
        "url": "s3://tesk/out",
        "type": Type.Directory.value,
    }
    assert process_file("outputs", filedata) == 0
    assert keys() == ["out/res;1/a;1.txt"]


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_s3_wildcard_match_with_line_break_is_refused(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a wildcard match with a line break fails an S3 output."""
    write(tmp_path / "a\nb.txt")

    filedata = {
        "path": f"{tmp_path}/*.txt",
        "path_prefix": str(tmp_path),
        "url": "s3://tesk/out",
        "type": Type.File.value,
    }
    assert process_file("outputs", filedata) == 1
    assert keys() == []
    assert repr(str(tmp_path / "a\nb.txt")) in caplog.text


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
def test_http_directory_entry_names_are_escaped(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Test that HTTP(S) directory uploads keep such names, escaped."""
    put = mocker.patch("tesk_core.filer.requests.put")
    put.return_value.status_code = 201
    write(tmp_path / "a#1")

    filedata = directory(tmp_path, "https://example.org/out")
    assert process_file("outputs", filedata) == 0
    assert put.call_args.args[0] == "https://example.org/out/a%231"


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_s3_wildcard_directory_match_entries_are_refused(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that entries below a matched directory are checked too."""
    write(tmp_path / "res_0" / "a.txt")
    write(tmp_path / "res_1" / "b#1.txt")

    filedata = {
        "path": f"{tmp_path}/res_*",
        "path_prefix": str(tmp_path),
        "url": "s3://tesk/out",
        "type": Type.Directory.value,
    }
    assert process_file("outputs", filedata) == 1
    assert keys() == []
    assert str(tmp_path / "res_1" / "b#1.txt") in caplog.text


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.parametrize("pattern", ["*.txt", "res_*"])
def test_ftp_wildcard_match_with_semicolon_is_refused(
    tmp_path: Path,
    pattern: str,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that `;` in a match, or below a matched directory, fails FTP outputs."""
    mocker.patch("tesk_core.filer.FTP")
    mocker.patch("tesk_core.filer.ftp_login")
    upload = mocker.patch("tesk_core.filer.ftp_upload_file", return_value=0)
    write(tmp_path / "a.txt")
    write(tmp_path / "b;1.txt")
    write(tmp_path / "res_1" / "b;1.txt")

    filedata = {
        "path": f"{tmp_path}/{pattern}",
        "path_prefix": str(tmp_path),
        "url": "ftp://ftp.example.org/out",
        "type": Type.File.value,
    }
    assert process_file("outputs", filedata) == 1
    upload.assert_not_called()
    assert "b;1.txt" in caplog.text


def test_unsafe_entry(tmp_path: Path) -> None:
    """Test that the first entry with such a name is found, by sorted path."""
    write(tmp_path / "b" / "c#1")
    write(tmp_path / "a?1")
    (tmp_path / "d#2").mkdir()

    assert unsafe_entry(str(tmp_path), "#?") == str(tmp_path / "a?1")
    assert unsafe_entry(str(tmp_path / "b"), "#?") == str(tmp_path / "b" / "c#1")
    assert unsafe_entry(str(tmp_path), "") is None
    assert unsafe_entry(str(tmp_path / "missing"), "#?") is None


def test_file_directory_entry_names_are_copied(tmp_path: Path) -> None:
    """Test that `file:` directory outputs copy such names as they are."""
    shared = tmp_path / "shared"
    shared.mkdir()
    work = tmp_path / "work"
    write(work / "a#1;2?")
    with (
        patch("tesk_core.path.TASK_PATH_PREFIX", "/"),
        patch("tesk_core.path.HOST_BASE_PATH", str(shared)),
        patch("tesk_core.path.CONTAINER_BASE_PATH", str(shared)),
    ):
        assert process_file("outputs", directory(work, f"file://{shared}/out")) == 0
    assert (shared / "out" / "a#1;2?").read_text() == "x"


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
def test_http_wildcard_match_names_are_escaped(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Test that HTTP(S) wildcard matches keep such names, escaped."""
    put = mocker.patch("tesk_core.filer.requests.put")
    put.return_value.status_code = 201
    write(tmp_path / "a#1;2.txt")

    filedata = {
        "path": f"{tmp_path}/*.txt",
        "path_prefix": str(tmp_path),
        "url": "https://example.org/out",
        "type": Type.File.value,
    }
    assert process_file("outputs", filedata) == 0
    assert put.call_args.args[0] == "https://example.org/out/a%231%3B2.txt"
