"""Tests of S3 directory downloads and streamed HTTP downloads."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import boto3
from moto import mock_aws
import pytest
from pytest_mock import MockerFixture
import requests

from tesk_core.filer import (
    ERROR_BODY_BYTES,
    HTTP_CHUNK_SIZE,
    HTTPTransput,
    error_body,
)
from tesk_core.filer_s3 import S3Transput
from tesk_core.transput import BODY_LOG_LIMIT, Type, within

KEYS = {
    "in/a.txt": b"a",
    "in/sub/b.txt": b"b",
    "in/empty/": b"",
    "in/": b"",
    "in/../../escape.txt": b"x",
    "in/sub/../../../escape2.txt": b"x",
    "in/../../escapedir/": b"",
    "in/../detour/../dir/back/c.txt": b"c",
}


@pytest.fixture
def bucket() -> Iterator[None]:
    """Mock S3 with a bucket `tesk` that holds `KEYS`.

    Yields:
        Nothing, while the mock is active.
    """
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="tesk")
        for key, body in KEYS.items():
            s3.Bucket("tesk").put_object(Key=key, Body=body)
        yield


@pytest.mark.usefixtures("bucket")
def test_s3_download_dir_keeps_objects_below(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that objects that would land outside the directory are skipped."""
    target = tmp_path / "work" / "dir"
    with S3Transput(str(target), "s3://tesk/in", Type.Directory) as transfer:
        assert transfer.download_dir() == 0
    assert (target / "a.txt").read_bytes() == b"a"
    assert (target / "sub" / "b.txt").read_bytes() == b"b"
    assert (target / "empty").is_dir()
    # The keys would land 1 and 2 levels above `target`, inside `tmp_path`
    assert not list(tmp_path.rglob("escape*"))
    assert "Skipping object 'in/../../escape.txt'" in caplog.text
    assert "Skipping object 'in/sub/../../../escape2.txt'" in caplog.text
    assert "Skipping object 'in/../../escapedir/'" in caplog.text
    # A detour through `..` segments creates no directories on the way
    assert (target / "back" / "c.txt").read_bytes() == b"c"
    assert not (tmp_path / "work" / "detour").exists()


def test_s3_download_dir_reads_all_pages(tmp_path: Path, mocker: MockerFixture) -> None:
    """Test that objects of every page of the listing are downloaded."""
    client = mocker.patch("boto3.client").return_value
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "in/a.txt"}]},
        {"Contents": [{"Key": "in/b.txt"}]},
        {},
    ]
    transfer = S3Transput(str(tmp_path / "dir"), "s3://tesk/in", Type.Directory)
    get = mocker.patch.object(transfer, "get_s3_file", return_value=0)
    assert transfer.download_dir() == 0
    client.get_paginator.assert_called_once_with("list_objects_v2")
    assert [call.args[1] for call in get.call_args_list] == ["in/a.txt", "in/b.txt"]


@pytest.mark.usefixtures("bucket")
def test_s3_download_dir_fails_with_an_object(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Test that a failed object download fails the directory."""
    with S3Transput(str(tmp_path / "dir"), "s3://tesk/in", Type.Directory) as t:
        mocker.patch.object(t, "get_s3_file", return_value=1)
        assert t.download_dir() == 1


def test_within(tmp_path: Path) -> None:
    """Test the containment check of paths, also through links."""
    parent = tmp_path / "dir"
    parent.mkdir()
    (parent / "link").symlink_to(tmp_path)
    assert within(str(parent), f"{parent}/a")
    assert not within(str(parent), str(parent))
    assert not within(str(parent), f"{parent}/../a")
    assert not within(str(parent), f"{parent}2/a")
    assert not within(str(parent), f"{parent}/link/a")


def streamed(chunks: list[bytes], status: int = 200) -> MagicMock:
    """Describe a streamed response.

    Args:
        chunks: The chunks of the body.
        status: The status code.

    Returns:
        The response, usable as a context manager.
    """
    response = MagicMock(
        status_code=status, url="https://example.org/in", history=[], encoding=None
    )
    response.__enter__.return_value = response
    response.iter_content.return_value = iter(chunks)
    return response


def test_http_download_streams(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a download is written in chunks, without the whole body."""
    response = streamed([b"first ", b"second"])
    get = mocker.patch("requests.get", return_value=response)
    path = tmp_path / "in.txt"
    assert HTTPTransput(str(path), "https://example.org/in", Type.File).download() == 0
    assert path.read_bytes() == b"first second"
    assert get.call_args.kwargs["stream"] is True
    response.iter_content.assert_called_once_with(chunk_size=HTTP_CHUNK_SIZE)


def test_http_download_error_while_streaming(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a connection that breaks off during the body fails the input."""

    def broken() -> Iterator[bytes]:
        """Yield a chunk, then fail.

        Yields:
            The first chunk.

        Raises:
            ChunkedEncodingError: Always, after the first chunk.
        """
        yield b"first"
        raise requests.exceptions.ChunkedEncodingError("broken")

    response = streamed([])
    response.iter_content.return_value = broken()
    mocker.patch("requests.get", return_value=response)
    transfer = HTTPTransput(str(tmp_path / "in"), "https://example.org/in", Type.File)
    assert transfer.download() == 1
    assert "ChunkedEncodingError" in caplog.text
    # The partial file is removed
    assert not (tmp_path / "in").exists()


def test_http_download_error_status_writes_nothing(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    """Test that an error status fails the input without writing the file."""
    mocker.patch("requests.get", return_value=streamed([b"error"], status=404))
    path = tmp_path / "in"
    assert HTTPTransput(str(path), "https://example.org/in", Type.File).download() == 1
    assert not path.exists()


def test_http_download_write_error(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a failed write fails the input and removes the partial file."""
    mocker.patch("requests.get", return_value=streamed([b"first", b"second"]))
    path = tmp_path / "in"
    real_open = open

    def full_volume(*args: object, **kwargs: object) -> MagicMock:
        """Open a file whose second write fails, as on a full volume.

        Args:
            *args: Passed to `open`.
            **kwargs: Passed to `open`.

        Returns:
            The file, as a mock around the real one.
        """
        file = real_open(*args, **kwargs)  # type: ignore[call-overload]
        wrapper = MagicMock(wraps=file)
        wrapper.__enter__.return_value = wrapper
        wrapper.__exit__.side_effect = file.__exit__
        wrapper.write.side_effect = [5, OSError(28, "No space left on device")]
        return wrapper

    mocker.patch("builtins.open", side_effect=full_volume)
    assert HTTPTransput(str(path), "https://example.org/in", Type.File).download() == 1
    assert not path.exists()
    assert "No space left on device" in caplog.text


def test_http_download_open_error_keeps_file(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a file that can't be opened fails the input, and stays."""
    mocker.patch("requests.get", return_value=streamed([b"new"]))
    path = tmp_path / "in"
    path.write_bytes(b"old")
    mocker.patch("builtins.open", side_effect=PermissionError(13, "Permission denied"))
    assert HTTPTransput(str(path), "https://example.org/in", Type.File).download() == 1
    assert "Unable to write input" in caplog.text
    assert path.read_bytes() == b"old"


def test_error_body_reads_only_the_start() -> None:
    """Test that only the start of a large error body is read."""
    big = b"x" * ERROR_BODY_BYTES
    response = streamed([big, big, b"never"], status=500)
    assert error_body(response) == big.decode()
    response.iter_content.assert_called_once_with(chunk_size=ERROR_BODY_BYTES)
    assert list(response.iter_content.return_value) == [big, b"never"]
    assert ERROR_BODY_BYTES > 4 * BODY_LOG_LIMIT


def test_http_error_body_redacts_values_at_the_cut(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a value that the log's cut splits is still redacted."""
    query_value = "s" * 200
    url = f"https://example.org/in?q={query_value}"
    body = "😀" * (BODY_LOG_LIMIT - 10) + query_value
    mocker.patch("requests.get", return_value=streamed([body.encode()], status=403))
    assert HTTPTransput(str(tmp_path / "in"), url, Type.File).download() == 1
    assert "s" * 4 not in caplog.text


def test_error_body_decoding() -> None:
    """Test that error bodies are decoded with a fallback to UTF-8."""
    response = streamed(["é".encode("latin-1")], status=500)
    response.encoding = "latin-1"
    assert error_body(response) == "é"
    response = streamed(["é".encode()], status=500)
    response.encoding = "no-such-codec"
    assert error_body(response) == "é"


def test_http_download_logs_error_body(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the error body of a streamed download is logged."""
    mocker.patch("requests.get", return_value=streamed([b"not found"], status=404))
    transfer = HTTPTransput(str(tmp_path / "in"), "https://example.org/in", Type.File)
    assert transfer.download() == 1
    assert "Response body: not found" in caplog.text
