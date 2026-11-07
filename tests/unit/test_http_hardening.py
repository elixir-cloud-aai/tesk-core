"""Tests for the timeout of the filer's HTTP requests and its log messages."""

from collections.abc import Iterator
import json
import logging
import os
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from tesk_core.filer import (
    HTTP_TIMEOUT_DEFAULT,
    HTTP_TIMEOUT_ENV,
    HTTPTransput,
    Type,
    http_timeout,
    log_invalid_http_timeout,
)
from tesk_core.filer_class import Filer
from tesk_core.transput import BODY_LOG_LIMIT, body_for_log, redact_urls
from tesk_core.Util import pprint

SIGNATURE = "c2lnbmF0dXJlLXZhbHVl"
URL = f"https://example.org/bucket/a.txt?X-Amz-Signature={SIGNATURE}&x=1"


@pytest.fixture(autouse=True)
def _clear_log_cache() -> Iterator[None]:
    """Log the invalid timeout again in each test.

    Yields:
        Nothing; clears the cache before and after the test.
    """
    log_invalid_http_timeout.cache_clear()
    yield
    log_invalid_http_timeout.cache_clear()


def _response(status: int, body: bytes = b"") -> Response:
    """Get a `requests` response.

    Args:
        status: The status code.
        body: The body.

    Returns:
        The response.
    """
    resp = Response()
    resp.status_code = status
    resp._content = body
    return resp


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, HTTP_TIMEOUT_DEFAULT),
        ("", HTTP_TIMEOUT_DEFAULT),
        ("30", 30.0),
        (" 12.5 ", 12.5),
    ],
)
def test_http_timeout(value: str | None, expected: float) -> None:
    """Test the timeout from the environment, and its default.

    Args:
        value: The value of the environment variable; unset if `None`.
        expected: The expected timeout, in seconds.
    """
    env = {} if value is None else {HTTP_TIMEOUT_ENV: value}
    with mock.patch.dict(os.environ, env, clear=True):
        assert http_timeout() == expected


@pytest.mark.parametrize("value", ["abc", "0", "-1", "inf", "nan", "1e999"])
def test_http_timeout_invalid(value: str, caplog: pytest.LogCaptureFixture) -> None:
    """Test that an invalid timeout falls back to the default, logged once.

    Args:
        value: The invalid value.
        caplog: The log capture fixture.
    """
    with mock.patch.dict(os.environ, {HTTP_TIMEOUT_ENV: value}, clear=True):
        assert http_timeout() == HTTP_TIMEOUT_DEFAULT
        assert http_timeout() == HTTP_TIMEOUT_DEFAULT
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert HTTP_TIMEOUT_ENV in errors[0].getMessage()


def test_download_passes_timeout(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that downloads use the timeout.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
    """
    get = mocker.patch("requests.get", return_value=_response(200))
    with mock.patch.dict(os.environ, {HTTP_TIMEOUT_ENV: "7"}):
        assert HTTPTransput(str(tmp_path / "a"), URL, Type.File).download_file() == 0
    get.assert_called_once_with(URL, timeout=7.0)


def test_upload_passes_timeout(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that uploads use the timeout.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
    """
    path = tmp_path / "a"
    path.write_bytes(b"x")
    put = mocker.patch("requests.put", return_value=_response(200))
    with mock.patch.dict(os.environ, {HTTP_TIMEOUT_ENV: "7"}):
        assert HTTPTransput(str(path), URL, Type.File).upload_file() == 0
    assert put.call_args.kwargs["timeout"] == 7.0


@pytest.mark.parametrize("method", ["get", "put"])
def test_timeout_fails_transfer(
    mocker: MockerFixture,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    method: str,
) -> None:
    """Test that a timed-out request fails the transfer, with a logged error.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        caplog: The log capture fixture.
        method: The request method.
    """
    path = tmp_path / "a"
    path.write_bytes(b"x")
    mocker.patch(f"requests.{method}", side_effect=requests.ReadTimeout(URL))
    transfer = HTTPTransput(str(path), URL, Type.File)
    result = transfer.download_file() if method == "get" else transfer.upload_file()
    assert result == 1
    assert "ReadTimeout" in caplog.text
    assert SIGNATURE not in caplog.text


@pytest.mark.parametrize("method", ["get", "put"])
def test_error_body_redacted(
    mocker: MockerFixture,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    method: str,
) -> None:
    """Test that error bodies are logged without URL queries or secrets.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        caplog: The log capture fixture.
        method: The request method.
    """
    path = tmp_path / "a"
    path.write_bytes(b"x")
    body = (
        "<Error><Code>SignatureDoesNotMatch</Code>"
        f"<SignatureProvided>{SIGNATURE}</SignatureProvided>"
        f"<Resource>{URL}</Resource></Error>"
    )
    mocker.patch(f"requests.{method}", return_value=_response(403, body.encode()))
    transfer = HTTPTransput(str(path), URL, Type.File)
    result = transfer.download_file() if method == "get" else transfer.upload_file()
    assert result == 1
    assert "SignatureDoesNotMatch" in caplog.text
    assert "https://example.org/bucket/a.txt" in caplog.text
    assert SIGNATURE not in caplog.text


def test_error_body_truncated(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that long error bodies are truncated in the log.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        caplog: The log capture fixture.
    """
    body = "x" * (BODY_LOG_LIMIT + 500)
    mocker.patch("requests.get", return_value=_response(500, body.encode()))
    HTTPTransput(str(tmp_path / "a"), URL, Type.File).download_file()
    assert "x" * (BODY_LOG_LIMIT + 1) not in caplog.text
    assert "500 more characters" in caplog.text


def test_empty_error_body_not_logged(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that an empty error body logs no body line.

    Args:
        mocker: The mocker fixture.
        tmp_path: The temporary directory of the test.
        caplog: The log capture fixture.
    """
    mocker.patch("requests.get", return_value=_response(500))
    HTTPTransput(str(tmp_path / "a"), URL, Type.File).download_file()
    assert "Got status code: 500" in caplog.text
    assert "Response body" not in caplog.text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("see https://h.example.org/a?t=1 now", "see https://h.example.org/a now"),
        ("ftp://u:pw@h.example.org/d", "ftp://h.example.org/d"),
        ('"s3://b/k?v=2"', '"s3://b/k"'),
        ("http://[bad/x?y", "<URL>"),
        ("no url here", "no url here"),
    ],
)
def test_redact_urls(text: str, expected: str) -> None:
    """Test that URLs in text lose their queries and user information.

    Args:
        text: The text.
        expected: The text with the URLs redacted.
    """
    assert redact_urls(text) == expected


def test_body_for_log_secrets_of_request() -> None:
    """Test that the request's query values and password are redacted."""
    url = "https://user:topsecret@example.org/a?token=a%2Bb%3D&short=1"
    body = "token a+b= or a%2Bb%3D, password topsecret, short 1"
    logged = body_for_log(body, url)
    assert "a+b=" not in logged
    assert "a%2Bb%3D" not in logged
    assert "topsecret" not in logged
    # Values shorter than 4 characters are kept
    assert logged.endswith("short 1")


def test_body_for_log_limit() -> None:
    """Test that bodies are cut at the limit, after redaction."""
    assert body_for_log("abcdef", "https://example.org/", limit=4) == (
        "abcd... (2 more characters)"
    )
    assert body_for_log("abcd", "https://example.org/", limit=4) == "abcd"


@mock.patch.dict(os.environ, {HTTP_TIMEOUT_ENV: "45"})
def test_filer_passes_timeout() -> None:
    """Test that the taskmaster passes the timeout to the filer."""
    variables = {var["name"]: var["value"] for var in Filer("n", {}).getEnv()}
    assert variables[HTTP_TIMEOUT_ENV] == "45"


@mock.patch.dict(os.environ, clear=True)
def test_filer_without_timeout() -> None:
    """Test that no timeout is passed if the taskmaster has none."""
    names = [var["name"] for var in Filer("n", {}).getEnv()]
    assert HTTP_TIMEOUT_ENV not in names


def test_pprint_redacts_job_manifest() -> None:
    """Test that job manifests are logged without URL queries or credentials."""
    task = {"inputs": [{"url": URL, "path": "/data/a.txt"}]}
    body: dict[str, Any] = {
        "spec": {
            "containers": [
                {
                    "command": ["curl", "https://u:p@example.org/x?k=v"],
                    "env": [
                        {"name": "JSON_INPUT", "value": json.dumps(task)},
                        {"name": "TESK_FTP_USERNAME", "value": "ftpuser"},
                        {"name": "TESK_FTP_PASSWORD", "value": "ftppass"},
                        {"name": "HOST_BASE_PATH", "value": "/transfer"},
                    ],
                }
            ]
        }
    }
    original = json.dumps(body)
    logged = pprint(body)
    assert SIGNATURE not in logged
    assert "k=v" not in logged
    assert "u:p@" not in logged
    assert "ftpuser" not in logged
    assert "ftppass" not in logged
    assert "https://example.org/bucket/a.txt" in logged
    assert "/transfer" in logged
    # The manifest itself is unchanged
    assert json.dumps(body) == original
