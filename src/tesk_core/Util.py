"""Helpers for log messages."""

import json
import logging
from typing import Any, TextIO

from tesk_core.transput import redact_urls, url_for_log

# Environment variables of job manifests whose values aren't logged
SECRET_ENV = frozenset({"TESK_FTP_USERNAME", "TESK_FTP_PASSWORD"})
# Loggers of the HTTP and S3 libraries, which log every request with its
# full URL and headers at `DEBUG`; they are kept at `WARNING` or above, so
# that debug logs show the messages of the taskmaster and the filer, whose
# URLs leave out queries and user info (see `url_for_log`)
LIBRARY_LOGGERS = ("urllib3", "botocore", "boto3", "s3transfer")
LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: int, stream: TextIO | None = None) -> None:
    """Configure the root logger, replacing any earlier configuration.

    Args:
        level: The log level of the root logger. The loggers in
            `LIBRARY_LOGGERS` get this level too, but at least `WARNING`.
        stream: The stream to log to; standard error if `None`.
    """
    logging.basicConfig(
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        level=level,
        stream=stream,
        force=True,
    )
    for name in LIBRARY_LOGGERS:
        logging.getLogger(name).setLevel(max(level, logging.WARNING))


def redact(data: Any) -> Any:
    """Redact data for log messages, e.g., a job manifest.

    Strips the URLs in all strings (see `redact_urls`) and the whole value
    of each `url` key (see `url_for_log`), also in the task in
    `JSON_INPUT` (re-serialized; as text if it isn't JSON), and hides the
    values of the variables in `SECRET_ENV`.
    Only environment entries with a literal `value` hold secrets: those
    with `valueFrom` only name a source.

    Args:
        data: Data that can be serialized as JSON; it isn't changed.

    Returns:
        A redacted copy of the data.
    """
    if isinstance(data, str):
        return redact_urls(data)
    if isinstance(data, (list, tuple)):
        return [redact(item) for item in data]
    if isinstance(data, dict):
        name = data.get("name")
        if isinstance(name, str) and "value" in data:
            rest = {key: value for key, value in data.items() if key != "value"}
            if name in SECRET_ENV:
                return {**redact(rest), "value": "<redacted>"}
            if name == "JSON_INPUT" and isinstance(data["value"], str):
                return {**redact(rest), "value": _redact_json(data["value"])}
        return {
            key: _redact_url(value) if key == "url" else redact(value)
            for key, value in data.items()
        }
    return data


def _redact_url(value: Any) -> Any:
    """Strip the value of a `url` key for log messages.

    Args:
        value: The value.

    Returns:
        The URL stripped (see `url_for_log`), `<URL>` if it can't be parsed,
        or other values redacted (see `redact`).
    """
    if not isinstance(value, str):
        return redact(value)
    try:
        return url_for_log(value)
    except ValueError:
        return "<URL>"


def _redact_json(text: str) -> str:
    """Redact data serialized as JSON, e.g., the task in `JSON_INPUT`.

    Args:
        text: The JSON.

    Returns:
        The redacted data as JSON, or the text redacted (see `redact_urls`)
        if it isn't JSON.
    """
    try:
        data = json.loads(text)
    except ValueError:
        return redact_urls(text)
    return json.dumps(redact(data))


def pprint(data: Any) -> str:
    """Format data as indented JSON, for log messages, redacted (see `redact`).

    Args:
        data: Data that can be serialized as JSON.

    Returns:
        The redacted data as JSON, indented by four spaces.
    """
    return json.dumps(redact(data), indent=4)
