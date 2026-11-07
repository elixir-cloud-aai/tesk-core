"""Helpers for log messages."""

import json
from typing import Any

from tesk_core.transput import redact_urls, url_for_log

# Environment variables of job manifests whose values aren't logged
SECRET_ENV = frozenset({"TESK_FTP_USERNAME", "TESK_FTP_PASSWORD"})


def redact(data: Any) -> Any:
    """Redact data for log messages, e.g., a job manifest.

    Strips the URLs in all strings (see `redact_urls`) and the whole value
    of each `url` key (see `url_for_log`), also in the task in
    `JSON_INPUT`, and hides the values of the variables in `SECRET_ENV`.
    Only environment entries with a literal `value` hold secrets: those
    with `valueFrom` only name a source.

    Args:
        data: Data that can be serialized as JSON; it isn't changed.

    Returns:
        A redacted copy of the data.
    """
    if isinstance(data, str):
        return redact_urls(data)
    if isinstance(data, list):
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
