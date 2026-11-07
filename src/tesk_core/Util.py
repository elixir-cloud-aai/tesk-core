"""Helpers for log messages."""

import json
from typing import Any

from tesk_core.transput import redact_urls

# Environment variables of job manifests whose values aren't logged
SECRET_ENV = frozenset({"TESK_FTP_USERNAME", "TESK_FTP_PASSWORD"})


def redact(data: Any) -> Any:
    """Redact data for log messages, e.g., a job manifest.

    Strips the URLs in all strings (see `redact_urls`), including the task
    in `JSON_INPUT`, and hides the values of the variables in `SECRET_ENV`.

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
        if data.get("name") in SECRET_ENV and "value" in data:
            return {
                **redact({k: v for k, v in data.items() if k != "value"}),
                "value": "<redacted>",
            }
        return {key: redact(value) for key, value in data.items()}
    return data


def pprint(data: Any) -> str:
    """Format data as indented JSON, for log messages, redacted (see `redact`).

    Args:
        data: Data that can be serialized as JSON.

    Returns:
        The redacted data as JSON, indented by four spaces.
    """
    return json.dumps(redact(data), indent=4)
