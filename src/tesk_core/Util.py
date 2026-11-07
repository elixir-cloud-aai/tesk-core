"""Helpers for log messages."""

import json
from typing import Any


def pprint(data: Any) -> str:
    """Format data as indented JSON, for log messages.

    Args:
        data: Data that can be serialized as JSON.

    Returns:
        The data as JSON, indented by four spaces.
    """
    return json.dumps(data, indent=4)
