"""Tests for the filer's command line (`filer.main`)."""

from collections.abc import Iterator
import gzip
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import filer, path


@pytest.fixture
def prefix(tmp_path: Path) -> Iterator[Path]:
    """Mount the task's paths below a temporary directory.

    Args:
        tmp_path: The temporary directory.

    Yields:
        The directory.
    """
    with patch.object(path, "TASK_PATH_PREFIX", str(tmp_path)):
        yield tmp_path


@pytest.fixture
def log_config() -> Iterator[MagicMock]:
    """Mock the filer's logging setup, which changes the root logger.

    Yields:
        The mocked setup.
    """
    with patch("tesk_core.filer.logConfig") as mock:
        yield mock


def task(content: str) -> dict[str, Any]:
    """Describe a task with an input with content.

    Args:
        content: The input's content.

    Returns:
        The task.
    """
    return {
        "inputs": [{"path": "/a.txt", "type": "FILE", "content": content}],
        "outputs": [],
    }


def test_main_stages_inputs(
    prefix: Path, log_config: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the filer stages the inputs of the task on the command line."""
    monkeypatch.setattr("sys.argv", ["filer", "inputs", json.dumps(task("a"))])

    assert filer.main() == 0

    assert (prefix / "a.txt").read_text() == "a"
    log_config.assert_called_once_with(logging.ERROR)


def test_main_reads_gzipped_task(
    prefix: Path, log_config: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the filer reads a gzipped task file, with debug logging."""
    data = prefix / "task.json.gz"
    with gzip.open(data, "wt") as fh:
        json.dump(task("b"), fh)
    monkeypatch.setattr("sys.argv", ["filer", "-d", "inputs", str(data)])

    assert filer.main() == 0

    assert (prefix / "a.txt").read_text() == "b"
    log_config.assert_called_once_with(logging.DEBUG)


def test_main_fails_with_failed_input(
    prefix: Path,
    log_config: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that the filer exits with 1 at the first input that fails."""
    monkeypatch.setattr("sys.argv", ["filer", "inputs", json.dumps(task("a"))])

    with patch("tesk_core.filer.process_file", return_value=1) as mock_process:
        assert filer.main() == 1

    mock_process.assert_called_once()
    assert "Unable to process file, aborting" in caplog.text
