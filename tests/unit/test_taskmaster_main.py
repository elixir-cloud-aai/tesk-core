"""Tests for how the taskmaster's `main` reads its configuration and task."""

import gzip
import io
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import taskmaster

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the module state that `main` sets, after each test.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    for name in ("args", "logger", "created_pvc", "max_queue", "poll_interval"):
        monkeypatch.setattr(taskmaster, name, getattr(taskmaster, name))


@pytest.fixture
def task() -> dict[str, Any]:
    """Get the task from the input file.

    Returns:
        The task.
    """
    task: dict[str, Any] = json.loads((RESOURCES / "inputFile.json").read_text())
    return task


@pytest.fixture
def mocks() -> Any:
    """Mock what `main` calls besides reading the task.

    Yields:
        The mocked Kubernetes configuration and run of the task.
    """
    with (
        patch("tesk_core.taskmaster.config") as mock_config,
        patch("tesk_core.taskmaster.init_own_job"),
        patch("tesk_core.taskmaster.check_cancelled", return_value=False),
        patch("tesk_core.taskmaster.run_task") as mock_run_task,
    ):
        yield mock_config, mock_run_task


def test_local_kube_config(
    mocks: tuple[MagicMock, MagicMock],
    task: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that `--localKubeConfig` loads the local kubeconfig."""
    mock_config, mock_run_task = mocks
    monkeypatch.setattr(
        "sys.argv", ["taskmaster", "--localKubeConfig", json.dumps(task)]
    )

    taskmaster.main()

    mock_config.load_kube_config.assert_called_once_with()
    mock_config.load_incluster_config.assert_not_called()
    assert mock_run_task.call_args.args[0] == task


def test_task_from_stdin(
    mocks: tuple[MagicMock, MagicMock],
    task: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that `-f -` reads the task from standard input."""
    _, mock_run_task = mocks
    monkeypatch.setattr("sys.argv", ["taskmaster", "-f", "-"])
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(task)))

    taskmaster.main()

    assert mock_run_task.call_args.args[0] == task
    assert mock_run_task.call_args.args[3] is False


def test_task_from_gzipped_file(
    mocks: tuple[MagicMock, MagicMock],
    task: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a gzipped task file is read, as the task's ConfigMap.

    The filer then reads the task from the ConfigMap too.
    """
    _, mock_run_task = mocks
    path = tmp_path / "task.json.gz"
    with gzip.open(path, "wt") as fh:
        json.dump(task, fh)
    monkeypatch.setattr("sys.argv", ["taskmaster", "-f", str(path)])

    taskmaster.main()

    assert mock_run_task.call_args.args[0] == task
    assert mock_run_task.call_args.args[3] is True


def test_task_from_file(
    mocks: tuple[MagicMock, MagicMock],
    task: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a plain task file is read."""
    _, mock_run_task = mocks
    path = tmp_path / "task.json"
    path.write_text(json.dumps(task))
    monkeypatch.setattr("sys.argv", ["taskmaster", "-f", str(path)])

    taskmaster.main()

    assert mock_run_task.call_args.args[0] == task
    assert mock_run_task.call_args.args[3] is False
