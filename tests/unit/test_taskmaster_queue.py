"""Tests for the max queue time of the taskmaster's jobs."""

from argparse import Namespace
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import taskmaster
from tesk_core.taskmaster import max_queue_seconds, new_job, run_task

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the module state and the setting.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(
            namespace="tesk", debug=False, pull_policy_always=False, pod_timeout=240
        ),
    )
    monkeypatch.setattr(taskmaster, "max_queue", None)
    monkeypatch.setattr(taskmaster, "owner_references", [])
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)
    monkeypatch.delenv(taskmaster.max_queue_env, raising=False)


@pytest.mark.parametrize(("value", "expected"), [("1", 1), (" 3600 ", 3600)])
def test_max_queue_seconds(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: int
) -> None:
    """Test that whole numbers of seconds from 1 set the max queue time.

    Args:
        monkeypatch: The fixture that sets the environment.
        value: The setting.
        expected: The max queue time.
    """
    monkeypatch.setenv(taskmaster.max_queue_env, value)

    assert max_queue_seconds() == expected


@pytest.mark.parametrize("value", [None, "", " "])
def test_max_queue_seconds_unset(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str | None,
) -> None:
    """Test that an unset or empty setting waits forever, quietly.

    Args:
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
        value: The setting, or `None` for unset.
    """
    if value is not None:
        monkeypatch.setenv(taskmaster.max_queue_env, value)

    assert max_queue_seconds() is None
    assert not caplog.records


@pytest.mark.parametrize("value", ["0", "-5", "1h", "2147483648", "9" * 5000])
def test_max_queue_seconds_invalid(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str,
) -> None:
    """Test that an invalid setting is logged, and pods wait forever.

    Args:
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
        value: The setting.
    """
    monkeypatch.setenv(taskmaster.max_queue_env, value)

    with caplog.at_level(logging.ERROR, logger=taskmaster.__name__):
        assert max_queue_seconds() is None

    assert taskmaster.max_queue_env in caplog.text
    assert "pods wait to be scheduled forever" in caplog.text


def test_new_job_max_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that new jobs get the max queue time, and are failed, not deleted.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "max_queue", 600)

    job = new_job({"metadata": {}}, "task-1234-inputs-filer")

    assert job.max_queue == 600
    assert not job.delete_unschedulable


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch(
    "tesk_core.taskmaster.Job.run_to_completion",
    autospec=True,
    return_value="Complete",
)
def test_run_task_unschedulable_executors_deleted(
    mock_run: MagicMock,
    mock_create: MagicMock,
    mock_delete: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that only executor jobs are deleted if they can't be scheduled.

    A deleted executor gives `SYSTEM_ERROR` in the TESK API, a failed one
    `EXECUTOR_ERROR`; failed filers give `SYSTEM_ERROR`.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "max_queue", 600)
    with (RESOURCES / "inputFile.json").open() as fh:
        data: dict[str, Any] = json.load(fh)
    data["inputs"] = [{"path": "/in/a", "url": "s3://b/a", "type": "FILE"}]
    data["outputs"] = [{"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}]

    run_task(data, "filer_name", "filer_version")

    jobs = {
        call.args[0].name: call.args[0].delete_unschedulable
        for call in mock_run.call_args_list
    }
    assert jobs == {
        "task-1000-inputs-filer": False,
        "task-1000-ex-00": True,
        "task-1000-outputs-filer": False,
    }
    assert all(call.args[0].max_queue == 600 for call in mock_run.call_args_list)


@patch("tesk_core.taskmaster.run_task")
@patch("tesk_core.taskmaster.check_cancelled", return_value=False)
@patch("tesk_core.taskmaster.config.load_incluster_config")
def test_main_reads_max_queue(
    mock_config: MagicMock,
    mock_cancelled: MagicMock,
    mock_run_task: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that `main` reads the max queue time for the task's jobs.

    Args:
        mock_config: The mocked loading of the Kubernetes configuration.
        mock_cancelled: The mocked check for cancellation.
        mock_run_task: The mocked run of the task.
        monkeypatch: The fixture that sets the environment.
    """
    monkeypatch.setenv(taskmaster.max_queue_env, "900")
    monkeypatch.setattr("sys.argv", ["taskmaster", "-n", "tesk", "{}"])

    taskmaster.main()

    assert taskmaster.max_queue == 900
    mock_run_task.assert_called_once()
