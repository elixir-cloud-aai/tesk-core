"""Tests for the deletion of the task's claim when the taskmaster stops."""

from argparse import Namespace
from collections.abc import Callable
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import HTTPError

from tesk_core import taskmaster
from tesk_core.job import Job
from tesk_core.pvc import PVC
from tesk_core.taskmaster import exit_cancelled, keep_failed_pvc, run_task

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments, created jobs and claim of the taskmaster module.

    Also unsets `KEEP_FAILED_PVC`, so that the environment of the test run
    doesn't count.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(
            debug=False, namespace="default", pull_policy_always=False, pod_timeout=240
        ),
    )
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)
    monkeypatch.setattr(taskmaster, "logger", logging.getLogger("tesk_core.test"))
    monkeypatch.delenv("KEEP_FAILED_PVC", raising=False)


@pytest.fixture
def data() -> dict[str, Any]:
    """Get a task with inputs and outputs, so that it has a claim.

    Returns:
        The task.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        task: dict[str, Any] = json.load(fh)
    return task


def statuses(*names: str) -> Callable[..., str]:
    """Mock `Job.run_to_completion` with a status per job name.

    Args:
        names: Names of the jobs that don't complete; they get `Failed`.

    Returns:
        The mock, which returns `Complete` for the other jobs.
    """

    def run_to_completion(job: Job, *args: Any) -> str:
        return "Failed" if job.name in names else "Complete"

    return run_to_completion


@pytest.mark.parametrize(
    "failing_job",
    ["task-1000-inputs-filer", "task-1000-ex-00", "task-1000-outputs-filer"],
)
@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.PVC.create")
def test_failed_job_deletes_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    failing_job: str,
    data: dict[str, Any],
) -> None:
    """Test that a job that doesn't complete deletes the claim, once.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        failing_job: The job that fails.
        data: The task.
    """
    with (
        patch.object(Job, "run_to_completion", statuses(failing_job)),
        pytest.raises(SystemExit) as exit_info,
    ):
        run_task(data, "filer_name", "filer_version")

    assert exit_info.value.code == 0
    mock_delete.assert_called_once_with()
    assert taskmaster.created_pvc is None


@patch("tesk_core.taskmaster.check_cancelled")
@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", statuses())
def test_cancelled_task_deletes_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    mock_cancelled: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a task cancelled between two jobs deletes the claim.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        mock_cancelled: The mocked check for cancellation.
        data: The task.
    """
    # Not cancelled before the executor, but before the outputs filer
    mock_cancelled.side_effect = [False, True]

    with pytest.raises(SystemExit):
        run_task(data, "filer_name", "filer_version")

    mock_delete.assert_called_once_with()
    assert [job.name for job in taskmaster.created_jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
    ]


@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", statuses())
def test_completed_task_deletes_pvc_once(
    mock_create: MagicMock, mock_delete: MagicMock, data: dict[str, Any]
) -> None:
    """Test that a completed task deletes the claim once, and forgets it.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        data: The task.
    """
    run_task(data, "filer_name", "filer_version")

    mock_delete.assert_called_once_with()
    assert taskmaster.created_pvc is None


@pytest.mark.parametrize("value", ["true", "TRUE", " True "])
@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", statuses("task-1000-ex-00"))
def test_keep_failed_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    value: str,
    data: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that `KEEP_FAILED_PVC=true` keeps the claim of a failed task.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        value: The value of `KEEP_FAILED_PVC`.
        data: The task.
        monkeypatch: The fixture that sets the environment.
    """
    monkeypatch.setenv("KEEP_FAILED_PVC", value)

    with pytest.raises(SystemExit):
        run_task(data, "filer_name", "filer_version")

    mock_delete.assert_not_called()


@pytest.mark.parametrize(
    ("value", "keep", "warns"),
    [
        (None, False, False),
        ("", False, False),
        ("false", False, False),
        ("False", False, False),
        ("true", True, False),
        ("yes", False, True),
        ("1", False, True),
    ],
)
def test_keep_failed_pvc_values(
    value: str | None,
    keep: bool,
    warns: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test which values of `KEEP_FAILED_PVC` keep the claim.

    Only `true` (in any case) keeps it; values other than `true` and `false`
    log a warning.

    Args:
        value: The value of `KEEP_FAILED_PVC`, or `None` for unset.
        keep: Whether the claim is kept.
        warns: Whether a warning is logged.
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
    """
    if value is not None:
        monkeypatch.setenv("KEEP_FAILED_PVC", value)

    with caplog.at_level(logging.WARNING):
        assert keep_failed_pvc() is keep

    assert ("KEEP_FAILED_PVC" in caplog.text) is warns


@pytest.mark.parametrize(
    "error",
    [ApiException(status=403, reason="Forbidden"), HTTPError("connection refused")],
)
def test_delete_error_is_logged(
    error: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that an error deleting the claim is logged, and the taskmaster exits.

    Args:
        error: The error of the deletion.
        caplog: The fixture that captures the log.
    """
    pvc = MagicMock(spec=PVC)
    pvc.name = "task-1000-pvc"
    pvc.delete.side_effect = error
    taskmaster.created_pvc = pvc

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exit_info:
        exit_cancelled("Got status Failed")

    assert exit_info.value.code == 0
    assert "Can't delete PVC 'task-1000-pvc'" in caplog.text
    assert taskmaster.created_pvc is None


def test_missing_pvc_is_not_an_error(caplog: pytest.LogCaptureFixture) -> None:
    """Test that a claim that is already gone isn't logged as an error.

    Args:
        caplog: The fixture that captures the log.
    """
    pvc = MagicMock(spec=PVC)
    pvc.name = "task-1000-pvc"
    pvc.delete.side_effect = ApiException(status=404, reason="Not Found")
    taskmaster.created_pvc = pvc

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit):
        exit_cancelled("Got status Failed")

    assert "Can't delete PVC" not in caplog.text


@patch("tesk_core.taskmaster.PVC.delete")
def test_exit_without_pvc(mock_delete: MagicMock) -> None:
    """Test that a taskmaster without a claim (yet) deletes none.

    Args:
        mock_delete: The mocked deletion of the claim.
    """
    with pytest.raises(SystemExit):
        exit_cancelled("Cancelled during init")

    mock_delete.assert_not_called()
