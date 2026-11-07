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
from tesk_core.taskmaster import (
    clean_on_interrupt,
    exit_cancelled,
    keep_failed_pvc,
    run_task,
)

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
    # A logger that propagates to `caplog`, not one set up by `newLogger`
    monkeypatch.setattr(taskmaster, "logger", logging.getLogger("tesk_core.test"))
    monkeypatch.delenv(taskmaster.keep_failed_pvc_env, raising=False)


@pytest.fixture
def data() -> dict[str, Any]:
    """Get a task with inputs and outputs, so that it has a claim.

    Returns:
        The task.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        task: dict[str, Any] = json.load(fh)
    # An output, so that the outputs filer runs too
    task["outputs"] = [{"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}]
    return task


def assert_deleted_task_pvc(mock_delete: MagicMock, namespace: str = "default") -> None:
    """Assert that the task's claim was deleted once, with the timeout.

    Args:
        mock_delete: The mocked deletion of the claim, autospecced, so that
            it gets the claim.
        namespace: The namespace of the claim.
    """
    mock_delete.assert_called_once()
    deleted = mock_delete.call_args.args[0]
    assert (deleted.name, deleted.namespace) == ("task-1000-pvc", namespace)
    assert mock_delete.call_args.kwargs == {
        "request_timeout": taskmaster.pvc_delete_timeout
    }


def statuses(*names: str, status: str = "Failed") -> Callable[..., str]:
    """Mock `Job.run_to_completion` with a status per job name.

    Args:
        names: Names of the jobs that don't complete.
        status: The status of those jobs.

    Returns:
        The mock, which returns `Complete` for the other jobs.
    """

    def run_to_completion(job: Job, *args: Any) -> str:
        return status if job.name in names else "Complete"

    return run_to_completion


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
@pytest.mark.parametrize(
    "failing_job",
    ["task-1000-inputs-filer", "task-1000-ex-00", "task-1000-outputs-filer"],
)
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
def test_failed_job_deletes_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    failing_job: str,
    status: str,
    data: dict[str, Any],
) -> None:
    """Test that a job that doesn't complete deletes the claim, once.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        failing_job: The job that doesn't complete.
        status: Its status: failed, or cancelled while it ran.
        data: The task.
    """
    with (
        patch.object(Job, "run_to_completion", statuses(failing_job, status=status)),
        pytest.raises(SystemExit) as exit_info,
    ):
        run_task(data, "filer_name", "filer_version")

    assert exit_info.value.code == 0
    assert_deleted_task_pvc(mock_delete)
    assert taskmaster.created_pvc is None


@patch("tesk_core.taskmaster.check_cancelled")
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
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

    assert_deleted_task_pvc(mock_delete)
    assert [job.name for job in taskmaster.created_jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
    ]


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", statuses())
def test_completed_task_ignores_keep_failed_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    data: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a completed task deletes the claim also with `KEEP_FAILED_PVC`.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        data: The task.
        monkeypatch: The fixture that sets the environment.
    """
    monkeypatch.setenv(taskmaster.keep_failed_pvc_env, "true")

    run_task(data, "filer_name", "filer_version")

    assert_deleted_task_pvc(mock_delete)
    assert taskmaster.created_pvc is None


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", statuses())
def test_completed_task_delete_error_is_logged(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    data: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an error deleting the claim of a completed task is logged.

    The task completed, so the taskmaster doesn't fail (and isn't retried).

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        data: The task.
        caplog: The fixture that captures the log.
    """
    mock_delete.side_effect = ApiException(status=500, reason="Server Error")

    with caplog.at_level(logging.ERROR):
        run_task(data, "filer_name", "filer_version")

    assert "Can't delete PVC 'task-1000-pvc': 500 Server Error" in caplog.text


@patch("tesk_core.taskmaster.client")
@patch("tesk_core.taskmaster.config")
@patch("tesk_core.taskmaster.check_cancelled", return_value=True)
@patch("tesk_core.taskmaster.run_task")
def test_cancelled_during_init_deletes_pvc(
    mock_run_task: MagicMock,
    mock_cancelled: MagicMock,
    mock_config: MagicMock,
    mock_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a task cancelled before it starts deletes its claim.

    A retry of the taskmaster's pod after an unexpected error finds the claim
    of the earlier attempt; a first attempt finds none, which is no error.

    Args:
        mock_run_task: The mocked run of the task.
        mock_cancelled: The mocked check for cancellation.
        mock_config: The mocked Kubernetes configuration.
        mock_client: The mocked Kubernetes client.
        monkeypatch: The fixture that sets the command line.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        task = fh.read()
    monkeypatch.setattr("sys.argv", ["taskmaster", "-n", "tesk", task])

    with (
        patch.object(PVC, "delete", autospec=True) as mock_delete,
        pytest.raises(SystemExit),
    ):
        taskmaster.main()

    # The claim that `init_pvc` creates
    assert_deleted_task_pvc(mock_delete, namespace="tesk")
    mock_run_task.assert_not_called()


@pytest.mark.parametrize("value", ["true", "TRUE", " True "])
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
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
    monkeypatch.setenv(taskmaster.keep_failed_pvc_env, value)

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
        (" Yes ", False, True),
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
        monkeypatch.setenv(taskmaster.keep_failed_pvc_env, value)

    with caplog.at_level(logging.WARNING):
        assert keep_failed_pvc() is keep

    assert (f"KEEP_FAILED_PVC={value!r}" in caplog.text) is warns


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


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
def test_exit_without_pvc(mock_delete: MagicMock) -> None:
    """Test that a taskmaster without a claim (yet) deletes none.

    Args:
        mock_delete: The mocked deletion of the claim.
    """
    with pytest.raises(SystemExit):
        exit_cancelled("Cancelled during init")

    mock_delete.assert_not_called()


@patch("tesk_core.pvc.client")
def test_pvc_delete_timeout(mock_client: MagicMock) -> None:
    """Test that `PVC.delete` passes the request timeout, if any.

    Args:
        mock_client: The mocked Kubernetes client.
    """
    pvc = PVC("task-1000-pvc", 1, "tesk")
    api = mock_client.CoreV1Api.return_value

    pvc.delete()
    pvc.delete(request_timeout=(5, 30))

    first, second = api.delete_namespaced_persistent_volume_claim.call_args_list
    assert "_request_timeout" not in first.kwargs
    assert second.kwargs["_request_timeout"] == (5, 30)


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
def test_unexpected_error_keeps_pvc(
    mock_create: MagicMock, mock_delete: MagicMock, data: dict[str, Any]
) -> None:
    """Test that an unexpected error keeps the claim, for a retry of the pod.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        data: The task.
    """
    error = ApiException(status=500, reason="Server Error")
    with (
        patch.object(Job, "run_to_completion", side_effect=error),
        pytest.raises(ApiException),
    ):
        run_task(data, "filer_name", "filer_version")

    mock_delete.assert_not_called()
    assert taskmaster.created_pvc is not None


@patch("tesk_core.taskmaster.Job.delete")
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
def test_interrupt_keeps_pvc(
    mock_create: MagicMock,
    mock_delete: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that an interrupt deletes the jobs and keeps the claim.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        mock_job_delete: The mocked deletion of the jobs.
        data: The task.
    """
    with (
        patch.object(Job, "run_to_completion", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        run_task(data, "filer_name", "filer_version")
    clean_on_interrupt()

    mock_job_delete.assert_called_once_with()
    mock_delete.assert_not_called()


@patch("tesk_core.taskmaster.check_cancelled")
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch.object(Job, "run_to_completion", autospec=True, side_effect=statuses())
def test_cancelled_after_executors_without_outputs(
    mock_run: MagicMock,
    mock_create: MagicMock,
    mock_delete: MagicMock,
    mock_cancelled: MagicMock,
    data: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a task without outputs cancelled at its end stops as cancelled.

    No outputs filer runs, but the task still counts as cancelled, so that
    `KEEP_FAILED_PVC` keeps its claim.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        mock_cancelled: The mocked check for cancellation.
        data: The task.
        monkeypatch: The fixture that sets the environment.
    """
    data["outputs"] = []
    monkeypatch.setenv(taskmaster.keep_failed_pvc_env, "true")
    # Not cancelled before the executor, but after it
    mock_cancelled.side_effect = [False, True]

    with pytest.raises(SystemExit):
        run_task(data, "filer_name", "filer_version")

    # The executor ran to its end, and no outputs filer after it
    assert mock_run.call_args_list[-1].args[0].name == "task-1000-ex-00"
    assert mock_cancelled.call_count == 2
    mock_delete.assert_not_called()
