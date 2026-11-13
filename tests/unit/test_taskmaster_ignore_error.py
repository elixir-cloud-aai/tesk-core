"""Tests for executors whose errors are ignored (TES `ignore_error`)."""

from argparse import Namespace
from collections.abc import Callable
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest

from tesk_core import taskmaster
from tesk_core.job import Job
from tesk_core.taskmaster import (
    ignore_error_annotation,
    ignores_error,
    run_executor,
    run_task,
)

RESOURCES = Path(__file__).parent / "resources"
OUTPUT = {"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments, the created jobs and the claim of the taskmaster.

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


def task(*ignore: str | None) -> dict[str, Any]:
    """Get the task of the input file, with one executor per value.

    Args:
        *ignore: For each executor, the value of its annotation
            `ignore_error_annotation`; `None` for none.

    Returns:
        The task.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        data: dict[str, Any] = json.load(fh)
    template = json.dumps(data["executors"][0])
    data["executors"] = []
    for index, value in enumerate(ignore):
        executor = json.loads(template)
        name = f"task-1000-ex-{index:02d}"
        executor["metadata"]["name"] = name
        if value is not None:
            executor["metadata"]["annotations"] = {ignore_error_annotation: value}
        data["executors"].append(executor)
    return data


def statuses(failing: dict[str, str]) -> Callable[..., str]:
    """Mock `Job.run_to_completion` with the given statuses of some jobs.

    Args:
        failing: The status of each job that doesn't complete, by name;
            a name ending in `!` marks the job as deleted.

    Returns:
        The mock, which returns `Complete` for the other jobs.
    """

    def run(job: Job, *args: Any) -> str:
        if job.name + "!" in failing:
            job.deleted = True
            return failing[job.name + "!"]
        return failing.get(job.name, "Complete")

    return run


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({}, False),
        ({"annotations": None}, False),
        ({"annotations": {}}, False),
        ({"annotations": {ignore_error_annotation: "true"}}, True),
        ({"annotations": {ignore_error_annotation: "false"}}, False),
        ({"annotations": {ignore_error_annotation: "True"}}, False),
        ({"annotations": {ignore_error_annotation: ""}}, False),
    ],
)
def test_ignores_error(metadata: dict[str, Any], expected: bool) -> None:
    """Test that only the annotation's value `true` ignores errors.

    Args:
        metadata: The metadata of the executor's job.
        expected: Whether the executor's errors are ignored.
    """
    assert ignores_error({"metadata": metadata}) is expected


@patch("tesk_core.taskmaster.Job.run_to_completion", autospec=True)
def test_run_executor_ignores_failure(mock_run: MagicMock) -> None:
    """Test that a failed executor that ignores errors returns.

    The job is kept, so that the TESK API reports the executor's exit code
    and logs.

    Args:
        mock_run: The mocked run of the job.
    """
    mock_run.side_effect = statuses({"task-1000-ex-00": "Failed"})
    executor = task("true")["executors"][0]

    run_executor(executor, "default")

    assert [job.name for job in taskmaster.created_jobs] == ["task-1000-ex-00"]


@pytest.mark.parametrize(
    ("ignore", "failing"),
    [
        (None, {"task-1000-ex-00": "Failed"}),
        ("false", {"task-1000-ex-00": "Failed"}),
        ("true", {"task-1000-ex-00": "Cancelled"}),
        ("true", {"task-1000-ex-00!": "Failed"}),
    ],
)
@patch("tesk_core.taskmaster.delete_created_pvc")
@patch("tesk_core.taskmaster.Job.run_to_completion", autospec=True)
def test_run_executor_stops(
    mock_run: MagicMock,
    mock_delete_pvc: MagicMock,
    ignore: str | None,
    failing: dict[str, str],
) -> None:
    """Test that an executor stops the task unless it failed and ignores it.

    Errors of executors without the annotation `true` stop the task, and so
    do a cancellation and an executor whose job was deleted (it never ran:
    the TESK API reports `SYSTEM_ERROR`).

    Args:
        mock_run: The mocked run of the job.
        mock_delete_pvc: The mocked deletion of the claim.
        ignore: The executor's annotation `ignore_error_annotation`.
        failing: The status of the executor's job.
    """
    mock_run.side_effect = statuses(failing)
    executor = task(ignore)["executors"][0]

    with pytest.raises(SystemExit) as exit_info:
        run_executor(executor, "default")

    assert exit_info.value.code == 0
    mock_delete_pvc.assert_called_once_with()


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", autospec=True)
def test_run_task_continues_after_ignored_error(
    mock_run: MagicMock, mock_create: MagicMock, mock_delete: MagicMock
) -> None:
    """Test that the task goes on after an executor whose error is ignored.

    The later executors and the outputs filer run, and the claim is deleted
    as for a task that completed.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
    """
    mock_run.side_effect = statuses({"task-1000-ex-00": "Failed"})
    data = task("true", None)
    data["outputs"] = [OUTPUT]

    run_task(data, "filer_name", "filer_version")

    assert [job.name for job in taskmaster.created_jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
        "task-1000-ex-01",
        "task-1000-outputs-filer",
    ]
    mock_delete.assert_called_once()


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", autospec=True)
def test_run_task_stops_at_later_error(
    mock_run: MagicMock, mock_create: MagicMock, mock_delete: MagicMock
) -> None:
    """Test that a later executor without the annotation still stops the task.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
    """
    mock_run.side_effect = statuses(
        {"task-1000-ex-00": "Failed", "task-1000-ex-01": "Failed"}
    )
    data = task("true", None, None)

    with pytest.raises(SystemExit):
        run_task(data, "filer_name", "filer_version")

    assert [job.name for job in taskmaster.created_jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
        "task-1000-ex-01",
    ]


def new_job() -> Job:
    """Create a job with mocked Kubernetes clients.

    Returns:
        The job.
    """
    job = Job({"metadata": {}}, "task-1000-ex-00")
    job.bv1 = MagicMock()
    return job


def test_job_not_deleted() -> None:
    """Test that a new job isn't marked deleted."""
    assert new_job().deleted is False


@pytest.mark.parametrize("error", [None, ApiException(status=404)])
def test_delete_unscheduled_marks_deleted(error: ApiException | None) -> None:
    """Test that a job deleted as unschedulable, or gone, is marked deleted.

    Args:
        error: The error of the deletion, if any.
    """
    job = new_job()
    job.bv1.delete_namespaced_job.side_effect = error

    assert job.delete_unscheduled() == "Failed"
    assert job.deleted is True


@pytest.mark.parametrize("status", [403, 500])
def test_delete_unscheduled_not_deleted(status: int) -> None:
    """Test that a job whose deletion failed isn't marked deleted.

    A rejected deletion fails the job instead (and keeps it, if that
    succeeds); after other errors, the next poll tries again.

    Args:
        status: The status of the Kubernetes API's error.
    """
    job = new_job()
    job.bv1.delete_namespaced_job.side_effect = ApiException(status=status)

    assert job.delete_unscheduled() == "Running"
    assert job.deleted is False


def test_fail_rejected_marks_deleted() -> None:
    """Test that a job that can't be failed, and so is deleted, is marked."""
    job = new_job()
    job.bv1.patch_namespaced_job.side_effect = ApiException(status=403)

    assert job.fail() == "Failed"
    assert job.deleted is True
