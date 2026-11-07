"""Tests for running jobs to completion and for the status of jobs."""

from argparse import Namespace
import datetime
from datetime import timezone
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import ProtocolError

from tesk_core import taskmaster
from tesk_core.job import (
    DELETE_TIMEOUT,
    FAIL_DEADLINE,
    Job,
    cannot_start,
    unschedulable,
)

START_TIME = datetime.datetime.now(timezone.utc)
RESOURCES = Path(__file__).parent / "resources"


class MockObject:
    """Object whose attributes are the entries of a dictionary.

    Nested dictionaries become nested objects.
    """

    def __init__(self, dictionary: dict[str, Any]) -> None:
        """Create the object.

        Args:
            dictionary: The attributes.
        """
        for k, v in dictionary.items():
            if isinstance(v, dict):
                self.__dict__[k] = MockObject(v)
            else:
                self.__dict__[k] = v


def read_namespaced_job_error(name: str, namespace: str) -> MockObject:
    """Mock reading a job that has been active for ten minutes.

    Args:
        name: The name of the job.
        namespace: The namespace of the job.

    Returns:
        The job with an active status.
    """
    return_value = {
        "active": 1,
        "completion_time": None,
        "conditions": None,
        "failed": None,
        "start_time": START_TIME - datetime.timedelta(minutes=10),
        "succeeded": None,
    }
    return MockObject({"status": return_value})


def read_namespaced_job_pending(diff_time: int = 5) -> MockObject:
    """Mock reading a pending job.

    Args:
        diff_time: The minutes since the start of the job.

    Returns:
        The job with an active status.
    """
    return_value = {
        "active": 1,
        "completion_time": None,
        "conditions": None,
        "failed": None,
        "start_time": START_TIME - datetime.timedelta(minutes=diff_time),
        "succeeded": None,
    }
    return MockObject({"status": return_value})


def read_namespaced_job_success(name: str, namespace: str) -> MockObject:
    """Mock reading a job that completed.

    Args:
        name: The name of the job.
        namespace: The namespace of the job.

    Returns:
        The job with the condition `Complete`.
    """
    return_value = {
        "active": None,
        "completion_time": datetime.datetime(
            2020, 7, 20, 5, 12, 42, tzinfo=timezone.utc
        ),
        "conditions": [
            MockObject(
                {
                    "last_probe_time": datetime.datetime(
                        2020, 7, 20, 5, 12, 42, tzinfo=timezone.utc
                    ),
                    "last_transition_time": datetime.datetime(
                        2020, 7, 20, 5, 12, 42, tzinfo=timezone.utc
                    ),
                    "message": None,
                    "reason": None,
                    "status": "True",
                    "type": "Complete",
                }
            )
        ],
        "failed": None,
        "start_time": datetime.datetime(2020, 7, 20, 5, 12, 35, tzinfo=timezone.utc),
        "succeeded": 1,
    }
    return MockObject({"status": return_value})


def read_namespaced_job_running(name: str, namespace: str) -> MockObject:
    """Mock reading a running job.

    If the `conditions` value is `None`, it is assumed that the pod is
    running. Hence the value of `conditions` is kept as `None`.

    Args:
        name: The name of the job.
        namespace: The namespace of the job.

    Returns:
        The job without conditions.
    """
    return_value = {
        "active": None,
        "completion_time": None,
        "conditions": None,
        "failed": None,
        "start_time": datetime.datetime(2020, 7, 20, 5, 12, 35, tzinfo=timezone.utc),
        "succeeded": None,
    }
    return MockObject({"status": return_value})


def list_namespaced_pod_error_ImagePullBackOff(diff_time: int = 10) -> MockObject:
    """Mock listing a pod that waits because of `ImagePullBackOff`.

    Args:
        diff_time: The minutes since the start of the pod.

    Returns:
        The list with the pending pod.
    """
    return_value = {
        "metadata": {"name": "task-1000-ex-00-pod"},
        "status": {
            "conditions": [],
            "container_statuses": [
                MockObject(
                    {
                        "container_id": None,
                        "image": "ubuntu_mock_test_image",
                        "image_id": "",
                        "last_state": {
                            "running": None,
                            "terminated": None,
                            "waiting": None,
                        },
                        "name": "task-1000-ex-00",
                        "ready": False,
                        "restart_count": 0,
                        "state": {
                            "running": None,
                            "terminated": None,
                            "waiting": {
                                "message": "Back-off "
                                "pulling "
                                "image "
                                "ubuntu_mock_test_image",
                                "reason": "ImagePullBackOff",
                            },
                        },
                    }
                )
            ],
            "host_ip": "192.168.99.100",
            "init_container_statuses": None,
            "message": None,
            "nominated_node_name": None,
            "phase": "Pending",
            "pod_ip": "172.17.0.5",
            "qos_class": "BestEffort",
            "reason": None,
            "start_time": START_TIME - datetime.timedelta(minutes=diff_time),
        },
    }
    return MockObject({"items": [MockObject(return_value)]})


def list_namespaced_pod_pending_unknown_error(diff_time: int = 5) -> MockObject:
    """Mock listing a pod that waits for an unknown reason.

    Args:
        diff_time: The minutes since the start of the pod.

    Returns:
        The list with the pending pod.
    """
    return_value = {
        "metadata": {"name": "task-1000-ex-00-pod"},
        "status": {
            "conditions": [],
            "container_statuses": [
                MockObject(
                    {
                        "container_id": None,
                        "image": "ubuntu_mock_test_image",
                        "image_id": "",
                        "last_state": {
                            "running": None,
                            "terminated": None,
                            "waiting": None,
                        },
                        "name": "task-1000-ex-00",
                        "ready": False,
                        "restart_count": 0,
                        "state": {
                            "running": None,
                            "terminated": None,
                            "waiting": {
                                "message": "Unknown error",
                                "reason": "Unknown",
                            },
                        },
                    }
                )
            ],
            "host_ip": "192.168.99.100",
            "init_container_statuses": None,
            "message": None,
            "nominated_node_name": None,
            "phase": "Pending",
            "pod_ip": "172.17.0.5",
            "qos_class": "BestEffort",
            "reason": None,
            "start_time": START_TIME - datetime.timedelta(minutes=diff_time),
        },
    }
    return MockObject({"items": [MockObject(return_value)]})


@pytest.fixture(autouse=True)
def taskmaster_args(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments of the taskmaster module.

    Args:
        monkeypatch: The fixture that restores the arguments afterwards.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(
            debug=False,
            file=None,
            filer_version="v0.1.9",
            json="json",
            namespace="default",
            poll_interval=5,
            state_file="/tmp/.teskstate",
            localKubeConfig=False,
            pull_policy_always=False,
            filer_name="eu.gcr.io/tes-wes/filer",
            pod_timeout=240,
        ),
    )


@pytest.fixture
def data() -> dict[str, Any]:
    """Get the task from the input file.

    Returns:
        The task as read from the JSON file.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        task: dict[str, Any] = json.load(fh)
    return task


def test_job() -> None:
    """Test that a Job object is created successfully."""
    job = Job({"metadata": {"name": "test"}})
    assert job.name == "task-job"
    assert job.namespace == "default"


@patch("kubernetes.client.BatchV1Api.create_namespaced_job")
@patch(
    "tesk_core.job.Job.get_status",
    side_effect=[
        ("Running", True),
        ("Running", True),
        ("Running", True),
        ("Running", True),
        ("Running", True),
        ("Complete", True),
    ],
)
def test_run_to_completion_success(
    mock_get_status: MagicMock,
    mock_create_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job runs to completion successfully."""
    for executor in data["executors"]:
        jobname = executor["metadata"]["name"]
        job = Job(executor, jobname, taskmaster.args.namespace)
        status = job.run_to_completion(
            1, taskmaster.check_cancelled, taskmaster.args.pod_timeout
        )
        assert status == "Complete"


@patch("tesk_core.job.Job.delete")
@patch("tesk_core.taskmaster.check_cancelled", return_value=True)
@patch("kubernetes.client.BatchV1Api.create_namespaced_job")
@patch("tesk_core.job.Job.get_status", side_effect=[("Running", True)])
def test_run_to_completion_cancelled(
    mock_get_status: MagicMock,
    mock_create_namespaced_job: MagicMock,
    mock_check_cancelled: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job is cancelled."""
    for executor in data["executors"]:
        jobname = executor["metadata"]["name"]
        job = Job(executor, jobname, taskmaster.args.namespace)
        status = job.run_to_completion(
            taskmaster.args.poll_interval,
            taskmaster.check_cancelled,
            taskmaster.args.pod_timeout,
        )
        assert status == "Cancelled"


@patch("tesk_core.taskmaster.check_cancelled", return_value=False)
@patch(
    "kubernetes.client.BatchV1Api.create_namespaced_job",
    side_effect=ApiException(status=409, reason="conflict"),
)
@patch("tesk_core.job.Job.get_status", side_effect=[("Complete", True)])
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_running,
)
def test_run_to_completion_check_conflict_exception(
    mock_get_status: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_check_cancelled: MagicMock,
    mock_create_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job status is complete after an ApiException of 409."""
    for executor in data["executors"]:
        jobname = executor["metadata"]["name"]
        job = Job(executor, jobname, taskmaster.args.namespace)
        status = job.run_to_completion(
            taskmaster.args.poll_interval,
            taskmaster.check_cancelled,
            taskmaster.args.pod_timeout,
        )
        assert status == "Complete"


@patch(
    "kubernetes.client.BatchV1Api.create_namespaced_job",
    side_effect=ApiException(status=500, reason="Random Exception"),
)
def test_run_to_completion_check_other_K8_exception(
    mock_create_namespaced_job: MagicMock, data: dict[str, Any]
) -> None:
    """Test that an ApiException with a status other than 409 is raised."""
    for executor in data["executors"]:
        jobname = executor["metadata"]["name"]
        job = Job(executor, jobname, taskmaster.args.namespace)
        with pytest.raises(ApiException):
            job.run_to_completion(
                taskmaster.args.poll_interval,
                taskmaster.check_cancelled,
                taskmaster.args.pod_timeout,
            )


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_success,
)
def test_get_status_success(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job status is complete."""
    executor = data["executors"][0]
    jobname = executor["metadata"]["name"]
    job = Job(executor, jobname, taskmaster.args.namespace)
    status, _ = job.get_status(False)
    assert status == "Complete"


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_running,
)
def test_get_status_running(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job is running in an ideal situation."""
    executor = data["executors"][0]
    jobname = executor["metadata"]["name"]
    job = Job(executor, jobname, taskmaster.args.namespace)
    status, _ = job.get_status(False)
    assert status == "Running"


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_job_status_for_failed_pod(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job is running if the pod failed to start for another reason.

    Another reason than ImagePullBackOff.
    """
    mock_list_namespaced_pod.return_value = list_namespaced_pod_pending_unknown_error()
    mock_read_namespaced_job.return_value = read_namespaced_job_pending()
    executor = data["executors"][0]
    jobname = executor["metadata"]["name"]
    job = Job(executor, jobname, taskmaster.args.namespace)
    status, _ = job.get_status(False)
    assert status == "Running"


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_job_status_for_wrong_image(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job is running during the default pod timeout.

    Assuming the image name is wrong, the status of the job returned from
    the method is "running" during the default pod timeout.
    """
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff(
        2
    )
    mock_read_namespaced_job.return_value = read_namespaced_job_pending(2)
    executor = data["executors"][0]
    jobname = executor["metadata"]["name"]
    job = Job(executor, jobname, taskmaster.args.namespace)
    status, _ = job.get_status(False)
    assert status == "Running"


def job_with_conditions(conditions: list[tuple[str, str]] | None) -> MockObject:
    """Mock reading a job with the given conditions.

    Args:
        conditions: The type and status of each condition, or `None` for a
            job without conditions.

    Returns:
        The job, started ten minutes ago, with an active pod.
    """
    return MockObject(
        {
            "status": {
                "active": 1,
                "conditions": (
                    None
                    if conditions is None
                    else [
                        MockObject({"type": type_, "status": status})
                        for type_, status in conditions
                    ]
                ),
                "failed": None,
                "start_time": START_TIME - datetime.timedelta(minutes=10),
                "succeeded": None,
            }
        }
    )


@pytest.mark.parametrize(
    ("conditions", "expected"),
    [
        (None, "Running"),
        ([], "Running"),
        ([("Complete", "True")], "Complete"),
        ([("SuccessCriteriaMet", "True")], "Complete"),
        ([("SuccessCriteriaMet", "True"), ("Complete", "True")], "Complete"),
        ([("Failed", "True")], "Failed"),
        ([("FailureTarget", "True")], "Failed"),
        ([("FailureTarget", "True"), ("Failed", "True")], "Failed"),
        ([("Suspended", "True")], "Running"),
        ([("Suspended", "False")], "Running"),
        ([("Complete", "False")], "Running"),
        ([("Failed", "False")], "Running"),
        ([("FailureTarget", "False")], "Running"),
        ([("Suspended", "False"), ("FailureTarget", "True")], "Failed"),
        ([("Suspended", "False"), ("Complete", "True")], "Complete"),
    ],
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_status_conditions(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    conditions: list[tuple[str, str]] | None,
    expected: str,
    data: dict[str, Any],
) -> None:
    """Test the status of a job for each combination of its conditions.

    Only conditions with the status `"True"` count; `FailureTarget`, which
    Kubernetes 1.31+ sets before `Failed`, counts as `Failed`, and other
    conditions leave the job running.
    """
    mock_read_namespaced_job.return_value = job_with_conditions(conditions)
    mock_list_namespaced_pod.return_value = MockObject({"items": []})
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    status, _ = job.get_status(False)
    assert status == expected


@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_timeout_fails_job(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pod that times out fails its job, once, and keeps it.

    The job gets an active deadline that has passed, so that Kubernetes
    marks it `Failed` (`DeadlineExceeded`); it runs until then.
    """
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff()
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    status, _ = job.get_status(False)
    assert status == "Running"
    status, _ = job.get_status(False)
    assert status == "Running"
    mock_patch_namespaced_job.assert_called_once_with(
        "task-ex-00",
        taskmaster.args.namespace,
        {"spec": {"activeDeadlineSeconds": 1}},
    )


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.create_namespaced_job")
def test_run_to_completion_pod_timeout(
    mock_create_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a job whose pod times out ends `Failed` and is kept."""
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff()
    mock_read_namespaced_job.side_effect = [
        job_with_conditions(None),
        job_with_conditions([("FailureTarget", "True")]),
    ]
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    status = job.run_to_completion(0, lambda: False, 50)
    assert status == "Failed"
    mock_patch_namespaced_job.assert_called_once()
    mock_job_delete.assert_not_called()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.create_namespaced_job")
def test_run_to_completion_pod_timeout_cancelled(
    mock_create_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a job that is failing still is deleted on cancellation."""
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff()
    mock_read_namespaced_job.return_value = job_with_conditions(None)
    check_cancelled = MagicMock(side_effect=[False, True])
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    status = job.run_to_completion(0, check_cancelled, 50)
    assert status == "Cancelled"
    mock_patch_namespaced_job.assert_called_once()
    assert job.failing_since is not None
    mock_job_delete.assert_called_once()


@pytest.mark.parametrize(
    ("patch_status", "delete_fails", "expected", "deleted"),
    [
        (403, False, "Failed", True),
        (403, True, "Failed", True),
        (404, False, "Failed", True),
        (500, False, "Running", False),
        (None, False, "Running", False),
    ],
)
@patch("kubernetes.client.BatchV1Api.delete_namespaced_job")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_timeout_patch_error(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_delete_namespaced_job: MagicMock,
    patch_status: int | None,
    delete_fails: bool,
    expected: str,
    deleted: bool,
    data: dict[str, Any],
) -> None:
    """Test a job whose change to fail it returns an error.

    A rejected change (client error) deletes the job, as before, and the job
    fails, also if it can't be deleted. After other errors, the job runs on,
    and the next poll tries again.
    """
    mock_patch_namespaced_job.side_effect = ApiException(status=patch_status)
    if delete_fails:
        mock_delete_namespaced_job.side_effect = ApiException(status=403)
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff()
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    status, all_running = job.get_status(False)
    assert status == expected
    assert job.failing_since is None
    assert mock_delete_namespaced_job.called == deleted
    if expected == "Running":
        job.get_status(all_running)
        assert mock_patch_namespaced_job.call_count == 2


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_status_failed_by_earlier_run(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a job failed by an earlier taskmaster run counts as failing.

    After a restart, the taskmaster waits for Kubernetes to mark it, for at
    most another timeout, without checking the pods again.
    """
    read = job_with_conditions(None)
    read.__dict__["spec"] = MockObject({"active_deadline_seconds": FAIL_DEADLINE})
    mock_read_namespaced_job.return_value = read
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    status, _ = job.get_status(False)
    assert status == "Running"
    assert job.failing_since is not None
    mock_list_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    ("waited", "conditions", "expected", "deleted"),
    [
        (10, None, "Running", False),
        (60, None, "Failed", False),
        (10, [("Failed", "True")], "Failed", False),
        (10, [("Complete", "True")], "Complete", False),
    ],
)
@patch("kubernetes.client.BatchV1Api.delete_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_status_failing(
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    mock_delete_namespaced_job: MagicMock,
    waited: int,
    conditions: list[tuple[str, str]] | None,
    expected: str,
    deleted: bool,
    data: dict[str, Any],
) -> None:
    """Test the status of a job that was failed.

    It runs until Kubernetes ends it, for at most another timeout; then it
    counts as failed, and is kept. The pods aren't checked again.
    """
    mock_read_namespaced_job.return_value = job_with_conditions(conditions)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    job.failing_since = datetime.datetime.now(timezone.utc) - datetime.timedelta(
        seconds=waited
    )
    status, _ = job.get_status(False)
    assert status == expected
    assert mock_delete_namespaced_job.called == deleted
    mock_list_namespaced_pod.assert_not_called()


@patch("kubernetes.client.BatchV1Api.delete_namespaced_job")
def test_delete(mock_delete_namespaced_job: MagicMock, data: dict[str, Any]) -> None:
    """Test that deleting a job deletes its pods in the background."""
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.delete()
    mock_delete_namespaced_job.assert_called_once()
    args, kwargs = mock_delete_namespaced_job.call_args
    assert args == ("task-ex-00", taskmaster.args.namespace)
    assert kwargs["body"].propagation_policy == "Background"


def pending_pod(
    reason: str | None,
    minutes: int = 10,
    started: bool = True,
    init: bool = False,
    phase: str = "Pending",
    created: bool = True,
) -> MockObject:
    """Mock listing a pending pod whose container waits.

    Args:
        reason: Why the container waits; `None` for a pod without container
            statuses (e.g., one that isn't scheduled yet).
        minutes: The minutes since the pod was created (and started).
        started: Whether the pod has a start time.
        init: Whether the waiting container is an init container.
        phase: The phase of the pod.
        created: Whether the pod has a creation time.

    Returns:
        The list with the pending pod.
    """
    since = START_TIME - datetime.timedelta(minutes=minutes)
    statuses = (
        None
        if reason is None
        else [MockObject({"state": {"waiting": {"reason": reason, "message": "mock"}}})]
    )
    status = {
        "phase": phase,
        "start_time": since if started else None,
        "container_statuses": None if init else statuses,
        "init_container_statuses": statuses if init else None,
    }
    pod = MockObject(
        {
            "metadata": {
                "name": "pod",
                "creation_timestamp": since if created else None,
            },
            "status": status,
        }
    )
    return MockObject({"items": [pod]})


@pytest.mark.parametrize(
    "reason",
    [
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "CreateContainerConfigError",
    ],
)
@pytest.mark.parametrize("started", [True, False])
@pytest.mark.parametrize("init", [False, True])
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
    reason: str,
    started: bool,
    init: bool,
) -> None:
    """Test that pods that can't start fail their job after the timeout.

    Whether or not they have a start time, and for init containers too.
    """
    mock_list_namespaced_pod.return_value = pending_pod(reason, 10, started, init)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    assert job.get_status(False) == ("Running", False)
    mock_patch_namespaced_job.assert_called_once_with(
        "task-ex-00",
        taskmaster.args.namespace,
        {"spec": {"activeDeadlineSeconds": 1}},
    )


@pytest.mark.parametrize(
    ("reason", "minutes"),
    [
        (None, 10),  # Not scheduled yet: queued
        ("ContainerCreating", 10),  # E.g., pulling a large image
        ("ErrImagePull", 0),  # Within the timeout
    ],
)
@pytest.mark.parametrize("started", [True, False])
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_pending(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
    reason: str | None,
    minutes: int,
    started: bool,
) -> None:
    """Test that pods that may still start keep the job running."""
    mock_list_namespaced_pod.return_value = pending_pod(reason, minutes, started)
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    assert job.get_status(False) == ("Running", False)
    mock_patch_namespaced_job.assert_not_called()


def test_cannot_start_without_state() -> None:
    """Test that containers without a state are not taken as unable to start."""
    pod = MockObject(
        {
            "status": {
                "init_container_statuses": None,
                "container_statuses": [MockObject({"state": None})],
            }
        }
    )
    assert cannot_start(pod) is None


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pods_running(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the pods are no longer checked once none is pending."""
    mock_list_namespaced_pod.return_value = pending_pod("ErrImagePull", phase="Running")
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    assert job.get_status(False) == ("Running", True)


@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_without_times(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pending pod without start and creation time keeps waiting."""
    mock_list_namespaced_pod.return_value = pending_pod(
        "ErrImagePull", started=False, created=False
    )
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    assert job.get_status(False) == ("Running", False)
    mock_patch_namespaced_job.assert_not_called()


@pytest.mark.parametrize("reason", ["ErrImagePull", "ImagePullBackOff"])
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_unscheduled_then_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
    reason: str,
) -> None:
    """Test that a pod is checked again after it was waiting to be scheduled.

    Not at the next poll, but after `pod_check_interval`.
    """
    mock_list_namespaced_pod.side_effect = [
        pending_pod(None, started=False),
        pending_pod(reason),
    ]
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    status, all_running = job.get_status(False)
    assert (status, all_running) == ("Running", False)
    assert job.get_status(all_running) == ("Running", False)
    assert mock_list_namespaced_pod.call_count == 1
    mock_patch_namespaced_job.assert_not_called()
    job.next_pod_check = datetime.datetime.now(timezone.utc)
    job.get_status(all_running)
    assert mock_list_namespaced_pod.call_count == 2
    mock_patch_namespaced_job.assert_called_once()


@pytest.mark.parametrize(
    ("init_reason", "reason", "expected"),
    [
        ("PodInitializing", "ErrImagePull", "ErrImagePull"),
        ("ImagePullBackOff", "PodInitializing", "ImagePullBackOff"),
        ("PodInitializing", "ContainerCreating", None),
    ],
)
def test_cannot_start_init_and_main_containers(
    init_reason: str, reason: str, expected: str | None
) -> None:
    """Test that init and main containers are both checked, init ones first."""

    def status(waiting_reason: str) -> MockObject:
        return MockObject({"state": {"waiting": {"reason": waiting_reason}}})

    pod = MockObject(
        {
            "status": {
                "init_container_statuses": [status("Completed"), status(init_reason)],
                "container_statuses": [status(reason)],
            }
        }
    )
    waiting = cannot_start(pod)
    assert (waiting.reason if waiting is not None else None) == expected


@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_replacement_pod_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that pods are checked again once a pod of the job failed.

    A retry or a replacement of an evicted pod can't start either.
    """
    running = job_with_conditions(None)
    retried = job_with_conditions(None)
    retried.__dict__["status"].__dict__["failed"] = 1
    mock_read_namespaced_job.side_effect = [running, running, retried]
    mock_list_namespaced_pod.side_effect = [
        pending_pod("ErrImagePull", phase="Running"),
        pending_pod("ErrImagePull"),
    ]
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    status, all_running = job.get_status(False)
    assert (status, all_running) == ("Running", True)
    assert job.get_status(all_running) == ("Running", True)
    assert mock_list_namespaced_pod.call_count == 1
    job.get_status(all_running)
    assert mock_list_namespaced_pod.call_count == 2
    mock_patch_namespaced_job.assert_called_once()


# The scheduler's message for a pod that no node has room for
NO_ROOM = "0/3 nodes are available: 3 Insufficient cpu."


def unschedulable_pod(
    minutes: int = 10,
    reason: str = "Unschedulable",
    status: str = "False",
    conditions: bool = True,
) -> MockObject:
    """Mock listing a pending pod that the scheduler can't place.

    Args:
        minutes: The minutes since the pod was created.
        reason: The reason of the pod's condition `PodScheduled`.
        status: The status of that condition.
        conditions: Whether the pod has conditions.

    Returns:
        The list with the pending pod.
    """
    condition = MockObject(
        {
            "type": "PodScheduled",
            "status": status,
            "reason": reason,
            "message": NO_ROOM,
        }
    )
    pod = MockObject(
        {
            "metadata": {
                "name": "pod",
                "creation_timestamp": START_TIME - datetime.timedelta(minutes=minutes),
            },
            "status": {
                "phase": "Pending",
                "start_time": None,
                "container_statuses": None,
                "init_container_statuses": None,
                "conditions": [condition] if conditions else None,
            },
        }
    )
    return MockObject({"items": [pod]})


@pytest.mark.parametrize(
    ("reason", "status", "conditions", "expected"),
    [
        ("Unschedulable", "False", True, NO_ROOM),
        ("Unschedulable", "True", True, None),
        ("SchedulingGated", "False", True, None),
        ("Unschedulable", "False", False, None),
    ],
)
def test_unschedulable(
    reason: str, status: str, conditions: bool, expected: str | None
) -> None:
    """Test that only the condition `PodScheduled` with reason `Unschedulable` counts.

    Args:
        reason: The reason of the condition.
        status: The status of the condition.
        conditions: Whether the pod has conditions.
        expected: The scheduler's message, or `None`.
    """
    pods: Any = unschedulable_pod(reason=reason, status=status, conditions=conditions)
    assert unschedulable(pods.items[0]) == expected


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_fails_job(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a pod unschedulable for longer than `max_queue` fails its job.

    Also before `timeout`; the job is kept, as for pods that can't start.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
        caplog: The fixture that captures the log.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 3600
    job.max_queue = 60

    assert job.get_status(False) == ("Running", False)

    mock_patch_namespaced_job.assert_called_once_with(
        "task-ex-00",
        taskmaster.args.namespace,
        {"spec": {"activeDeadlineSeconds": 1}},
    )
    mock_job_delete.assert_not_called()
    assert "Pod 'pod' of job 'task-ex-00' wasn't scheduled within 60 s" in caplog.text
    assert "Insufficient cpu" in caplog.text


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_deletes_job(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that with `delete_unschedulable`, the job is deleted and fails.

    For executors: a deleted executor job gives `SYSTEM_ERROR`, not
    `EXECUTOR_ERROR`, in the TESK API.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Failed", False)

    mock_job_delete.assert_called_once_with(request_timeout=DELETE_TIMEOUT)
    mock_patch_namespaced_job.assert_not_called()


@pytest.mark.parametrize(
    ("max_queue", "minutes", "reason"),
    [
        (None, 10, "Unschedulable"),  # No max queue time
        (3600, 10, "Unschedulable"),  # Within it
        (60, 10, "SchedulingGated"),  # Not unschedulable
    ],
)
@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_waits(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
    max_queue: int | None,
    minutes: int,
    reason: str,
) -> None:
    """Test that pods keep waiting without a max queue time, or within it.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
        max_queue: The max queue time.
        minutes: The minutes since the pod was created.
        reason: The reason of the pod's condition `PodScheduled`.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes, reason)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    job.max_queue = max_queue
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Running", False)

    mock_patch_namespaced_job.assert_not_called()
    mock_job_delete.assert_not_called()


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_checks_pods_early(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the pods are checked after `max_queue`, before `timeout`.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = MockObject({"items": []})
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 3600
    job.get_status(False)
    mock_list_namespaced_pod.assert_not_called()

    job.max_queue = 60
    job.get_status(False)
    mock_list_namespaced_pod.assert_called_once()


@pytest.mark.parametrize(
    ("status", "expected", "failed"),
    [
        (404, "Failed", False),  # Already gone
        (403, "Running", True),  # Rejected: failed and kept instead
        (503, "Running", False),  # Tried again at the next poll
    ],
)
@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_delete_error(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
    status: int,
    expected: str,
    failed: bool,
) -> None:
    """Test that a job that can't be deleted is failed, or deleted later.

    So that an executor job that is left can't run after the task stopped.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
        status: The status of the API error of the deletion.
        expected: The status of the job.
        failed: Whether the job is failed instead.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    mock_job_delete.side_effect = ApiException(status=status)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == (expected, False)

    assert mock_patch_namespaced_job.called is failed
    if expected == "Running" and not failed:
        # Not waiting for the next pod check: the next poll tries again
        job.get_status(False)
        assert mock_job_delete.call_count == 2


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_retry_kept(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a job whose earlier pod failed is failed and kept, not deleted.

    Its executor ran, so its logs are kept, and the TESK API reports the
    executor's failure.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
        caplog: The fixture that captures the log.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True
    job.failed_pods = 1

    with caplog.at_level(logging.INFO):
        assert job.get_status(False) == ("Running", False)

    mock_job_delete.assert_not_called()
    mock_patch_namespaced_job.assert_called_once()
    assert "its pod wasn't scheduled in time" in caplog.text


@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pod that can't start still fails its job with a max queue time.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        data: The task.
    """
    pods: Any = pending_pod("ErrImagePull", 10)
    pods.items[0].status.conditions = None
    mock_list_namespaced_pod.return_value = pods
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 50
    job.max_queue = 3600

    assert job.get_status(False) == ("Running", False)

    mock_patch_namespaced_job.assert_called_once()


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_pod_check_interval(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that pods within the max queue time are checked once a minute.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=1)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 3600
    job.max_queue = 300

    job.get_status(False)
    job.get_status(False)

    mock_list_namespaced_pod.assert_called_once()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_deleted_despite_error(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a deletion that failed is tried again first, at the next poll.

    Whatever the job does meanwhile; a job that is gone then counts as
    deleted, as a server error can come after the deletion happened.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    mock_read_namespaced_job.side_effect = read_namespaced_job_error
    mock_job_delete.side_effect = [ApiException(status=504), ApiException(status=404)]
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Running", False)
    assert job.get_status(False) == ("Failed", False)
    # The job isn't read again before the deletion is tried again
    assert mock_read_namespaced_job.call_count == 1


@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_get_status_missing_job(
    mock_read_namespaced_job: MagicMock, data: dict[str, Any]
) -> None:
    """Test that a job that is gone, without a deletion, is still an error.

    Args:
        mock_read_namespaced_job: The mocked reading of the job.
        data: The task.
    """
    mock_read_namespaced_job.side_effect = ApiException(status=404)
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)

    with pytest.raises(ApiException):
        job.get_status(False)


@patch("tesk_core.job.Job.delete")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_delete_transport_error(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a transport error deleting the job is tried again.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10)
    mock_job_delete.side_effect = [ProtocolError("Connection aborted"), None]
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Running", False)
    assert job.get_status(False) == ("Failed", False)
    assert mock_job_delete.call_count == 2


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_pod_ran_kept(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a job with a pod that ran is failed and kept, not deleted.

    Also before its failed pods are counted.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
    """
    pods: Any = unschedulable_pod(minutes=10)
    pods.items.insert(
        0,
        pending_pod(None, phase="Failed").items[0],  # type: ignore[attr-defined]
    )
    mock_list_namespaced_pod.return_value = pods
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Running", False)

    mock_job_delete.assert_not_called()
    mock_patch_namespaced_job.assert_called_once()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_max_queue_scheduled(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_patch_namespaced_job: MagicMock,
    mock_job_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pod scheduled before the max queue time keeps its job.

    Args:
        mock_list_namespaced_pod: The mocked listing of the job's pods.
        mock_read_namespaced_job: The mocked reading of the job.
        mock_patch_namespaced_job: The mocked patch of the job.
        mock_job_delete: The mocked deletion of the job.
        data: The task.
    """
    mock_list_namespaced_pod.return_value = unschedulable_pod(minutes=10, status="True")
    job = Job(data["executors"][0], "task-ex-00", taskmaster.args.namespace)
    job.timeout = 3600
    job.max_queue = 60
    job.delete_unschedulable = True

    assert job.get_status(False) == ("Running", False)

    mock_job_delete.assert_not_called()
    mock_patch_namespaced_job.assert_not_called()
