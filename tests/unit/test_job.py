"""Tests for running jobs to completion and for the status of jobs."""

from argparse import Namespace
import datetime
from datetime import timezone
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest

from tesk_core import taskmaster
from tesk_core.job import FAIL_DEADLINE, Job

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
        }
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
        }
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
    status, _ = job.get_status(False)
    assert status == expected
    assert job.failing_since is None
    assert mock_delete_namespaced_job.called == deleted


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
