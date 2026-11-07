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
from tesk_core.job import Job, cannot_start

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
    side_effect=read_namespaced_job_error,
)
@patch("tesk_core.job.Job.delete")
@patch("tesk_core.taskmaster.check_cancelled", return_value=False)
@patch("kubernetes.client.BatchV1Api.create_namespaced_job")
def test_run_to_completion_error(
    mock_create_namespaced_job: MagicMock,
    mock_check_cancelled: MagicMock,
    mock_job_delete: MagicMock,
    mock_read_namespaced_job: MagicMock,
    mock_list_namespaced_pod: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the job state is 'error' for a pending pod with ImagePullBackOff."""
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff(
        10
    )
    for executor in data["executors"]:
        jobname = executor["metadata"]["name"]
        job = Job(executor, jobname, taskmaster.args.namespace)
        status = job.run_to_completion(1, taskmaster.check_cancelled, 120)
        assert status == "Error"


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_job_status_ImagaPullBackoff_error(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that the state is 'error' for a pod that failed with ImagePullBackOff."""
    mock_list_namespaced_pod.return_value = list_namespaced_pod_error_ImagePullBackOff()
    executor = data["executors"][0]
    jobname = executor["metadata"]["name"]
    job = Job(executor, jobname, taskmaster.args.namespace)
    job.timeout = 50
    status, _ = job.get_status(False)
    assert status == "Error"


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
            "metadata": {"creation_timestamp": since if created else None},
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
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
    reason: str,
    started: bool,
    init: bool,
) -> None:
    """Test that pods that can't start fail the job after the timeout.

    Whether or not they have a start time, and for init containers too. A
    started pod whose first container can't pull its image
    (`ImagePullBackOff`) gives `Error`, as before; the other cases give
    `PodTimeout`.
    """
    mock_list_namespaced_pod.return_value = pending_pod(reason, 10, started, init)
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    status, _ = job.get_status(False)
    if reason == "ImagePullBackOff" and started and not init:
        assert status == "Error"
    else:
        assert status == "PodTimeout"


@pytest.mark.parametrize(
    ("reason", "minutes"),
    [
        (None, 10),  # Not scheduled yet: queued
        ("ContainerCreating", 10),  # E.g., pulling a large image
        ("ErrImagePull", 0),  # Within the timeout
    ],
)
@pytest.mark.parametrize("started", [True, False])
@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_pending(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
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


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_pod_without_times(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
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


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_unscheduled_then_cannot_start(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pod is checked again after it was waiting to be scheduled."""
    mock_list_namespaced_pod.side_effect = [
        pending_pod(None, started=False),
        pending_pod("ErrImagePull"),
    ]
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    status, all_running = job.get_status(False)
    assert (status, all_running) == ("Running", False)
    assert job.get_status(all_running)[0] == "PodTimeout"


@patch(
    "kubernetes.client.BatchV1Api.read_namespaced_job",
    side_effect=read_namespaced_job_error,
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
def test_get_status_unscheduled_then_image_pull_back_off(
    mock_list_namespaced_pod: MagicMock,
    mock_read_namespaced_job: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a pod found unscheduled first gives `PodTimeout`, not `Error`.

    The earlier check stopped at the unscheduled pod and never found the
    `ImagePullBackOff`, so the job is kept.
    """
    mock_list_namespaced_pod.side_effect = [
        pending_pod(None, started=False),
        pending_pod("ImagePullBackOff"),
    ]
    executor = data["executors"][0]
    job = Job(executor, executor["metadata"]["name"], taskmaster.args.namespace)
    job.timeout = 50
    _, all_running = job.get_status(False)
    assert job.get_status(all_running)[0] == "PodTimeout"
