"""Tests for jobs whose pods Kubernetes can't create (issue #169)."""

import datetime
from datetime import timezone
import logging
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import ProtocolError

from tesk_core.job import (
    DELETE_TIMEOUT,
    EVENTS_TIMEOUT,
    MAX_MESSAGE_LENGTH,
    NOT_CREATED_REASON,
    Job,
    permanent_create_error,
)
from tests.unit.test_job import MockObject

NOW = datetime.datetime.now(timezone.utc)
# The event seen at the FI TESK on CSC Rahti: a `LimitRange` with a default
# memory limit of 1Gi, and an executor that requests 2 GiB without a limit
INVALID = (
    'Error creating: Pod "task-ex-00-abcde" is invalid:'
    ' spec.containers[0].resources.requests: Invalid value: "2147483648":'
    " must be less than or equal to memory limit of 1Gi"
)
OVER_MAX = (
    'Error creating: pods "task-ex-00-abcde" is forbidden: maximum memory'
    " usage per Container is 1Gi, but limit is 2Gi"
)
QUOTA = (
    'Error creating: pods "task-ex-00-abcde" is forbidden: exceeded quota:'
    " q, requested: limits.cpu=2, used: limits.cpu=7, limited: limits.cpu=8"
)


def job_without_pods(
    suspend: bool | None = False,
    uid: str | None = "uid-1",
    **status: Any,
) -> MockObject:
    """Mock a running job without pods.

    Args:
        suspend: The job's `spec.suspend`.
        uid: The job's UID.
        **status: Fields of the job's status that replace the defaults.

    Returns:
        The job.
    """
    fields: dict[str, Any] = {
        "active": None,
        "ready": None,
        "terminating": None,
        "succeeded": None,
        "failed": None,
        "conditions": None,
        "start_time": NOW,
    }
    fields.update(status)
    return MockObject(
        {
            "metadata": {"uid": uid},
            "spec": {"suspend": suspend, "active_deadline_seconds": None},
            "status": fields,
        }
    )


def event(
    message: str,
    minutes: int = 0,
    timestamps: bool = True,
) -> MockObject:
    """Mock a `FailedCreate` event of a job.

    Args:
        message: The event's message.
        minutes: Minutes before now when the event was last seen.
        timestamps: Whether the event has timestamps; without, it counts as
            the oldest.

    Returns:
        The event.
    """
    seen = NOW - datetime.timedelta(minutes=minutes) if timestamps else None
    return MockObject(
        {
            "message": message,
            "last_timestamp": seen,
            "event_time": None,
            "metadata": {"creation_timestamp": None},
        }
    )


def events(*items: MockObject) -> MockObject:
    """Mock a list of events.

    Args:
        *items: The events.

    Returns:
        The list.
    """
    return MockObject({"items": list(items)})


def new_job(delete_unschedulable: bool = True, max_queue: int | None = None) -> Job:
    """Describe an executor job, as the taskmaster does.

    Args:
        delete_unschedulable: Whether the job is deleted (executors) rather
            than failed (filers) when it ends.
        max_queue: The max queue time.

    Returns:
        The job.
    """
    job = Job({"metadata": {}}, "task-ex-00", "ns")
    job.delete_unschedulable = delete_unschedulable
    job.max_queue = max_queue
    return job


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (INVALID, True),
        (OVER_MAX, True),
        (
            'Error creating: pods "p" is forbidden: failed quota: q: must'
            " specify limits.cpu for: executor",
            True,
        ),
        (
            'Error creating: pods "p" is forbidden: violates PodSecurity'
            ' "restricted:latest": runAsNonRoot != true',
            True,
        ),
        (QUOTA, False),
        (
            'Error creating: pods "p" is forbidden: error looking up service'
            ' account ns/sa: serviceaccount "sa" not found',
            False,
        ),
        (
            'Error creating: Internal error occurred: failed calling webhook "w"',
            False,
        ),
        (
            'Error creating: pods "p" is forbidden: unable to create new content'
            " in namespace ns because it is being terminated",
            False,
        ),
        ("Error creating: the server was unable to return a response", False),
    ],
)
def test_permanent_create_error(message: str, expected: bool) -> None:
    """Test that only refusals that waiting can't resolve are permanent.

    Args:
        message: The message of the `FailedCreate` event.
        expected: Whether it is permanent.
    """
    assert permanent_create_error(message) is expected


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
@pytest.mark.parametrize("message", [INVALID, OVER_MAX])
def test_executor_not_created_is_deleted(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_patch: MagicMock,
    mock_delete: MagicMock,
    message: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an executor whose pod is refused ends at once (regression).

    Its job is deleted, from which the TESK API derives `SYSTEM_ERROR` (a
    missing executor), and the taskmaster logs Kubernetes' message, which
    the TESK API shows in the task's system logs. Before, the job waited
    for a pod forever, and the task stayed `RUNNING`.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_patch: The mocked patch of the job.
        mock_delete: The mocked deletion of the job.
        message: The message of the `FailedCreate` event.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(QUOTA, minutes=5), event(message))
    job = new_job()

    assert job.get_status(False) == ("Failed", False)

    mock_events.assert_called_once_with(
        "ns",
        field_selector="involvedObject.kind=Job,involvedObject.name=task-ex-00"
        ",reason=FailedCreate,involvedObject.uid=uid-1",
        _request_timeout=EVENTS_TIMEOUT,
    )
    mock_delete.assert_called_once_with(request_timeout=DELETE_TIMEOUT)
    mock_patch.assert_not_called()
    assert job.ended_by_taskmaster
    assert f"Pods of job 'task-ex-00' can't be created: {message}" in caplog.text


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_filer_not_created_is_failed(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_patch: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a filer (or a job whose pod ran before) is failed and kept.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_patch: The mocked patch of the job.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    caplog.set_level(logging.INFO)
    mock_read.return_value = job_without_pods(uid=None)
    mock_events.return_value = events(event(INVALID))
    job = new_job(delete_unschedulable=False)

    assert job.get_status(False) == ("Running", False)

    assert mock_events.call_args.kwargs["field_selector"].endswith(
        "reason=FailedCreate"
    )
    mock_patch.assert_called_once_with(
        "task-ex-00", "ns", {"spec": {"activeDeadlineSeconds": 1}}
    )
    mock_delete.assert_not_called()
    assert f"Failing job 'task-ex-00': {NOT_CREATED_REASON}" in caplog.text


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_temporary_refusal_waits_for_max_queue(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a quota that is used up keeps the job waiting, up to `max_queue`.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(QUOTA))
    job = new_job(max_queue=60)

    assert job.get_status(False) == ("Running", False)
    mock_delete.assert_not_called()

    # Past the max queue time, and the next check of the events
    assert job.no_pods_since is not None
    job.no_pods_since -= datetime.timedelta(seconds=61)
    job.next_create_check = None

    assert job.get_status(False) == ("Failed", False)
    mock_delete.assert_called_once_with(request_timeout=DELETE_TIMEOUT)
    assert f"No pod of job 'task-ex-00' was created within 60 s: {QUOTA}" in caplog.text


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_without_max_queue_waits(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that without a max queue time, a temporary refusal waits forever.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events()
    job = new_job()
    job.no_pods_since = NOW - datetime.timedelta(days=30)

    assert job.get_status(False) == ("Running", False)

    mock_delete.assert_not_called()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_events_forbidden(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that without the right to read events, only `max_queue` ends the job.

    The taskmaster warns once, doesn't ask again, and logs where to look.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.side_effect = ApiException(status=403, reason="Forbidden")
    job = new_job(max_queue=60)

    assert job.get_status(False) == ("Running", False)
    assert "Can't read the events of job 'task-ex-00' (403 Forbidden)" in caplog.text

    assert job.no_pods_since is not None
    job.no_pods_since -= datetime.timedelta(seconds=61)
    job.next_create_check = None

    assert job.get_status(False) == ("Failed", False)
    mock_events.assert_called_once()
    mock_delete.assert_called_once_with(request_timeout=DELETE_TIMEOUT)
    assert (
        "No pod of job 'task-ex-00' was created within 60 s:"
        " see the job's events (kubectl describe job)" in caplog.text
    )


@pytest.mark.parametrize(
    "error", [ApiException(status=500, reason="Error"), ProtocolError("reset")]
)
@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_events_error_tries_again(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    error: Exception,
) -> None:
    """Test that after other errors, the events are read again at the next check.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        error: The error of the listing.
    """
    mock_read.return_value = job_without_pods()
    mock_events.side_effect = [error, events(event(INVALID))]
    job = new_job()

    assert job.get_status(False) == ("Running", False)
    mock_delete.assert_not_called()

    job.next_create_check = None
    assert job.get_status(False) == ("Failed", False)
    assert mock_events.call_count == 2
    mock_delete.assert_called_once()


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_events_checked_at_intervals(
    mock_read: MagicMock, mock_events: MagicMock
) -> None:
    """Test that the events are read at most every `pod_check_interval` seconds.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(QUOTA))
    job = new_job()

    for _ in range(3):
        assert job.get_status(False) == ("Running", False)

    mock_events.assert_called_once()


@pytest.mark.parametrize(
    ("suspend", "status"),
    [
        (True, {}),  # Suspended, e.g. by a queue such as Kueue
        (False, {"active": 1}),  # A pod exists, e.g. pending
        (None, {"ready": 1}),
        (False, {"terminating": 1}),
        (False, {"succeeded": 1}),  # Completes soon
    ],
)
@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_jobs_with_pods_or_suspended_not_checked(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_pods: MagicMock,
    suspend: bool | None,
    status: dict[str, Any],
) -> None:
    """Test that only running jobs without pods are checked.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_pods: The mocked listing of the job's pods (a job with an active
            pod has them checked as before).
        suspend: The job's `spec.suspend`.
        status: Fields of the job's status.
    """
    mock_pods.return_value = MockObject({"items": []})
    mock_read.return_value = job_without_pods(suspend=suspend, **status)
    job = new_job(max_queue=1)
    job.no_pods_since = NOW - datetime.timedelta(hours=1)

    assert job.get_status(False)[0] == "Running"

    mock_events.assert_not_called()
    assert job.no_pods_since is None


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_retry_pod_not_created_keeps_job(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that a job whose failed pod's retry is refused is failed and kept.

    Its failed pod ran, so the job keeps its logs (`end_unschedulable`).

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
    """
    mock_read.return_value = job_without_pods(failed=1)
    mock_events.return_value = events(event(OVER_MAX))
    job = new_job()

    with patch("kubernetes.client.BatchV1Api.patch_namespaced_job") as mock_patch:
        assert job.get_status(False) == ("Running", False)

    mock_patch.assert_called_once()
    mock_delete.assert_not_called()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_latest_event_counts(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that the latest event decides, and events without times count as old.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(
        event(INVALID, minutes=10), event(QUOTA), event(OVER_MAX, timestamps=False)
    )
    job = new_job()

    assert job.get_status(False) == ("Running", False)

    mock_delete.assert_not_called()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_long_message_shortened(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a long message is shortened in the log.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(INVALID + "x" * 5000))
    job = new_job()

    assert job.get_status(False) == ("Failed", False)

    assert INVALID in caplog.text
    assert "x" * (MAX_MESSAGE_LENGTH + 1) not in caplog.text
    assert "x…" in caplog.text


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_rejected_deletion_fails_with_reason(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_patch: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a deletion that is tried again keeps the reason.

    A server error makes the next poll try the deletion again; a rejection
    then fails the job, with the reason why it ended.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_patch: The mocked patch of the job.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    caplog.set_level(logging.INFO)
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(INVALID))
    mock_delete.side_effect = [ApiException(status=503), ApiException(status=403)]
    job = new_job()

    assert job.get_status(False) == ("Running", False)
    assert job.deleting
    assert job.get_status(False) == ("Running", False)

    mock_patch.assert_called_once()
    assert f"Failing job 'task-ex-00': {NOT_CREATED_REASON}" in caplog.text
