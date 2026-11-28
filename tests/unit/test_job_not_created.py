"""Tests for jobs whose pods Kubernetes can't create (issue #169)."""

import datetime
from datetime import timezone
import logging
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import ProtocolError

from tesk_core import job as job_module
from tesk_core.job import (
    CREATE_CHECK_DELAY,
    DELETE_TIMEOUT,
    EVENTS_TIMEOUT,
    MAX_CREATE_CHECK_INTERVAL,
    MAX_MESSAGE_LENGTH,
    MAX_RETRY_BACKOFF,
    NOT_CREATED_REASON,
    POD_CHECK_INTERVAL,
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


@pytest.fixture(autouse=True)
def events_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test as if the taskmaster may list events.

    Args:
        monkeypatch: The fixture that resets the module's flag.
    """
    monkeypatch.setattr(job_module, "events_forbidden", False)


def job_without_pods(
    suspend: bool | None = False,
    uid: str | None = "uid-1",
    started: datetime.datetime | None = None,
    created: datetime.datetime | None = None,
    **status: Any,
) -> MockObject:
    """Mock a running job without pods.

    Args:
        suspend: The job's `spec.suspend`.
        uid: The job's UID.
        started: When the job started or resumed (`status.startTime`).
        created: When the job was created.
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
        "start_time": started,
    }
    fields.update(status)
    return MockObject(
        {
            "metadata": {"uid": uid, "creation_timestamp": created},
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
    now = datetime.datetime.now(timezone.utc)
    seen = now - datetime.timedelta(minutes=minutes) if timestamps else None
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
    # Without pods for long enough that the events are read at once
    job.no_pods_since = datetime.datetime.now(timezone.utc) - datetime.timedelta(
        seconds=CREATE_CHECK_DELAY
    )
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
        (  # The pod alone requests more than the quota's hard limit
            'Error creating: pods "p" is forbidden: exceeded quota: q,'
            " requested: limits.memory=2Gi,requests.memory=2Gi, used:"
            " limits.memory=0,requests.memory=0, limited:"
            " limits.memory=1Gi,requests.memory=1Gi",
            True,
        ),
        (
            'Error creating: pods "p" is forbidden: exceeded quota: q,'
            " requested: limits.memory=1Gi, used: limits.memory=1Gi, limited:"
            " limits.memory=1Gi",
            False,
        ),
        (
            'Error creating: pods "p" is forbidden: exceeded quota: q,'
            " requested: limits.memory=2Xi, used: limits.memory=0, limited:"
            " limits.memory=1Gi",
            False,
        ),
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
        (
            'Error creating: pods "p" is forbidden: status unknown for quota: q,'
            " resources: limits.cpu",
            False,
        ),
        (
            'Error creating: admission webhook "w" denied the request: no',
            False,
        ),
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
    mock_events.return_value = events(event(QUOTA))
    job = new_job()
    job.no_pods_since = NOW - datetime.timedelta(days=30)

    with patch("kubernetes.client.BatchV1Api.patch_namespaced_job") as mock_patch:
        assert job.get_status(False) == ("Running", False)
        mock_patch.assert_not_called()

    mock_events.assert_called_once()
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


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_first_check_delayed(mock_read: MagicMock, mock_events: MagicMock) -> None:
    """Test that a job just seen without pods doesn't read its events yet.

    Its first pod may be on its way, so the events are read only after
    `CREATE_CHECK_DELAY` seconds without pods.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
    """
    mock_read.return_value = job_without_pods()
    job = new_job()
    job.no_pods_since = None

    assert job.get_status(False) == ("Running", False)

    mock_events.assert_not_called()
    assert job.next_create_check is not None
    assert job.no_pods_since is not None
    delay = (job.next_create_check - job.no_pods_since).total_seconds()
    assert delay == CREATE_CHECK_DELAY


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_check_interval_doubles(mock_read: MagicMock, mock_events: MagicMock) -> None:
    """Test that the interval between reads doubles, up to its maximum.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(QUOTA))
    job = new_job()
    intervals = []

    for _ in range(6):
        interval = job.create_check_interval
        # Due again, without waiting for the interval
        job.next_create_check = None
        before = datetime.datetime.now(timezone.utc)
        assert job.get_status(False) == ("Running", False)
        after = datetime.datetime.now(timezone.utc)
        intervals.append(job.create_check_interval)
        # The next read is due after the interval before the doubling
        assert job.next_create_check is not None
        assert before + datetime.timedelta(seconds=interval) <= job.next_create_check
        assert job.next_create_check <= after + datetime.timedelta(seconds=interval)
        # And not before
        assert job.get_status(False) == ("Running", False)

    assert intervals == [
        min(POD_CHECK_INTERVAL * 2**n, MAX_CREATE_CHECK_INTERVAL) for n in range(1, 7)
    ]
    assert intervals[-1] == MAX_CREATE_CHECK_INTERVAL
    assert mock_events.call_count == 6


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.BatchV1Api.patch_namespaced_job")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_retry_wait_not_ended_by_max_queue(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_patch: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that a job waiting to retry a failed pod outlasts `max_queue`.

    Kubernetes waits up to 6 minutes before it retries a failed pod, without
    pods meanwhile; only a permanent refusal ends such a job.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_patch: The mocked patch of the job.
        mock_delete: The mocked deletion of the job.
    """
    mock_read.return_value = job_without_pods(failed=4)
    mock_events.return_value = events(event(QUOTA))
    job = new_job(delete_unschedulable=False, max_queue=120)
    assert job.no_pods_since is not None
    job.no_pods_since -= datetime.timedelta(seconds=300)

    assert job.get_status(False) == ("Running", False)

    mock_patch.assert_not_called()
    mock_delete.assert_not_called()

    # Still refused after the longest backoff: the job ends
    job.no_pods_since -= datetime.timedelta(seconds=MAX_RETRY_BACKOFF)
    assert job.get_status(False) == ("Running", False)  # Failing
    mock_patch.assert_called_once()


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_refusal_before_last_pod_ignored(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    mock_pods: MagicMock,
) -> None:
    """Test that a refusal from before a pod that was created after all is ignored.

    E.g. an operator raised a `LimitRange`'s maximum, the pod ran and
    failed, and the job waits to retry it: the old event doesn't end it,
    but a refusal of the retry does.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        mock_pods: The mocked listing of the job's pods.
    """
    mock_pods.return_value = MockObject({"items": []})
    job = new_job(delete_unschedulable=False)
    mock_read.return_value = job_without_pods(active=1)
    assert job.get_status(False)[0] == "Running"
    assert job.pods_seen_at is not None

    mock_read.return_value = job_without_pods(failed=1)
    mock_events.return_value = events(event(OVER_MAX, minutes=5))
    assert job.get_status(False) == ("Running", False)  # Waits CREATE_CHECK_DELAY
    assert job.no_pods_since is not None
    job.no_pods_since -= datetime.timedelta(seconds=CREATE_CHECK_DELAY)
    job.next_create_check = None
    with patch("kubernetes.client.BatchV1Api.patch_namespaced_job") as mock_patch:
        assert job.get_status(False) == ("Running", False)
        mock_patch.assert_not_called()

        mock_events.return_value = events(event(OVER_MAX, minutes=5), event(INVALID))
        job.next_create_check = None
        assert job.get_status(False) == ("Running", False)
        mock_patch.assert_called_once()
    mock_delete.assert_not_called()


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_events_forbidden_shared(
    mock_read: MagicMock,
    mock_events: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that after one refusal, no job of the task reads events, nor warns.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.side_effect = ApiException(status=403, reason="Forbidden")

    for _ in range(2):
        assert new_job().get_status(False) == ("Running", False)

    mock_events.assert_called_once()
    assert caplog.text.count("Can't read the events") == 1


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_max_queue_checked_between_reads(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that the max queue time ends a job at the next poll, not the next read.

    The events are read then, for the log, even before their next read is due.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        caplog: The fixture that captures the log.
    """
    mock_read.return_value = job_without_pods()
    mock_events.return_value = events(event(QUOTA))
    job = new_job(max_queue=60)
    job.next_create_check = datetime.datetime.now(timezone.utc) + datetime.timedelta(
        minutes=10
    )
    assert job.no_pods_since is not None
    job.no_pods_since -= datetime.timedelta(seconds=61)

    assert job.get_status(False) == ("Failed", False)

    mock_events.assert_called_once()
    mock_delete.assert_called_once()
    assert f"was created within 60 s: {QUOTA}" in caplog.text


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_failed_pod_counts_as_seen(
    mock_read: MagicMock, mock_events: MagicMock
) -> None:
    """Test that a failed pod, also one never seen running, filters old refusals.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
    """
    mock_read.return_value = job_without_pods(failed=1)
    mock_events.return_value = events(event(INVALID, minutes=5))
    job = new_job(delete_unschedulable=False)

    with patch("kubernetes.client.BatchV1Api.patch_namespaced_job") as mock_patch:
        assert job.get_status(False) == ("Running", False)

    assert job.pods_seen_at is not None
    mock_patch.assert_not_called()


@patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_retry_refused_before_check_ends(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
    mock_pods: MagicMock,
) -> None:
    """Test that a retry refused between two checks ends the job.

    The pod fails and its retry is refused before the taskmaster sees the
    job without pods: the refusal is newer than the last check that saw the
    pod, so it counts.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
        mock_pods: The mocked listing of the job's pods.
    """
    mock_pods.return_value = MockObject({"items": []})
    job = new_job()
    mock_read.return_value = job_without_pods(active=1)
    assert job.get_status(False)[0] == "Running"
    # The check that saw the pod was two minutes ago
    two_minutes = datetime.timedelta(minutes=2)
    assert job.pods_seen_at is not None
    assert job.last_create_check is not None
    job.pods_seen_at -= two_minutes
    job.last_create_check -= two_minutes

    mock_read.return_value = job_without_pods(failed=1)
    mock_events.return_value = events(event(OVER_MAX, minutes=1))
    assert job.get_status(False) == ("Running", False)  # Waits CREATE_CHECK_DELAY
    job.next_create_check = None

    # A job whose pod failed is failed and kept, for the pod's logs
    with patch("kubernetes.client.BatchV1Api.patch_namespaced_job") as mock_patch:
        assert job.get_status(False) == ("Running", False)
        mock_patch.assert_called_once()
    mock_delete.assert_not_called()


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_never_had_pods_waits_since_start(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that a job that never had a pod has waited since it started.

    So a taskmaster that restarted doesn't start the max queue time over.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
    """
    started = datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=5)
    mock_read.return_value = job_without_pods(started=started)
    mock_events.return_value = events(event(QUOTA))
    job = new_job(max_queue=60)
    job.no_pods_since = None

    assert job.get_status(False) == ("Failed", False)

    assert job.no_pods_since == started
    mock_delete.assert_called_once()


@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_job_with_failed_pod_waits_since_seen(
    mock_read: MagicMock, mock_events: MagicMock
) -> None:
    """Test that a job whose pod failed has waited since the taskmaster saw it.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
    """
    started = datetime.datetime.now(timezone.utc) - datetime.timedelta(hours=1)
    mock_read.return_value = job_without_pods(started=started, failed=1)
    mock_events.return_value = events(event(QUOTA))
    job = new_job(max_queue=60)
    job.no_pods_since = None

    assert job.get_status(False) == ("Running", False)

    assert job.no_pods_since is not None
    assert job.no_pods_since > started


@patch("tesk_core.job.Job.delete")
@patch("kubernetes.client.CoreV1Api.list_namespaced_event")
@patch("kubernetes.client.BatchV1Api.read_namespaced_job")
def test_resumed_job_waits_since_resumed(
    mock_read: MagicMock,
    mock_events: MagicMock,
    mock_delete: MagicMock,
) -> None:
    """Test that the time a job was suspended doesn't count as waiting.

    E.g. a queue such as Kueue creates the job suspended and admits it an
    hour later: Kubernetes sets its start time then, before its first pod.

    Args:
        mock_read: The mocked reading of the job.
        mock_events: The mocked listing of the job's events.
        mock_delete: The mocked deletion of the job.
    """
    mock_events.return_value = events()
    job = new_job(max_queue=600)
    job.no_pods_since = None
    now = datetime.datetime.now(timezone.utc)
    created = now - datetime.timedelta(hours=1)
    mock_read.return_value = job_without_pods(suspend=True, created=created)
    assert job.get_status(False) == ("Running", False)

    # Resumed, before the job controller has seen it: the start time is
    # still the one from before the suspension
    suspended = MockObject({"type": "Suspended", "status": "True"})
    mock_read.return_value = job_without_pods(
        created=created, started=created, conditions=[suspended]
    )
    assert job.get_status(False) == ("Running", False)
    assert job.no_pods_since is None

    resumed = datetime.datetime.now(timezone.utc)
    mock_read.return_value = job_without_pods(created=created, started=resumed)
    assert job.get_status(False) == ("Running", False)

    assert job.no_pods_since == resumed
    mock_delete.assert_not_called()
