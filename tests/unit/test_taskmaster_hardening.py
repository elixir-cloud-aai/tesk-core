"""Tests of cancellation, signals and settings of the taskmaster."""

import argparse
from argparse import Namespace
import logging
import signal
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import MaxRetryError

from tesk_core import taskmaster
from tesk_core.job import DELETE_TIMEOUT, Job
from tesk_core.taskmaster import (
    main,
    on_sigterm,
    positive_seconds,
    run_task,
    stop_task_jobs,
    task_job_names,
)


@pytest.fixture(autouse=True)
def module_state(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Set the arguments and a mocked Kubernetes client of the taskmaster.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.

    Returns:
        The mocked client of the batch API.
    """
    api = MagicMock()
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(namespace="tesk", pull_policy_always=False, debug=False),
    )
    monkeypatch.setattr(taskmaster, "batch_api", api)
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)
    monkeypatch.setattr(taskmaster, "poll_interval", 5)
    return api


def task(inputs: bool = True, outputs: bool = True) -> dict[str, Any]:
    """Make a task with two executors.

    Args:
        inputs: Whether it has an input.
        outputs: Whether it has an output.

    Returns:
        The task, as the taskmaster gets it.
    """
    return {
        "executors": [
            {
                "metadata": {
                    "name": f"task-1-ex-{index:02d}",
                    "labels": {"taskmaster-name": "task-1"},
                }
            }
            for index in range(2)
        ],
        "volumes": [],
        "inputs": [{"path": "/in", "url": "s3://b/in"}] if inputs else [],
        "outputs": [{"path": "/out", "url": "s3://b/out"}] if outputs else [],
    }


def job(*conditions: tuple[str, str]) -> MagicMock:
    """Make a job, as read from the Kubernetes API.

    Args:
        *conditions: The type and status of each condition.

    Returns:
        The job.
    """
    result = MagicMock()
    result.status.conditions = [
        MagicMock(type=kind, status=status) for kind, status in conditions
    ]
    return result


@pytest.mark.parametrize(
    "inputs, outputs, expected",
    [
        (
            True,
            True,
            [
                "task-1-inputs-filer",
                "task-1-ex-00",
                "task-1-ex-01",
                "task-1-outputs-filer",
            ],
        ),
        (False, False, ["task-1-ex-00", "task-1-ex-01"]),
    ],
)
def test_task_job_names(inputs: bool, outputs: bool, expected: list[str]) -> None:
    """Test that the jobs are named in the order the taskmaster runs them."""
    assert task_job_names(task(inputs, outputs)) == expected


def test_task_job_names_without_executors() -> None:
    """Test that an invalid task without executors names no jobs."""
    assert task_job_names({}) == []


def test_stop_task_jobs(
    module_state: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that running jobs are deleted, ended and missing ones skipped.

    A job that can't be read is deleted; errors are logged, not raised.
    """
    module_state.read_namespaced_job.side_effect = [
        ApiException(status=404),  # inputs filer: gone
        job(("Complete", "True")),  # executor 0: ended
        job(("Failed", "False")),  # executor 1: running
        ApiException(status=500, reason="Server Error"),  # outputs filer
    ]
    module_state.delete_namespaced_job.side_effect = [
        None,
        ApiException(status=404),
    ]

    with caplog.at_level(logging.ERROR):
        stop_task_jobs(task())

    deleted = [c.args[0] for c in module_state.delete_namespaced_job.call_args_list]
    assert deleted == ["task-1-ex-01", "task-1-outputs-filer"]
    for call in module_state.delete_namespaced_job.call_args_list:
        assert call.args[1] == "tesk"
        assert call.kwargs["_request_timeout"] == DELETE_TIMEOUT
        assert call.kwargs["body"].propagation_policy == "Background"
    assert "Can't read job 'task-1-outputs-filer': 500 Server Error" in caplog.text
    assert "Can't delete" not in caplog.text


def test_stop_task_jobs_errors(
    module_state: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that transport and API errors are logged, and all jobs tried."""
    error = MaxRetryError(MagicMock(), "/")
    module_state.read_namespaced_job.side_effect = [
        error,
        job(),
        MagicMock(status=None),
        job(("Suspended", "True")),
    ]
    module_state.delete_namespaced_job.side_effect = [
        error,
        ApiException(status=500, reason="Server Error"),
        None,
        None,
    ]

    with caplog.at_level(logging.ERROR):
        stop_task_jobs(task())

    assert module_state.delete_namespaced_job.call_count == 4
    assert "Can't read job 'task-1-inputs-filer'" in caplog.text
    assert "Can't delete job 'task-1-inputs-filer'" in caplog.text
    assert "Can't delete job 'task-1-ex-00' (500 Server Error)" in caplog.text


@patch("tesk_core.taskmaster.stop_task_jobs")
@patch("tesk_core.taskmaster.check_cancelled", return_value=True)
@patch("tesk_core.taskmaster.config")
def test_main_cancelled_during_init_stops_jobs(
    mock_config: MagicMock,
    mock_cancelled: MagicMock,
    mock_stop: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a task cancelled before it runs stops an earlier attempt's jobs.

    An earlier attempt of the taskmaster's pod can have left jobs running.
    """
    monkeypatch.setattr("sys.argv", ["taskmaster", "-n", "tesk", "-p", "0.5", "{}"])
    monkeypatch.setattr(taskmaster, "init_own_job", MagicMock())

    with patch("tesk_core.taskmaster.signal.signal"), pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
    mock_stop.assert_called_once_with({})
    assert taskmaster.poll_interval == 0.5


@pytest.mark.parametrize(
    "value, expected",
    [
        ("5", 5.0),
        ("0.5", 0.5),
        (" 2 ", 2.0),
        ("1e1", 10.0),
        ("0.1", 0.1),
        ("3600", 3600.0),
    ],
)
def test_positive_seconds(value: str, expected: float) -> None:
    """Test that positive numbers of seconds are accepted."""
    assert positive_seconds(value) == expected


@pytest.mark.parametrize(
    "value", ["0", "-1", "x", "", "nan", "inf", "0.01", "3601", "1e300"]
)
def test_positive_seconds_invalid(value: str) -> None:
    """Test that other values are rejected."""
    with pytest.raises(argparse.ArgumentTypeError):
        positive_seconds(value)


def test_poll_interval_invalid(capsys: pytest.CaptureFixture[str]) -> None:
    """Test that an invalid `--poll-interval` stops with a usage error."""
    with pytest.raises(SystemExit) as exc:
        taskmaster.newParser().parse_args(["-p", "0", "{}"])

    assert exc.value.code == 2
    assert "not a number from 0.1 to 3600: '0'" in capsys.readouterr().err


@patch("tesk_core.taskmaster.run_main", side_effect=KeyboardInterrupt)
def test_main_interrupt_deletes_jobs(
    mock_run_main: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that `SIGINT` deletes the jobs that the taskmaster created."""
    created = MagicMock(spec=Job)
    monkeypatch.setattr(taskmaster, "created_jobs", [created])

    with (
        patch("tesk_core.taskmaster.signal.signal") as mock_signal,
        pytest.raises(SystemExit) as exc,
    ):
        main()

    assert exc.value.code == 130
    created.delete_quietly.assert_called_once_with()
    mock_signal.assert_called_once_with(signal.SIGTERM, on_sigterm)


def test_on_sigterm_keeps_jobs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that `SIGTERM` exits as failed and keeps the jobs."""
    created = MagicMock(spec=Job)
    monkeypatch.setattr(taskmaster, "created_jobs", [created])

    with pytest.raises(SystemExit) as exc:
        on_sigterm(signal.SIGTERM, None)

    assert exc.value.code == 143
    created.delete_quietly.assert_not_called()
    created.delete.assert_not_called()
    assert "SIGTERM" in caplog.text


@pytest.mark.parametrize(
    "value, expected, logged",
    [
        ("2", 2, False),
        (" 0 ", 0, False),
        ("", None, False),
        (" ", None, False),
        ("x", None, True),
        ("-1", None, True),
    ],
)
@patch("tesk_core.taskmaster.init_pvc")
@patch("tesk_core.taskmaster.run_executor")
@patch("tesk_core.taskmaster.check_cancelled", return_value=False)
@patch("tesk_core.taskmaster.Filer")
def test_filer_backoff_limit(
    mock_filer: MagicMock,
    mock_cancelled: MagicMock,
    mock_run_executor: MagicMock,
    mock_init_pvc: MagicMock,
    value: str,
    expected: int | None,
    logged: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that `FILER_BACKOFF_LIMIT` is applied if valid, else ignored.

    Invalid values are logged as errors; empty ones mean unset.
    """
    monkeypatch.setenv("FILER_BACKOFF_LIMIT", value)
    monkeypatch.delenv("TESK_FTP_USERNAME", raising=False)

    with caplog.at_level(logging.ERROR):
        run_task(task(inputs=True, outputs=False), "filer", "v0")

    set_limit = mock_filer.return_value.set_backoffLimit
    if expected is None:
        set_limit.assert_not_called()
    else:
        set_limit.assert_called_once_with(expected)
    assert ("Ignoring FILER_BACKOFF_LIMIT" in caplog.text) == logged


def test_delete_quietly(caplog: pytest.LogCaptureFixture) -> None:
    """Test that deleting a job times out, and tolerates gone jobs and errors."""
    job_ = Job({"metadata": {}}, "job-1", "tesk")
    job_.bv1 = MagicMock()
    job_.bv1.delete_namespaced_job.side_effect = [
        None,
        ApiException(status=404),
        ApiException(status=500),
        MaxRetryError(MagicMock(), "/"),
    ]

    with caplog.at_level(logging.ERROR):
        for _ in range(4):
            job_.delete_quietly()

    for call in job_.bv1.delete_namespaced_job.call_args_list:
        assert call.kwargs["_request_timeout"] == DELETE_TIMEOUT
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert errors[0] == "Can't delete job 'job-1' (500)"
    assert errors[1].startswith("Can't delete job 'job-1' (")
    assert len(errors) == 2


def test_run_to_completion_cancelled_deletes_quietly() -> None:
    """Test that a cancelled job is deleted without raising errors."""
    job_ = Job({"metadata": {}}, "job-1", "tesk")
    job_.bv1 = MagicMock()
    job_.bv1.delete_namespaced_job.side_effect = ApiException(status=500)

    with patch.object(Job, "get_status", return_value=("Running", False)):
        status = job_.run_to_completion(0, lambda: True, 240)

    assert status == "Cancelled"
    assert (
        job_.bv1.delete_namespaced_job.call_args.kwargs["_request_timeout"]
        == DELETE_TIMEOUT
    )
