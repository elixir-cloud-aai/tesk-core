"""Tests for the command line parser and `run_task` of the taskmaster."""

from argparse import Namespace
from collections.abc import Callable
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import taskmaster
from tesk_core.job import Job
from tesk_core.taskmaster import newLogger, newParser, run_task

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the created jobs and claim of the taskmaster module.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)


def jobRunToCompletionMock(
    job: Job,
    poll_interval: float,
    check_cancelled: Callable[[], bool],
    pod_timeout: int,
) -> str:
    """Mock `Job.run_to_completion`.

    Args:
        job: The job.
        poll_interval: Unused.
        check_cancelled: Unused.
        pod_timeout: Unused.

    Returns:
        The state `Complete`.
    """
    print(f'[mock] Creating job "{job.name}"...')
    return "Complete"


def test_defaults() -> None:
    """Test the default values of the command line arguments."""
    parser = newParser()

    args = parser.parse_args(["json"])

    print(args)

    assert args == Namespace(
        debug=False,
        file=None,
        filer_name="eu.gcr.io/tes-wes/filer",
        filer_version="v0.1.9",
        json="json",
        namespace="default",
        poll_interval=5,
        pod_timeout=240,
        state_file="/tmp/.teskstate",
        localKubeConfig=False,
        pull_policy_always=False,
    )


def test_localKubeConfig() -> None:
    """Test that `--localKubeConfig` sets the option."""
    parser = newParser()

    args = parser.parse_args(["json", "--localKubeConfig"])

    print(args)

    assert args == Namespace(
        debug=False,
        file=None,
        filer_name="eu.gcr.io/tes-wes/filer",
        filer_version="v0.1.9",
        json="json",
        namespace="default",
        poll_interval=5,
        pod_timeout=240,
        state_file="/tmp/.teskstate",
        localKubeConfig=True,
        pull_policy_always=False,
    )


def test_pullPolicyAlways() -> None:
    """Test that `--pull-policy-always` sets the option."""
    parser = newParser()

    assert parser.parse_args(["json"]).pull_policy_always is False
    assert (
        parser.parse_args(["json", "--pull-policy-always"]).pull_policy_always is True
    )


@patch(
    "tesk_core.taskmaster.args",
    Namespace(
        debug=True, namespace="default", pull_policy_always=True, pod_timeout=240
    ),
)
@patch("tesk_core.taskmaster.logger", newLogger(logging.DEBUG))
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", jobRunToCompletionMock)
def test_run_task(mock_create: MagicMock, mock_delete: MagicMock) -> None:
    """Test that `run_task` runs the task from the input file.

    The inputs filer, the executor and the outputs filer run in this order,
    in a claim that is created first and deleted at the end.

    Args:
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        data = json.load(fh)

    run_task(data, "filer_name", "filer_version")

    assert [job.name for job in taskmaster.created_jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
        "task-1000-outputs-filer",
    ]
    mock_create.assert_called_once_with()
    mock_delete.assert_called_once()
    assert mock_delete.call_args.args[0].name == "task-1000-pvc"
    # Forgotten once deleted, so that stopping deletes it no more
    assert taskmaster.created_pvc is None
