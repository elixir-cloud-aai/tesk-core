"""Tests of the limits that the taskmaster gives the executors.

The TESK API sets an executor's requests from the task's `cpu_cores` and
`ram_gb`, and its limits from `cpu_cores` only; the taskmaster fills in the
missing limits from the requests, per resource, so that a `LimitRange`
default limit below the request can't make the executor's pod invalid.
"""

from argparse import Namespace
import copy
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import taskmaster
from tesk_core.taskmaster import limits_from_requests, run_executor

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments and the created jobs of the taskmaster module.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(
        taskmaster, "args", Namespace(namespace="default", pod_timeout=240)
    )
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.delenv("EXECUTOR_BACKOFF_LIMIT", raising=False)


@pytest.fixture
def executor() -> dict[str, Any]:
    """Get the executor job of the task from the input file.

    Returns:
        The job's manifest.
    """
    task = json.loads((RESOURCES / "inputFile.json").read_text())
    job: dict[str, Any] = task["executors"][0]
    return job


def container(job: dict[str, Any]) -> dict[str, Any]:
    """Get the first container of a job.

    Args:
        job: The job's manifest.

    Returns:
        The container.
    """
    first: dict[str, Any] = job["spec"]["template"]["spec"]["containers"][0]
    return first


@pytest.mark.parametrize(
    ("resources", "expected"),
    [
        # `ram_gb` only: the TESK API sets both
        (
            {"requests": {"memory": "1Gi"}, "limits": {"memory": "1Gi"}},
            {"requests": {"memory": "1Gi"}, "limits": {"memory": "1Gi"}},
        ),
        # `cpu_cores` and `ram_gb`: the TESK API sets only the CPU limit
        (
            {"requests": {"cpu": "1", "memory": "512Mi"}, "limits": {"cpu": "1"}},
            {
                "requests": {"cpu": "1", "memory": "512Mi"},
                "limits": {"cpu": "1", "memory": "512Mi"},
            },
        ),
        (
            {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "1"}},
            {
                "requests": {"cpu": "1", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
        ),
        # No resources: none are added, so that a `LimitRange` applies
        ({}, {}),
        # Requests without limits, as before
        (
            {"requests": {"cpu": "1", "memory": "1Gi"}},
            {
                "requests": {"cpu": "1", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
        ),
        (
            {"requests": {"cpu": "1"}, "limits": None},
            {"requests": {"cpu": "1"}, "limits": {"cpu": "1"}},
        ),
        # Every resource, not only CPU and memory
        (
            {"requests": {"ephemeral-storage": "1Gi"}},
            {
                "requests": {"ephemeral-storage": "1Gi"},
                "limits": {"ephemeral-storage": "1Gi"},
            },
        ),
        # Limits without requests stay as they are
        ({"limits": {"cpu": "2"}}, {"limits": {"cpu": "2"}}),
        ({"requests": None, "limits": None}, {"requests": None, "limits": None}),
        ({"requests": {}}, {"requests": {}}),
    ],
)
def test_limits_from_requests(
    resources: dict[str, Any], expected: dict[str, Any]
) -> None:
    """Test that each missing limit is filled in from its request."""
    assert limits_from_requests(resources) == expected


def test_limits_that_are_set_are_kept() -> None:
    """Test that a limit set by the TESK API isn't replaced by its request.

    Also one that differs from the request: Kubernetes checks it.
    """
    resources = {
        "requests": {"cpu": "1", "memory": "1Gi"},
        "limits": {"cpu": "2", "memory": "4Gi"},
    }

    assert limits_from_requests(copy.deepcopy(resources)) == resources


def test_limits_are_not_the_requests_object() -> None:
    """Test that the limits are a dict of their own, not the requests.

    Otherwise a change of one would change the other, and the manifest
    would serialise the same object twice.
    """
    resources: dict[str, Any] = {"requests": {"cpu": "1"}}

    limits_from_requests(resources)

    assert resources["limits"] is not resources["requests"]


@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
@patch("tesk_core.taskmaster.logger")
def test_executor_with_cpu_and_memory_gets_memory_limit(
    mock_logger: MagicMock, mock_run: MagicMock, executor: dict[str, Any]
) -> None:
    """Test the regression of #160: an executor with CPUs and memory.

    The TESK API sets the CPU limit only; the memory limit was left unset,
    so that a namespace's default memory limit below the request made the
    executor's pod invalid, and the task ran forever.
    """
    container(executor)["resources"] = {
        "requests": {"cpu": "1", "memory": "1Gi"},
        "limits": {"cpu": "1"},
    }

    run_executor(executor, "default")

    assert container(executor)["resources"] == {
        "requests": {"cpu": "1", "memory": "1Gi"},
        "limits": {"cpu": "1", "memory": "1Gi"},
    }


@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
@patch("tesk_core.taskmaster.logger")
def test_executor_without_resources_field(
    mock_logger: MagicMock, mock_run: MagicMock, executor: dict[str, Any]
) -> None:
    """Test that an executor without `resources` runs without them."""
    del container(executor)["resources"]

    run_executor(executor, "default")

    assert "resources" not in container(executor)
