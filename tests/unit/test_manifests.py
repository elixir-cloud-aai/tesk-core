"""Tests that the manifests the taskmaster creates are valid Kubernetes objects.

The Kubernetes client checks a manifest against its models when it sends
it; the other unit tests mock the client's calls, so these tests check the
manifests against the same models (`model_validate`), which reject unknown
fields.
"""

from argparse import Namespace
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes import client
import pytest

from tesk_core import taskmaster
from tesk_core.filer_class import Filer
from tesk_core.pvc import PVC
from tesk_core.taskmaster import generate_mounts, run_executor

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Get a task with an input, an output and a volume.

    Args:
        monkeypatch: The fixture that sets the taskmaster's arguments.

    Returns:
        The task.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(namespace="tesk", pod_timeout=240, pull_policy_always=False),
    )
    task: dict[str, Any] = json.loads((RESOURCES / "inputFile.json").read_text())
    task["outputs"] = [
        {"path": "/out/a.txt", "url": "https://example.org/a", "type": "FILE"}
    ]
    task["volumes"] = ["/vol"]
    return task


@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
def test_executor_job_is_valid(
    mock_run: MagicMock, data: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that an executor job with the task's PVC is a valid job."""
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    pvc = PVC("task-1000-pvc", 1, "tesk")
    pvc.set_volume_mounts(generate_mounts(data, pvc))
    executor = data["executors"][0]

    run_executor(executor, "tesk", pvc)

    client.V1Job.model_validate(executor)


@pytest.mark.parametrize("mode", ["inputs", "outputs"])
def test_filer_job_is_valid(data: dict[str, Any], mode: str) -> None:
    """Test that a filer job, with all its options, is a valid job."""
    pvc = PVC("task-1000-pvc", 1, "tesk")
    pvc.set_volume_mounts(generate_mounts(data, pvc))
    filer = Filer("task-1000-filer", data, "filer", "1.0", True, "task-1000")
    filer.set_ftp("user", "password")
    filer.set_backoffLimit(2)
    filer.add_volume_mount(pvc)
    filer.add_netrc_mount("netrc")

    client.V1Job.model_validate(filer.get_spec(mode, debug=True))


def test_pvc_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the task's PVC, with a storage class, is a valid claim."""
    monkeypatch.setenv("STORAGE_CLASS_NAME", "fast")

    client.V1PersistentVolumeClaim.model_validate(PVC("pvc-1", 1, "tesk").spec)
