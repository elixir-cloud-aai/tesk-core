"""Tests for the helpers and the run logic of the taskmaster."""

from argparse import Namespace
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest

from tesk_core import taskmaster
from tesk_core.filer_class import Filer
from tesk_core.taskmaster import (
    PVC,
    append_mount,
    dirname,
    generate_mounts,
    init_pvc,
    newParser,
    run_executor,
    run_task,
)

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments and the created jobs of the taskmaster module.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
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
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)


@pytest.fixture
def data() -> dict[str, Any]:
    """Get the task from the input file.

    Returns:
        The task as read from the JSON file.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        task: dict[str, Any] = json.load(fh)
    return task


@pytest.fixture
def task_name(data: dict[str, Any]) -> str:
    """Get the name of the task.

    Args:
        data: The task.

    Returns:
        The task name from the label of the first executor.
    """
    name: str = data["executors"][0]["metadata"]["labels"]["taskmaster-name"]
    return name


@pytest.fixture
def filer(data: dict[str, Any], task_name: str) -> Filer:
    """Get the filer of the task.

    Args:
        data: The task.
        task_name: The name of the task.

    Returns:
        The filer for the task.
    """
    return Filer(
        task_name + "-filer",
        data,
        taskmaster.args.filer_name,
        taskmaster.args.filer_version,
        taskmaster.args.pull_policy_always,
    )


@pytest.fixture
def pvc(data: dict[str, Any], task_name: str) -> PVC:
    """Get the volume claim of the task.

    Args:
        data: The task.
        task_name: The name of the task.

    Returns:
        The persistent volume claim for the task.
    """
    return PVC(
        task_name + "-pvc",
        data["resources"]["disk_gb"],
        taskmaster.args.namespace,
    )


@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
@patch("tesk_core.taskmaster.logger")
def test_pvc_creation(
    mock_logger: MagicMock,
    mock_run_to_compl: MagicMock,
    mock_pvc_create: MagicMock,
    data: dict[str, Any],
    filer: Filer,
) -> None:
    """Test that the PVC volume is created successfully."""
    assert isinstance(init_pvc(data, filer), PVC)


@patch("kubernetes.client.CoreV1Api.read_namespaced_persistent_volume_claim")
@patch(
    "kubernetes.client.CoreV1Api.create_namespaced_persistent_volume_claim",
    side_effect=ApiException(status=409, reason="conflict"),
)
def test_create_pvc_check_for_conflict_exception(
    mock_create_namespaced_pvc: MagicMock,
    mock_read_namespaced_pvc: MagicMock,
    pvc: PVC,
) -> None:
    """Test that an existing claim (conflict) is read instead."""
    pvc.create()
    mock_read_namespaced_pvc.assert_called_once()


@patch(
    "kubernetes.client.CoreV1Api.create_namespaced_persistent_volume_claim",
    side_effect=ApiException(status=500, reason="Random error"),
)
def test_create_pvc_check_for_other_exceptions(
    mock_create_namespaced_pvc: MagicMock, pvc: PVC
) -> None:
    """Test that other errors than a conflict are raised."""
    with pytest.raises(ApiException):
        pvc.create()


@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="error")
@patch("tesk_core.taskmaster.logger")
def test_pvc_failure(
    mock_logger: MagicMock,
    run_to_compl: MagicMock,
    mock_pvc_create: MagicMock,
    mock_pvc_delete: MagicMock,
    data: dict[str, Any],
    filer: Filer,
) -> None:
    """Test that a failed PVC creation exits."""
    with pytest.raises(SystemExit):
        init_pvc(data, filer)


@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.Job.delete")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Error")
@patch("tesk_core.taskmaster.logger")
def test_run_executor_failure(
    mock_logger: MagicMock,
    mock_run_to_compl: MagicMock,
    mock_job_delete: MagicMock,
    mock_pvc_delete: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a failed executor exits."""
    with pytest.raises(SystemExit):
        run_executor(data["executors"][0], taskmaster.args.namespace)


@patch("tesk_core.taskmaster.PVC")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
@patch("tesk_core.taskmaster.logger")
def test_run_executor_complete(
    mock_logger: MagicMock,
    mock_run_to_compl: MagicMock,
    mock_pvc: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a completed executor job runs once, without exiting."""
    run_executor(data["executors"][0], taskmaster.args.namespace, mock_pvc)
    mock_run_to_compl.assert_called_once()


@patch("tesk_core.taskmaster.logger")
def test_generate_mount(mock_logger: MagicMock, data: dict[str, Any], pvc: PVC) -> None:
    """Test that the volume mounts are generated as a list."""
    assert isinstance(generate_mounts(data, pvc), list)


@patch("tesk_core.taskmaster.logger")
def test_append_mount(mock_logger: MagicMock, data: dict[str, Any], pvc: PVC) -> None:
    """Test that the mount of an input directory is appended."""
    volume_mounts: list[dict[str, Any]] = []
    task_volume_name = "task-volume"
    for aninput in data["inputs"]:
        dirnm = dirname(aninput)
        append_mount(volume_mounts, task_volume_name, dirnm, pvc)
    assert volume_mounts == [
        {
            "name": task_volume_name,
            "mountPath": "/some/volume",
            "subPath": "dir0",
        }
    ]


@patch("tesk_core.taskmaster.logger")
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.PVC.delete")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
def test_run_task(
    mock_job: MagicMock,
    mock_pvc_create: MagicMock,
    mock_pvc_delete: MagicMock,
    mock_logger: MagicMock,
    data: dict[str, Any],
) -> None:
    """Test that a task runs through."""
    run_task(data, taskmaster.args.filer_name, taskmaster.args.filer_version)


def test_localKubeConfig() -> None:
    """Test that `--localKubeConfig` sets the option."""
    parser = newParser()
    args = parser.parse_args(["json", "--localKubeConfig"])
    assert args == Namespace(
        debug=False,
        file=None,
        filer_version="v0.1.9",
        json="json",
        namespace="default",
        poll_interval=5,
        state_file="/tmp/.teskstate",
        localKubeConfig=True,
        pull_policy_always=False,
        filer_name="eu.gcr.io/tes-wes/filer",
        pod_timeout=240,
    )
