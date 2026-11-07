"""Tests for the owner references and the TTL of the taskmaster's job."""

from argparse import Namespace
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest

from tesk_core import taskmaster
from tesk_core.taskmaster import init_own_job, new_job, run_task, task_ttl_seconds

RESOURCES = Path(__file__).parent / "resources"
POD_LABELS = 'batch.kubernetes.io/job-name="task-1234"\napp="tesk"'
OWNER = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "name": "task-1234",
    "uid": "0a1b2c3d",
}


@pytest.fixture(autouse=True)
def labels_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Reset the module state, and point it to a pod labels file.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
        tmp_path: A directory for the labels file.

    Returns:
        The path of the labels file, which doesn't exist yet.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(
            namespace="tesk",
            debug=False,
            pull_policy_always=False,
            pod_timeout=240,
        ),
    )
    monkeypatch.setattr(taskmaster, "batch_api", None)
    monkeypatch.setattr(taskmaster, "owner_references", [])
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)
    monkeypatch.delenv(taskmaster.task_ttl_env, raising=False)
    path = tmp_path / "labels"
    monkeypatch.setattr(taskmaster, "pod_labels_file", str(path))
    return path


def own_job() -> MagicMock:
    """Make the taskmaster's job, as read from the Kubernetes API.

    Returns:
        The job.
    """
    job = MagicMock()
    job.metadata.name = "task-1234"
    job.metadata.uid = "0a1b2c3d"
    return job


@pytest.mark.parametrize(
    ("value", "expected"),
    [("0", 0), ("3600", 3600), (" 60 ", 60), ("2147483647", 2147483647)],
)
def test_task_ttl_seconds(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: int
) -> None:
    """Test that whole numbers of seconds set the TTL.

    Args:
        monkeypatch: The fixture that sets the environment.
        value: The setting.
        expected: The TTL.
    """
    monkeypatch.setenv(taskmaster.task_ttl_env, value)

    assert task_ttl_seconds() == expected


@pytest.mark.parametrize("value", [None, "", "  "])
def test_task_ttl_seconds_unset(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str | None,
) -> None:
    """Test that an unset or empty setting sets no TTL, quietly.

    Args:
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
        value: The setting, or `None` for unset.
    """
    if value is not None:
        monkeypatch.setenv(taskmaster.task_ttl_env, value)

    assert task_ttl_seconds() is None
    assert not caplog.records


@pytest.mark.parametrize("value", ["-1", "abc", "1.5", "1e3", "2147483648", "٣"])
def test_task_ttl_seconds_invalid(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str,
) -> None:
    """Test that an invalid setting is logged and sets no TTL.

    Args:
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
        value: The setting.
    """
    monkeypatch.setenv(taskmaster.task_ttl_env, value)

    with caplog.at_level(logging.ERROR, logger=taskmaster.__name__):
        assert task_ttl_seconds() is None

    assert taskmaster.task_ttl_env in caplog.text


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_init_own_job(mock_api: MagicMock, labels_file: Path) -> None:
    """Test that the taskmaster's job owns the task's jobs, without a TTL.

    Args:
        mock_api: The mocked batch API.
        labels_file: The pod labels file.
    """
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.return_value = own_job()

    init_own_job()

    assert taskmaster.owner_references == [OWNER]
    mock_api.return_value.read_namespaced_job.assert_called_once_with(
        "task-1234", "tesk", _request_timeout=taskmaster.job_read_timeout
    )
    mock_api.return_value.patch_namespaced_job.assert_not_called()


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_init_own_job_ttl(
    mock_api: MagicMock, labels_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the TTL setting is set on the taskmaster's job.

    Args:
        mock_api: The mocked batch API.
        labels_file: The pod labels file.
        monkeypatch: The fixture that sets the environment.
    """
    labels_file.write_text(POD_LABELS)
    monkeypatch.setenv(taskmaster.task_ttl_env, "3600")
    mock_api.return_value.read_namespaced_job.return_value = own_job()

    init_own_job()

    mock_api.return_value.patch_namespaced_job.assert_called_once_with(
        "task-1234",
        "tesk",
        {"spec": {"ttlSecondsAfterFinished": 3600}},
        _request_timeout=taskmaster.job_read_timeout,
    )
    assert taskmaster.owner_references == [OWNER]


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_init_own_job_without_labels(mock_api: MagicMock) -> None:
    """Test that without the pod's labels (e.g. outside a pod), nothing is read.

    Args:
        mock_api: The mocked batch API.
    """
    init_own_job()

    assert taskmaster.owner_references == []
    mock_api.return_value.read_namespaced_job.assert_not_called()


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_init_own_job_read_error(
    mock_api: MagicMock,
    labels_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a job that can't be read is logged, and owns nothing.

    Args:
        mock_api: The mocked batch API.
        labels_file: The pod labels file.
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
    """
    labels_file.write_text(POD_LABELS)
    monkeypatch.setenv(taskmaster.task_ttl_env, "60")
    mock_api.return_value.read_namespaced_job.side_effect = ApiException(
        status=403, reason="Forbidden"
    )

    with caplog.at_level(logging.ERROR, logger=taskmaster.__name__):
        init_own_job()

    assert taskmaster.owner_references == []
    mock_api.return_value.patch_namespaced_job.assert_not_called()
    assert "Can't read job 'task-1234'" in caplog.text
    assert "403 Forbidden" in caplog.text


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_init_own_job_patch_error(
    mock_api: MagicMock,
    labels_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a TTL that can't be set is logged, and the task goes on.

    Args:
        mock_api: The mocked batch API.
        labels_file: The pod labels file.
        monkeypatch: The fixture that sets the environment.
        caplog: The fixture that captures the log.
    """
    labels_file.write_text(POD_LABELS)
    monkeypatch.setenv(taskmaster.task_ttl_env, "60")
    mock_api.return_value.read_namespaced_job.return_value = own_job()
    mock_api.return_value.patch_namespaced_job.side_effect = ApiException(
        status=403, reason="Forbidden"
    )

    with caplog.at_level(logging.ERROR, logger=taskmaster.__name__):
        init_own_job()

    assert taskmaster.owner_references == [OWNER]
    assert "Can't set the TTL of job 'task-1234'" in caplog.text


def test_new_job_owner_references(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that new jobs get the owner references, after any they have.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "owner_references", [OWNER])
    other = {"apiVersion": "v1", "kind": "ConfigMap", "name": "x", "uid": "1"}

    job = new_job({"metadata": {"ownerReferences": [other]}}, "task-1234-ex-00")
    bare = new_job({"metadata": {}}, "task-1234-ex-01")

    assert job.body["metadata"]["ownerReferences"] == [other, OWNER]
    assert bare.body["metadata"]["ownerReferences"] == [OWNER]
    assert job.namespace == "tesk"
    assert job.name == "task-1234-ex-00"


def test_new_job_without_owner() -> None:
    """Test that without owner references, the manifest gets none."""
    job = new_job({"metadata": {}}, "task-1234-ex-00")

    assert "ownerReferences" not in job.body["metadata"]


@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch(
    "tesk_core.taskmaster.Job.run_to_completion",
    autospec=True,
    return_value="Complete",
)
def test_run_task_jobs_owned(
    mock_run: MagicMock,
    mock_create: MagicMock,
    mock_delete: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the filer and executor jobs of a task are owned.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "owner_references", [OWNER])
    with (RESOURCES / "inputFile.json").open() as fh:
        data: dict[str, Any] = json.load(fh)
    data["inputs"] = [{"path": "/in/a", "url": "s3://b/a", "type": "FILE"}]
    data["outputs"] = [{"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}]

    run_task(data, "filer_name", "filer_version")

    jobs = [call.args[0] for call in mock_run.call_args_list]
    assert [job.name for job in jobs] == [
        "task-1000-inputs-filer",
        "task-1000-ex-00",
        "task-1000-outputs-filer",
    ]
    for job in jobs:
        assert job.body["metadata"]["ownerReferences"] == [OWNER]
