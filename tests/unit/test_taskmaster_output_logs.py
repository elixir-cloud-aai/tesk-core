"""Tests for the output file logs that the taskmaster stores (TES `logs[].outputs`)."""

from argparse import Namespace
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import ProtocolError

from tesk_core import taskmaster
from tesk_core.filer import OUTPUT_LOGS_MARKER
from tesk_core.taskmaster import (
    output_logs_annotation,
    parse_output_logs,
    record_output_logs,
    run_task,
)

RESOURCES = Path(__file__).parent / "resources"
OUTPUTS = [
    {"path": "/out/a", "url": "s3://b/a?t=1", "type": "FILE"},
    {"path": "/out/d", "url": "https://h/d", "type": "DIRECTORY"},
]
FILES = [
    {"output": 0, "relative": "", "size_bytes": "1"},
    {"output": 1, "relative": "x y", "size_bytes": "22"},
]
LOGS = [
    {"url": "s3://b/a?t=1", "path": "/out/a", "size_bytes": "1"},
    {"url": "https://h/d/x%20y", "path": "/out/d/x y", "size_bytes": "22"},
]
JOB = "task-1000-outputs-filer"


@pytest.fixture(autouse=True)
def taskmaster_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the arguments, the created jobs and the claim of the taskmaster.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(
        taskmaster,
        "args",
        Namespace(
            debug=False, namespace="ns", pull_policy_always=False, pod_timeout=240
        ),
    )
    monkeypatch.setattr(taskmaster, "created_jobs", [])
    monkeypatch.setattr(taskmaster, "created_pvc", None)


def line(files: Any) -> str:
    """Format the uploaded files as the filer prints them.

    Args:
        files: The files.

    Returns:
        The line.
    """
    return f"{OUTPUT_LOGS_MARKER} {json.dumps(files)}"


@pytest.mark.parametrize(
    ("log", "expected"),
    [
        (f"start\n{line(FILES)}\n", FILES),
        (f"{line([])}\n", []),
        # The last line counts, e.g. after a logged path with a forged line
        (f"{line([FILES[0]])}\n{line(FILES)}", FILES),
        ("no logs\n", None),
        (f"{OUTPUT_LOGS_MARKER} not JSON\n", None),
        (line({"output": 0}), None),
        (line(["u"]), None),
        (line([{"output": 0, "relative": ""}]), None),
        (line([{**FILES[0], "url": "u"}]), None),
        (line([{**FILES[0], "output": 2}]), None),
        (line([{**FILES[0], "output": -1}]), None),
        (line([{**FILES[0], "output": True}]), None),
        (line([{**FILES[0], "output": "0"}]), None),
        (line([{**FILES[0], "relative": 1}]), None),
        (line([{**FILES[0], "size_bytes": 1}]), None),
        (line([{**FILES[0], "size_bytes": "-1"}]), None),
        (line([{**FILES[0], "size_bytes": "²"}]), None),
    ],
)
def test_parse_output_logs(log: str, expected: list[dict[str, Any]] | None) -> None:
    """Test that the filer's last line with uploaded files is read and checked.

    Args:
        log: The log of the filer's pod.
        expected: The files, or `None` if there are none or they are invalid.
    """
    assert parse_output_logs(log, len(OUTPUTS)) == expected


def pod(name: str, phase: str) -> SimpleNamespace:
    """Describe a pod as the Kubernetes API lists it.

    Args:
        name: The pod's name.
        phase: The pod's phase.

    Returns:
        The pod.
    """
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name), status=SimpleNamespace(phase=phase)
    )


@pytest.fixture
def core() -> MagicMock:
    """Mock the client of the core API, with a failed and a succeeded pod.

    Returns:
        The client.
    """
    mock = MagicMock()
    mock.list_namespaced_pod.return_value = SimpleNamespace(
        items=[pod("p-1", "Failed"), pod("p-2", "Succeeded")]
    )
    mock.read_namespaced_pod_log.return_value = line(FILES) + "\n"
    return mock


@pytest.fixture
def batch(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock the client of the batch API.

    Args:
        monkeypatch: The fixture that sets the client.

    Returns:
        The client.
    """
    mock = MagicMock()
    monkeypatch.setattr(taskmaster, "batch_api", mock)
    return mock


def stored(batch: MagicMock) -> Any:
    """Get the output file logs that the taskmaster stored.

    Args:
        batch: The mocked client of the batch API.

    Returns:
        The logs.
    """
    body = batch.patch_namespaced_job.call_args.args[2]
    return json.loads(body["metadata"]["annotations"][output_logs_annotation])


def test_record_output_logs(core: MagicMock, batch: MagicMock) -> None:
    """Test that the files of the succeeded pod are stored with their URLs.

    Args:
        core: The mocked client of the core API.
        batch: The mocked client of the batch API.
    """
    with patch("tesk_core.taskmaster.client.CoreV1Api", return_value=core):
        record_output_logs(JOB, OUTPUTS)

    core.list_namespaced_pod.assert_called_once()
    assert core.list_namespaced_pod.call_args.kwargs["label_selector"] == (
        f"job-name={JOB}"
    )
    assert core.read_namespaced_pod_log.call_args.args[:2] == ("p-2", "ns")
    assert core.read_namespaced_pod_log.call_args.kwargs["tail_lines"] == 10
    batch.patch_namespaced_job.assert_called_once()
    assert batch.patch_namespaced_job.call_args.args[:2] == (JOB, "ns")
    assert stored(batch) == LOGS


def test_record_output_logs_cut(
    core: MagicMock,
    batch: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that logs longer than the annotation's limit are cut, with a warning.

    Args:
        core: The mocked client of the core API.
        batch: The mocked client of the batch API.
        monkeypatch: The fixture that sets the limit.
        caplog: The fixture that captures the log.
    """
    monkeypatch.setattr(taskmaster, "max_output_logs_bytes", 80)

    with patch("tesk_core.taskmaster.client.CoreV1Api", return_value=core):
        record_output_logs(JOB, OUTPUTS)

    assert stored(batch) == LOGS[:1]
    body = batch.patch_namespaced_job.call_args.args[2]
    assert len(body["metadata"]["annotations"][output_logs_annotation]) <= 80
    assert "only 1 of 2" in caplog.text


@pytest.mark.parametrize(
    ("pods", "log"),
    [
        ([pod("p-1", "Failed")], line(FILES)),
        ([pod("p-2", "Succeeded")], "no logs"),
    ],
)
def test_record_output_logs_none(
    core: MagicMock,
    batch: MagicMock,
    caplog: pytest.LogCaptureFixture,
    pods: list[SimpleNamespace],
    log: str,
) -> None:
    """Test that nothing is stored without a succeeded pod or files in its log.

    Args:
        core: The mocked client of the core API.
        batch: The mocked client of the batch API.
        caplog: The fixture that captures the log.
        pods: The pods of the filer's job.
        log: The log of the pods.
    """
    core.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
    core.read_namespaced_pod_log.return_value = log

    with patch("tesk_core.taskmaster.client.CoreV1Api", return_value=core):
        record_output_logs(JOB, OUTPUTS)

    batch.patch_namespaced_job.assert_not_called()
    assert "Can't read the output file logs" in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        ApiException(status=403),
        ProtocolError("x"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
    ],
)
@pytest.mark.parametrize(
    "failing", ["list_namespaced_pod", "read_namespaced_pod_log", "patch"]
)
def test_record_output_logs_errors(
    core: MagicMock,
    batch: MagicMock,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    failing: str,
) -> None:
    """Test that errors are logged, not raised: the task completes.

    Args:
        core: The mocked client of the core API.
        batch: The mocked client of the batch API.
        caplog: The fixture that captures the log.
        error: The error, of the Kubernetes API or of decoding the log.
        failing: The request that fails.
    """
    if failing == "patch":
        batch.patch_namespaced_job.side_effect = error
    else:
        getattr(core, failing).side_effect = error

    with patch("tesk_core.taskmaster.client.CoreV1Api", return_value=core):
        record_output_logs(JOB, OUTPUTS)

    assert "Can't store the output file logs" in caplog.text


@patch("tesk_core.taskmaster.record_output_logs")
@patch("tesk_core.taskmaster.PVC.delete", autospec=True)
@patch("tesk_core.taskmaster.PVC.create")
@patch("tesk_core.taskmaster.Job.run_to_completion", return_value="Complete")
def test_run_task_records_output_logs(
    mock_run: MagicMock,
    mock_create: MagicMock,
    mock_delete: MagicMock,
    mock_record: MagicMock,
) -> None:
    """Test that the logs are recorded once the outputs filer has completed.

    Args:
        mock_run: The mocked run of a job.
        mock_create: The mocked creation of the claim.
        mock_delete: The mocked deletion of the claim.
        mock_record: The mocked recording of the logs.
    """
    with (RESOURCES / "inputFile.json").open() as fh:
        data = json.load(fh)
    data["outputs"] = [{"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}]

    run_task(data, "filer_name", "filer_version")

    mock_record.assert_called_once_with("task-1000-outputs-filer", data["outputs"])
