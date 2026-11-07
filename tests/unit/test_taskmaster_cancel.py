"""Tests for how the taskmaster finds out that its task was cancelled."""

from argparse import Namespace
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
import pytest
from urllib3.exceptions import MaxRetryError

from tesk_core import taskmaster
from tesk_core.taskmaster import check_cancelled, main, read_pod_labels

# As the Downward API writes them: one `key="value"` per line, sorted by key,
# without a newline after the last line
POD_LABELS = 'batch.kubernetes.io/job-name="task-1234"\njob-name="task-1234"'


@pytest.fixture(autouse=True)
def labels_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Set the arguments and the pod labels file of the taskmaster module.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
        tmp_path: A directory for the labels file.

    Returns:
        The path of the labels file, which doesn't exist yet.
    """
    monkeypatch.setattr(taskmaster, "args", Namespace(namespace="tesk"))
    monkeypatch.setattr(taskmaster, "batch_api", None)
    path = tmp_path / "labels"
    monkeypatch.setattr(taskmaster, "pod_labels_file", str(path))
    return path


def taskmaster_job(labels: dict[str, str] | None) -> MagicMock:
    """Make a taskmaster job, as read from the Kubernetes API.

    Args:
        labels: The labels of the job.

    Returns:
        The job.
    """
    job = MagicMock()
    job.metadata.labels = labels
    return job


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_job_label(mock_api: MagicMock, labels_file: Path) -> None:
    """Test that a cancelled job counts, also when its pod has no label.

    The TESK API labels only a running pod: a task cancelled while its
    taskmaster's pod was pending, or before the pod existed, is labelled on
    its job only (elixir-cloud-aai/TESK#219).
    """
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.return_value = taskmaster_job(
        {"job-type": "taskmaster", "task-status": "Cancelled"}
    )

    assert check_cancelled()
    mock_api.return_value.read_namespaced_job.assert_called_once_with(
        "task-1234", "tesk"
    )


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_pod_label(mock_api: MagicMock, labels_file: Path) -> None:
    """Test that a cancelled pod counts, also with labels after its label."""
    labels_file.write_text(
        'job-name="task-1234"\ntask-status="Cancelled"\nz="last"',
    )

    assert check_cancelled()
    mock_api.return_value.read_namespaced_job.assert_not_called()


@pytest.mark.parametrize("labels", [None, {"job-type": "taskmaster"}])
@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_running(
    mock_api: MagicMock, labels_file: Path, labels: dict[str, str] | None
) -> None:
    """Test that a task whose pod and job have no cancel label runs on."""
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.return_value = taskmaster_job(labels)

    assert not check_cancelled()


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_older_job_name_label(
    mock_api: MagicMock, labels_file: Path
) -> None:
    """Test that the job is found by `job-name` alone (Kubernetes < 1.27)."""
    labels_file.write_text('job-name="task-1234"')
    mock_api.return_value.read_namespaced_job.return_value = taskmaster_job(
        {"task-status": "Cancelled"}
    )

    assert check_cancelled()
    mock_api.return_value.read_namespaced_job.assert_called_once_with(
        "task-1234", "tesk"
    )


@pytest.mark.parametrize("contents", [None, 'app="taskmaster"'])
@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_without_job(
    mock_api: MagicMock, labels_file: Path, contents: str | None
) -> None:
    """Test that a taskmaster that knows no job of its own runs on.

    Outside a job (e.g. run by hand), there is no labels file, or no label
    with the job's name.
    """
    if contents is not None:
        labels_file.write_text(contents)

    assert not check_cancelled()
    mock_api.return_value.read_namespaced_job.assert_not_called()


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_api_error(
    mock_api: MagicMock, labels_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that an error reading the job doesn't stop the taskmaster.

    The pod's label still cancels it; the job is read again at the next poll.
    """
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.side_effect = ApiException(
        status=403, reason="Forbidden"
    )

    with caplog.at_level(logging.WARNING, logger=taskmaster.logger.name):
        assert not check_cancelled()

    assert "Can't read job 'task-1234'" in caplog.text


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_connection_error(
    mock_api: MagicMock, labels_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the API server being unreachable doesn't stop the taskmaster."""
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.side_effect = MaxRetryError(
        pool=MagicMock(), url="/apis/batch/v1"
    )

    with caplog.at_level(logging.WARNING, logger=taskmaster.logger.name):
        assert not check_cancelled()

    assert "Can't read job 'task-1234'" in caplog.text


@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_check_cancelled_reuses_client(mock_api: MagicMock, labels_file: Path) -> None:
    """Test that the polls share one API client."""
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.return_value = taskmaster_job(None)

    check_cancelled()
    check_cancelled()

    mock_api.assert_called_once_with()
    assert mock_api.return_value.read_namespaced_job.call_count == 2


@pytest.mark.parametrize(
    ("contents", "labels"),
    [
        ("", {}),
        ("\n", {}),
        ("no-separator", {}),
        ('a="x=y"', {"a": "x=y"}),
        ('a="1"\nb="2"\n', {"a": "1", "b": "2"}),
    ],
)
def test_read_pod_labels(
    labels_file: Path, contents: str, labels: dict[str, str]
) -> None:
    """Test that the labels file is parsed line by line, quotes removed."""
    labels_file.write_text(contents)

    assert read_pod_labels() == labels


@patch("tesk_core.taskmaster.run_task")
@patch("tesk_core.taskmaster.config.load_incluster_config")
@patch("tesk_core.taskmaster.client.BatchV1Api")
def test_main_cancelled_before_start(
    mock_api: MagicMock,
    mock_config: MagicMock,
    mock_run_task: MagicMock,
    labels_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a task cancelled before its taskmaster runs never starts.

    Its pod has no cancel label (elixir-cloud-aai/TESK#219), so the label of
    its job must stop it before the first job is created.
    """
    labels_file.write_text(POD_LABELS)
    mock_api.return_value.read_namespaced_job.return_value = taskmaster_job(
        {"task-status": "Cancelled"}
    )
    monkeypatch.setattr("sys.argv", ["taskmaster", "-n", "tesk", "{}"])

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 0
    mock_run_task.assert_not_called()
