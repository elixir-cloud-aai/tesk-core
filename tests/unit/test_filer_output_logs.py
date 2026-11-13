"""Tests for the output file logs of the filer (TES `logs[].outputs`)."""

from collections.abc import Iterator
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import filer, path
from tesk_core.filer import OUTPUT_LOGS_MARKER, output_file_logs


@pytest.fixture
def prefix(tmp_path: Path) -> Iterator[Path]:
    """Mount the task's paths below a temporary directory.

    Args:
        tmp_path: The temporary directory.

    Yields:
        The directory.
    """
    with patch.object(path, "TASK_PATH_PREFIX", str(tmp_path)):
        yield tmp_path


def write(prefix: Path, relative: str, content: str) -> None:
    """Write a file of the task.

    Args:
        prefix: The directory below which the task's paths are.
        relative: The file's path, below `prefix`.
        content: The file's content.
    """
    target = prefix / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)


def test_file_output(prefix: Path) -> None:
    """Test that a file output gives its URL, path and size."""
    write(prefix, "out/c.txt", "abc")
    output = {"path": "/out/c.txt", "url": "s3://b/c.txt", "type": "FILE"}

    assert output_file_logs(output) == [
        {"url": "s3://b/c.txt", "path": "/out/c.txt", "size_bytes": "3"}
    ]


def test_directory_output(prefix: Path) -> None:
    """Test that a directory output gives each file below it, in order.

    HTTP URLs are escaped per component, as for the upload; empty
    directories give nothing, and symbolic links are left out.
    """
    write(prefix, "out/b.txt", "bb")
    write(prefix, "out/a dir/x.txt", "x")
    (prefix / "out/empty").mkdir()
    os.symlink(prefix / "out/b.txt", prefix / "out/link.txt")
    output = {"path": "/out/", "url": "https://h/up/", "type": "DIRECTORY"}

    assert output_file_logs(output) == [
        {
            "url": "https://h/up/a%20dir/x.txt",
            "path": "/out/a dir/x.txt",
            "size_bytes": "1",
        },
        {"url": "https://h/up/b.txt", "path": "/out/b.txt", "size_bytes": "2"},
    ]


def test_wildcard_output(prefix: Path) -> None:
    """Test that a wildcard output gives the files of every match.

    The paths are those of the matches; the URLs drop `path_prefix`, as the
    upload does. Matched directories give every file below them.
    """
    write(prefix, "data/s1/out.txt", "1")
    write(prefix, "data/s2/out.txt", "22")
    write(prefix, "data/d/out.txt/inner.txt", "333")
    output = {
        "path": "/data/*/out.txt",
        "path_prefix": "/data",
        "url": "s3://b/res",
        "type": "FILE",
    }

    assert output_file_logs(output) == [
        {
            "url": "s3://b/res/d/out.txt/inner.txt",
            "path": "/data/d/out.txt/inner.txt",
            "size_bytes": "3",
        },
        {"url": "s3://b/res/s1/out.txt", "path": "/data/s1/out.txt", "size_bytes": "1"},
        {"url": "s3://b/res/s2/out.txt", "path": "/data/s2/out.txt", "size_bytes": "2"},
    ]


@pytest.mark.parametrize(
    "output",
    [
        {"path": "/missing.txt", "url": "s3://b/m", "type": "FILE"},
        {"path": "/x/*.txt", "path_prefix": "/x", "url": "s3://b/x", "type": "FILE"},
    ],
)
def test_nothing_uploaded(prefix: Path, output: dict[str, Any]) -> None:
    """Test that an output without files gives no logs.

    Args:
        prefix: The directory below which the task's paths are.
        output: An output whose path is missing, or matches nothing.
    """
    assert output_file_logs(output) == []


def test_symlink_output(prefix: Path) -> None:
    """Test that an output that is a symbolic link gives no logs.

    The upload refuses such outputs.

    Args:
        prefix: The directory below which the task's paths are.
    """
    write(prefix, "out/c.txt", "abc")
    os.symlink(prefix / "out/c.txt", prefix / "out/link.txt")
    output = {"path": "/out/link.txt", "url": "s3://b/l", "type": "FILE"}

    assert output_file_logs(output) == []


def run_main(monkeypatch: pytest.MonkeyPatch, ttype: str, data: dict[str, Any]) -> int:
    """Run the filer's command line, with its logging setup mocked.

    Args:
        monkeypatch: The fixture that sets the command line.
        ttype: `inputs` or `outputs`.
        data: The task.

    Returns:
        The exit code.
    """
    monkeypatch.setattr("sys.argv", ["filer", ttype, json.dumps(data)])
    with patch("tesk_core.filer.logConfig"):
        return filer.main()


def marker_lines(text: str) -> list[str]:
    """Get the lines with output file logs.

    Args:
        text: The filer's standard output.

    Returns:
        The lines that start with `OUTPUT_LOGS_MARKER`.
    """
    return [line for line in text.splitlines() if line.startswith(OUTPUT_LOGS_MARKER)]


@patch("tesk_core.filer.process_file", return_value=0)
def test_main_prints_output_logs(
    mock_process: MagicMock,
    prefix: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that the outputs filer prints the logs of all outputs as one line.

    The line is printed whatever the log level, after the last upload.

    Args:
        mock_process: The mocked upload.
        prefix: The directory below which the task's paths are.
        monkeypatch: The fixture that sets the command line.
        capsys: The fixture that captures the standard output.
    """
    write(prefix, "a.txt", "a")
    write(prefix, "b.txt", "bb")
    outputs = [
        {"path": "/a.txt", "url": "s3://b/a", "type": "FILE"},
        {"path": "/b.txt", "url": "s3://b/b", "type": "FILE"},
    ]

    assert run_main(monkeypatch, "outputs", {"inputs": [], "outputs": outputs}) == 0

    lines = marker_lines(capsys.readouterr().out)
    assert len(lines) == 1
    assert json.loads(lines[0][len(OUTPUT_LOGS_MARKER) + 1 :]) == [
        {"url": "s3://b/a", "path": "/a.txt", "size_bytes": "1"},
        {"url": "s3://b/b", "path": "/b.txt", "size_bytes": "2"},
    ]


@pytest.mark.parametrize(("ttype", "status"), [("inputs", 0), ("outputs", 1)])
def test_main_prints_no_output_logs(
    prefix: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ttype: str,
    status: int,
) -> None:
    """Test that no logs are printed for inputs, nor after a failed upload.

    Args:
        prefix: The directory below which the task's paths are.
        monkeypatch: The fixture that sets the command line.
        capsys: The fixture that captures the standard output.
        ttype: `inputs` or `outputs`.
        status: The result of each input's or output's staging.
    """
    write(prefix, "a.txt", "a")
    files = [{"path": "/a.txt", "url": "s3://b/a", "type": "FILE"}]

    with patch("tesk_core.filer.process_file", return_value=status):
        assert (
            run_main(monkeypatch, ttype, {"inputs": files, "outputs": files}) == status
        )

    assert marker_lines(capsys.readouterr().out) == []
