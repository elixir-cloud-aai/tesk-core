"""Tests for the output file logs of the filer (TES `logs[].outputs`)."""

from collections.abc import Iterator
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import filer, path
from tesk_core.filer import OUTPUT_LOGS_MARKER, output_file_logs, output_file_url


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


def entry(output: int, relative: str, size: int) -> dict[str, Any]:
    """Describe an uploaded file as the filer lists it.

    Args:
        output: The index of the output.
        relative: The file's path below the output's.
        size: The file's size.

    Returns:
        The entry.
    """
    return {"output": output, "relative": relative, "size_bytes": str(size)}


def test_file_output(prefix: Path) -> None:
    """Test that a file output gives its size, without a URL.

    Args:
        prefix: The directory below which the task's paths are.
    """
    write(prefix, "out/c.txt", "abc")
    output = {"path": "/out/c.txt", "url": "s3://b/c.txt?token=x", "type": "FILE"}

    assert output_file_logs([output]) == [entry(0, "", 3)]


def test_directory_output(prefix: Path) -> None:
    """Test that a directory output gives each file below it, in order.

    Files come by name before the subdirectories; empty directories give
    nothing, and symbolic links are left out.

    Args:
        prefix: The directory below which the task's paths are.
    """
    write(prefix, "out/b.txt", "bb")
    write(prefix, "out/a dir/x.txt", "x")
    write(prefix, "out/a.txt", "a")
    (prefix / "out/empty").mkdir()
    os.symlink(prefix / "out/b.txt", prefix / "out/link.txt")
    output = {"path": "/out/", "url": "https://h/up/", "type": "DIRECTORY"}

    assert output_file_logs([output]) == [
        entry(0, "a.txt", 1),
        entry(0, "b.txt", 2),
        entry(0, "a dir/x.txt", 1),
    ]


def test_wildcard_output(prefix: Path) -> None:
    """Test that a wildcard output gives the files of every match.

    The paths are below `path_prefix`; matched directories give every file
    below them.

    Args:
        prefix: The directory below which the task's paths are.
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

    assert output_file_logs([output]) == [
        entry(0, "d/out.txt/inner.txt", 3),
        entry(0, "s1/out.txt", 1),
        entry(0, "s2/out.txt", 2),
    ]


@pytest.mark.parametrize(
    "output",
    [
        {"path": "/missing.txt", "url": "s3://b/m", "type": "FILE"},
        {"path": "/x/*.txt", "path_prefix": "/x", "url": "s3://b/x", "type": "FILE"},
        {"path": "/out/link.txt", "url": "s3://b/l", "type": "FILE"},
    ],
)
def test_nothing_uploaded(prefix: Path, output: dict[str, Any]) -> None:
    """Test that an output without files gives no logs.

    Args:
        prefix: The directory below which the task's paths are.
        output: An output whose path is missing, matches nothing, or is a
            symbolic link (which the upload refuses).
    """
    write(prefix, "out/c.txt", "abc")
    os.symlink(prefix / "out/c.txt", prefix / "out/link.txt")

    assert output_file_logs([output]) == []


def test_several_outputs(prefix: Path) -> None:
    """Test that the files of each output give its index.

    Args:
        prefix: The directory below which the task's paths are.
    """
    write(prefix, "a.txt", "a")
    write(prefix, "b.txt", "bb")
    outputs = [
        {"path": "/a.txt", "url": "s3://b/a", "type": "FILE"},
        {"path": "/b.txt", "url": "s3://b/b", "type": "FILE"},
    ]

    assert output_file_logs(outputs) == [entry(0, "", 1), entry(1, "", 2)]


def test_too_many_files(
    prefix: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the files beyond the limit are left out, with a warning.

    Args:
        prefix: The directory below which the task's paths are.
        monkeypatch: The fixture that sets the limit.
        caplog: The fixture that captures the log.
    """
    for name in ("a", "b", "c"):
        write(prefix, f"out/{name}", "x")
    one = len(json.dumps(entry(0, "a", 1), separators=(",", ":")))
    monkeypatch.setattr(filer, "OUTPUT_LOGS_MAX_BYTES", 2 + 2 * (one + 1))
    output = {"path": "/out", "url": "s3://b/out", "type": "DIRECTORY"}

    assert output_file_logs([output]) == [entry(0, "a", 1), entry(0, "b", 1)]
    assert "only the first 2 uploaded files" in caplog.text


@pytest.mark.parametrize(
    ("output", "relative", "expected"),
    [
        (
            {"path": "/out/c.txt", "url": "s3://b/c?t=1", "type": "FILE"},
            "",
            ("s3://b/c?t=1", "/out/c.txt"),
        ),
        (
            {"path": "/out/", "url": "https://h/up/?t=1", "type": "DIRECTORY"},
            "a dir/x.txt",
            ("https://h/up/a%20dir/x.txt?t=1", "/out/a dir/x.txt"),
        ),
        (
            {"path": "/out", "url": "s3://b/up/", "type": "DIRECTORY"},
            "x.txt",
            ("s3://b/up/x.txt", "/out/x.txt"),
        ),
        (
            {
                "path": "/data/*/out.txt",
                "path_prefix": "/data/",
                "url": "s3://b/res",
                "type": "FILE",
            },
            "s1/out.txt",
            ("s3://b/res/s1/out.txt", "/data/s1/out.txt"),
        ),
    ],
)
def test_output_file_url(
    output: dict[str, Any], relative: str, expected: tuple[str, str]
) -> None:
    """Test that a file's URL and path are derived as the upload does.

    Args:
        output: The output.
        relative: The file's path below the output's.
        expected: The URL and the path.
    """
    assert output_file_url(output, relative) == expected


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

    The line is printed whatever the log level, after the last upload, and
    holds no URLs.

    Args:
        mock_process: The mocked upload.
        prefix: The directory below which the task's paths are.
        monkeypatch: The fixture that sets the command line.
        capsys: The fixture that captures the standard output.
    """
    write(prefix, "a.txt", "a")
    write(prefix, "b.txt", "bb")
    outputs = [
        {"path": "/a.txt", "url": "s3://b/a?secret=x", "type": "FILE"},
        {"path": "/b.txt", "url": "s3://b/b", "type": "FILE"},
    ]

    assert run_main(monkeypatch, "outputs", {"inputs": [], "outputs": outputs}) == 0

    out = capsys.readouterr().out
    lines = marker_lines(out)
    assert len(lines) == 1
    assert json.loads(lines[0][len(OUTPUT_LOGS_MARKER) + 1 :]) == [
        entry(0, "", 1),
        entry(1, "", 2),
    ]
    assert "secret" not in out


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


@patch("tesk_core.filer.process_file", return_value=0)
@patch("tesk_core.filer.output_file_logs", side_effect=OSError("gone"))
def test_main_output_logs_error(
    mock_logs: MagicMock,
    mock_process: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an error listing the files is logged; the upload succeeds.

    Args:
        mock_logs: The listing of the files, which fails.
        mock_process: The mocked upload.
        monkeypatch: The fixture that sets the command line.
        capsys: The fixture that captures the standard output.
        caplog: The fixture that captures the log.
    """
    files = [{"path": "/a.txt", "url": "s3://b/a", "type": "FILE"}]

    assert run_main(monkeypatch, "outputs", {"inputs": [], "outputs": files}) == 0

    assert marker_lines(capsys.readouterr().out) == []
    assert "Can't list the uploaded files: gone" in caplog.text
