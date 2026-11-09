"""Tests of the log configuration of the taskmaster and the filer."""

from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from tesk_core.filer import HTTPTransput, Type, logConfig
from tesk_core.taskmaster import log_level, newLogger
from tesk_core.transput import Transput
from tesk_core.Util import LIBRARY_LOGGERS

QUERY = "token=QUERYVALUE123"


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """Restore the root logger and the library loggers after each test.

    Yields:
        Nothing, while the test runs.
    """
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    levels = {name: logging.getLogger(name).level for name in LIBRARY_LOGGERS}
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    for name, value in levels.items():
        logging.getLogger(name).setLevel(value)


def test_job_import_configures_no_logging() -> None:
    """Test that importing `tesk_core.job` leaves the root logger alone."""
    code = (
        "import logging, tesk_core.job; "
        "root = logging.getLogger(); "
        "print(len(root.handlers), root.level)"
    )
    result = subprocess.run(  # noqa: S603 -- fixed command
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.split() == ["0", str(logging.WARNING)]


@pytest.mark.parametrize("level", [logging.DEBUG, logging.ERROR])
def test_taskmaster_log_level(level: int) -> None:
    """Test that the taskmaster's level applies, also after another setup."""
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    logger = newLogger(level)

    assert logging.getLogger().level == level
    assert logger.getEffectiveLevel() == level
    assert logging.getLogger("tesk_core.job").getEffectiveLevel() == level


@pytest.mark.parametrize(
    "debug, shown, hidden",
    [(False, ["info", "warning", "error"], ["debug"]), (True, ["debug"], [])],
)
def test_taskmaster_default_level(
    debug: bool,
    shown: list[str],
    hidden: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that warnings and info show without `--debug`, debug only with it."""
    logger = logging.getLogger("tesk_core.job")
    newLogger(log_level(debug))
    for name in ("debug", "info", "warning", "error"):
        getattr(logger, name)(f"message-{name}")

    err = capsys.readouterr().err
    for name in shown:
        assert f"message-{name}" in err
    for name in hidden:
        assert f"message-{name}" not in err


def test_taskmaster_log_format(capsys: pytest.CaptureFixture[str]) -> None:
    """Test that taskmaster messages have a 24-hour time stamp and level."""
    logger = newLogger(logging.DEBUG)
    logger.debug("hello")

    err = capsys.readouterr().err
    assert " DEBUG: hello" in err
    assert err[:4].isdigit()


@pytest.mark.parametrize(
    "level, library_level",
    [
        (logging.DEBUG, logging.WARNING),
        (logging.INFO, logging.WARNING),
        (logging.ERROR, logging.ERROR),
    ],
)
def test_filer_library_log_levels(level: int, library_level: int) -> None:
    """Test that the HTTP and S3 libraries log at `WARNING` or above."""
    logConfig(level)

    assert logging.getLogger().level == level
    for name in LIBRARY_LOGGERS:
        assert logging.getLogger(name).level == library_level


class _Handler(BaseHTTPRequestHandler):
    """Answers every GET with a short body."""

    def do_GET(self) -> None:
        """Answer a GET request."""
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args: object) -> None:
        """Log nothing.

        Args:
            format: The message format.
            *args: Its arguments.
        """


@pytest.fixture
def http_url() -> Iterator[str]:
    """Run a local HTTP server.

    Yields:
        Its base URL.
    """
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def test_filer_debug_log_has_no_query(
    http_url: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that a download's debug log leaves out the URL's query."""
    for var in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(var, raising=False)
    logConfig(logging.DEBUG)
    url = f"{http_url}/a.txt?{QUERY}"

    assert HTTPTransput(str(tmp_path / "a.txt"), url, Type.File).download() == 0

    out = capsys.readouterr().out
    assert "/a.txt" in out
    assert "QUERYVALUE123" not in out


class _Transput(Transput):
    """A transput that transfers nothing."""

    def download_file(self) -> int:
        """Download nothing.

        Returns:
            0.
        """
        return 0

    def upload_file(self) -> int:
        """Upload nothing.

        Returns:
            0.
        """
        return 0

    def upload_dir(self) -> int:
        """Upload nothing.

        Returns:
            0.
        """
        return 0

    def download_dir(self) -> int:
        """Download nothing.

        Returns:
            0.
        """
        return 0


def test_missing_netrc_is_no_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a missing `.netrc` is logged at `DEBUG` only."""
    monkeypatch.setenv("HOME", str(tmp_path))
    caplog.set_level(logging.DEBUG)

    transput = _Transput("/tmp/a", "https://example.org/a", Type.File)

    assert transput.netrc_file is None
    assert not [r for r in caplog.records if r.levelno > logging.DEBUG]
    assert ".netrc" in caplog.text


def test_unreadable_netrc_is_an_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a `.netrc` that can't be read is still logged as an error."""
    (tmp_path / ".netrc").mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))

    _Transput("/tmp/a", "https://example.org/a", Type.File)

    assert [r for r in caplog.records if r.levelno == logging.ERROR]
