"""Tests of the base class of the filer's transfers."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from tesk_core.transput import Transput, Type


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Set `HOME` to an empty directory, without a `.netrc`.

    Args:
        tmp_path: The directory.
        monkeypatch: The fixture that restores `HOME` afterwards.

    Returns:
        The directory.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def transput() -> Transput:
    """Describe a file transfer.

    Returns:
        The transfer.
    """
    return Transput("/tmp/a", "https://example.org/a", Type.File)


@pytest.mark.parametrize(
    "method", ["download_file", "download_dir", "upload_file", "upload_dir"]
)
def test_transfers_not_implemented(home: Path, method: str) -> None:
    """Test that the base class implements no transfer."""
    with pytest.raises(NotImplementedError):
        getattr(transput(), method)()


def test_unknown_type_fails_download(home: Path) -> None:
    """Test that a download of neither a file nor a directory fails."""
    transfer = transput()
    transfer.ftype = "OTHER"  # type: ignore[assignment]  # not a `Type`

    assert transfer.download() == 1


def test_unknown_type_fails_upload(home: Path) -> None:
    """Test that an upload of neither a file nor a directory fails.

    Without the check for links, which could fail first on the host's path.
    """
    transfer = transput()
    transfer.ftype = "OTHER"  # type: ignore[assignment]  # not a `Type`

    assert transfer.upload(check_links=False) == 1


def test_invalid_netrc_is_an_error(
    home: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a `.netrc` that can't be parsed is logged, and not used."""
    (home / ".netrc").write_text("machine\n")
    (home / ".netrc").chmod(0o600)

    with caplog.at_level(logging.ERROR):
        transfer = transput()

    assert transfer.netrc_file is None
    assert "NetrcParseError" in caplog.text


def test_netrc_without_home(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the `.netrc` is looked for in `/` if `HOME` is unset.

    `netrc` is mocked, so that a `/.netrc` of the host doesn't count.
    """
    monkeypatch.delenv("HOME", raising=False)

    with (
        caplog.at_level(logging.DEBUG),
        patch(
            "tesk_core.transput.netrc.netrc", side_effect=FileNotFoundError
        ) as mock_netrc,
    ):
        transfer = transput()

    mock_netrc.assert_called_once_with("/.netrc")
    assert transfer.netrc_file is None
    assert "No .netrc at /.netrc" in caplog.text
