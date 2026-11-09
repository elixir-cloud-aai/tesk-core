"""Tests of the base class of the filer's transfers and of the task's claim."""

import logging
from pathlib import Path

import pytest

from tesk_core.pvc import PVC
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


@pytest.mark.parametrize(
    "method", ["download_file", "download_dir", "upload_file", "upload_dir"]
)
def test_transfers_not_implemented(home: Path, method: str) -> None:
    """Test that the base class implements no transfer."""
    transput = Transput("/tmp/a", "https://example.org/a", Type.File)

    with pytest.raises(NotImplementedError):
        getattr(transput, method)()


@pytest.mark.parametrize("direction", ["upload", "download"])
def test_unknown_type_fails(home: Path, direction: str) -> None:
    """Test that a transfer of neither a file nor a directory fails."""
    transput = Transput("/tmp/a", "https://example.org/a", Type.File)
    transput.ftype = "OTHER"  # type: ignore[assignment]  # not a `Type`

    assert getattr(transput, direction)() == 1


def test_invalid_netrc_is_an_error(
    home: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a `.netrc` that can't be parsed is logged, and not used."""
    (home / ".netrc").write_text("machine\n")
    (home / ".netrc").chmod(0o600)

    with caplog.at_level(logging.ERROR):
        transput = Transput("/tmp/a", "https://example.org/a", Type.File)

    assert transput.netrc_file is None
    assert "NetrcParseError" in caplog.text


def test_netrc_without_home(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the `.netrc` is looked for in `/` if `HOME` is unset."""
    monkeypatch.delenv("HOME", raising=False)

    with caplog.at_level(logging.DEBUG):
        transput = Transput("/tmp/a", "https://example.org/a", Type.File)

    assert transput.netrc_file is None
    assert "No .netrc at /.netrc" in caplog.text


def test_pvc_storage_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that `STORAGE_CLASS_NAME` sets the claim's storage class."""
    monkeypatch.setenv("STORAGE_CLASS_NAME", "fast")

    pvc = PVC("pvc-1", 1, "tesk")

    assert pvc.spec["spec"]["storageClassName"] == "fast"


def test_pvc_default_storage_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the claim has the cluster's default storage class if unset."""
    monkeypatch.delenv("STORAGE_CLASS_NAME", raising=False)

    pvc = PVC("pvc-1", 1, "tesk")

    assert "storageClassName" not in pvc.spec["spec"]
