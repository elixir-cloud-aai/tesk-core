"""Tests of the task's persistent volume claim."""

import pytest

from tesk_core.pvc import PVC


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
