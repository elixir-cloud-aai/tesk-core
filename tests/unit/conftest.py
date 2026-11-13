"""Fixtures shared by the unit tests."""

import pytest

from tesk_core import taskmaster


@pytest.fixture(autouse=True)
def no_task_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the task that `taskmaster.main` keeps for `SIGTERM`.

    Tests that run `main` set it; without the reset, it would leak into
    later tests.

    Args:
        monkeypatch: The fixture that restores the module state afterwards.
    """
    monkeypatch.setattr(taskmaster, "task_data", None)
