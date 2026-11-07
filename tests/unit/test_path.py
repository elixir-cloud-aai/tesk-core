"""Tests for the check that `file:` paths lie below `HOST_BASE_PATH`."""

from unittest import mock

import pytest

from tesk_core.exception import InvalidHostPath
from tesk_core.path import containerPath, isDescendant, validatePath

BASE = "/tmp"


@pytest.mark.parametrize(
    "path",
    [
        "/tmp",
        "/tmp/",
        "/tmp/a",
        "/tmp/a/b",
        "/tmp/a/../b",
        "/tmp/./a",
        "/tmp//a",
        "//tmp/a",
    ],
)
def test_isDescendant_inside(path: str) -> None:
    """Test that paths at or below the base path are descendants."""
    assert isDescendant(BASE, path)


@pytest.mark.parametrize(
    "path",
    [
        "/tmp2",
        "/tmp2/a",
        "/tmpfoo/a",
        "/tmp/..",
        "/tmp/../etc/passwd",
        "/tmp/a/../../etc",
        "/",
        "/etc",
        "tmp/a",
        "a/b",
        "",
    ],
)
def test_isDescendant_outside(path: str) -> None:
    """Test that siblings, paths leading out and relative paths are outside.

    Siblings that share a prefix with the base path, paths that `..`
    segments lead out of the base path, and relative paths are not
    descendants.
    """
    assert not isDescendant(BASE, path)


def test_isDescendant_double_slash_base() -> None:
    """Test that a base path with a leading `//` matches like one with `/`."""
    assert isDescendant("//tmp", "/tmp/a")
    assert isDescendant("//tmp", "//tmp/a")
    assert not isDescendant("//tmp", "/tmp2/a")


def test_isDescendant_relative_base() -> None:
    """Test that nothing is below a relative base path."""
    assert not isDescendant("tmp", "/tmp/a")
    assert not isDescendant("tmp", "tmp/a")


def test_isDescendant_root_base() -> None:
    """Test that every absolute path is below `/` as the base path."""
    assert isDescendant("/", "/a/b")
    assert not isDescendant("/", "a/b")


@mock.patch("tesk_core.path.HOST_BASE_PATH", BASE)
@pytest.mark.parametrize("path", ["/tmp2/a", "/tmp/../etc/passwd"])
def test_validatePath_rejects(path: str) -> None:
    """Test that paths outside the base path raise `InvalidHostPath`."""
    with pytest.raises(InvalidHostPath):
        validatePath(path)


@mock.patch("tesk_core.path.HOST_BASE_PATH", BASE)
@mock.patch("tesk_core.path.CONTAINER_BASE_PATH", "/transfer")
def test_containerPath_normalises() -> None:
    """Test that `..` segments below the base path are normalised.

    Such paths are mapped to their normalised location in the container.
    """
    assert containerPath("/tmp/a/../b/c") == "/transfer/b/c"
    assert containerPath("/tmp") == "/transfer"


@mock.patch("tesk_core.path.HOST_BASE_PATH", BASE)
@mock.patch("tesk_core.path.CONTAINER_BASE_PATH", None)
def test_containerPath_without_container_base_path() -> None:
    """Test that paths can't be mapped without `CONTAINER_BASE_PATH`.

    They raise an error instead of becoming relative.
    """
    with pytest.raises(ValueError, match="CONTAINER_BASE_PATH"):
        containerPath("/tmp/a")
