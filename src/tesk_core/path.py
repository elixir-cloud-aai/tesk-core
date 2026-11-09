"""Paths of the filer: the task's paths, and the shared volume for `file:` URLs.

The task's paths are mounted below `TASK_PATH_PREFIX` in the filer. The
shared volume is mounted at `CONTAINER_BASE_PATH` in the filer, and is the
path `HOST_BASE_PATH` for the URLs of tasks.
"""

import os
from os.path import relpath
import posixpath
from urllib.parse import urlparse

from tesk_core.exception import InvalidHostPath


def getEnv(varName: str) -> str | None:
    """Get an environment variable.

    Args:
        varName: Name of the variable.

    Returns:
        The value of the variable, or `None` if it is not set.
    """
    return os.environ.get(varName)


def getPathEnv(varName: str) -> str | None:
    """Get a path from an environment variable and normalize it.

    Normalizing (e.g., removing trailing slashes) removes some cases from the
    rest of the code.

    Args:
        varName: Name of the variable.

    Returns:
        The normalized path, or `None` if the variable is not set or empty.
    """
    varContent = getEnv(varName)

    return os.path.normpath(varContent) if varContent else None


HOST_BASE_PATH = getPathEnv("HOST_BASE_PATH")
CONTAINER_BASE_PATH = getPathEnv("CONTAINER_BASE_PATH")
TRANSFER_PVC_NAME = getEnv("TRANSFER_PVC_NAME")

TASK_PATH_PREFIX = "/tesk/task"
"""Directory below which the filer mounts the task's paths.

The task's paths are mounted there in the filer's pod only, so that they
can't overlay the filer's own files and configuration; the executors mount
them at the task's paths.
"""


def taskPath(path: str) -> str:
    """Normalize a path of the task, as an absolute path.

    Resolves `..` segments (as far as the root) and `.` segments, and drops
    repeated and trailing slashes; glob characters are kept. Two spellings
    of a directory (e.g., `/out/` and `/out`) give the same path. An empty
    path stays empty.

    Args:
        path: The path, as in the task (e.g., `/data/../out/`).

    Returns:
        The normalized path (e.g., `/out`).
    """
    if not path:
        return path
    return "/" + posixpath.normpath("/" + path).lstrip("/")


def filerPath(path: str) -> str:
    """Translate a path of the task to the path in the filer.

    The path is normalized first (see `taskPath`), so that it stays below
    `TASK_PATH_PREFIX`.

    Args:
        path: The path, as in the task (e.g., `/data/in.txt`).

    Returns:
        The path below `TASK_PATH_PREFIX` (e.g., `/tesk/task/data/in.txt`);
        an empty path stays empty, as for the executors (e.g., the directory
        of a file directly below `/`, which Kubernetes refuses as a mount
        path in both pods).
    """
    if not path:
        return path
    relative = taskPath(path).lstrip("/")
    return posixpath.join(TASK_PATH_PREFIX, relative) if relative else TASK_PATH_PREFIX


def checkBasePaths() -> None:
    """Check that the shared volume's path and the task paths' don't overlap.

    Raises:
        ValueError: `CONTAINER_BASE_PATH` is inside or contains
            `TASK_PATH_PREFIX`.
    """
    base = CONTAINER_BASE_PATH
    if base is not None and (
        isDescendant(base, TASK_PATH_PREFIX) or isDescendant(TASK_PATH_PREFIX, base)
    ):
        raise ValueError(
            f"CONTAINER_BASE_PATH ({base}) must be outside {TASK_PATH_PREFIX}, "
            "where the filer mounts the task's paths, and not contain it"
        )


def fileEnabled() -> bool:
    """Tell whether `file:` URLs can be used.

    Returns:
        Whether both base paths of the shared volume are set.
    """
    return HOST_BASE_PATH is not None and CONTAINER_BASE_PATH is not None


def getPath(url: str) -> str:
    """Get the path of a URL.

    Args:
        url: The URL.

    Returns:
        The path component of the URL.
    """
    parsed_url = urlparse(url)

    return parsed_url.path


def isDescendant(base: str | None, path: str) -> bool:
    """Tell whether a path is a base path or below it.

    `path` is normalised first (e.g., `..` segments are resolved), and the
    paths are compared by components, so that `/tmp2` is not below `/tmp`.
    `base` must be absolute: relative paths are never below it, and nothing
    is below a relative `base`.

    Args:
        base: The base path.
        path: The path to check.

    Returns:
        Whether `path` is `base` or below it.
    """
    if base is None or not (os.path.isabs(base) and os.path.isabs(path)):
        return False

    # 'commonpath' of 'base' alone normalises it the same way (e.g. a
    # leading '//', which 'normpath' keeps)
    return os.path.commonpath([base, os.path.normpath(path)]) == os.path.commonpath(
        [base]
    )


def validatePath(path: str) -> None:
    """Check that a path is on the shared volume.

    Args:
        path: The path, as in the URLs of tasks.

    Raises:
        InvalidHostPath: The path is not `HOST_BASE_PATH` or below it.
    """
    if not isDescendant(HOST_BASE_PATH, path):
        raise InvalidHostPath(
            f"'{path}' is not a descendant of 'HOST_BASE_PATH' ({HOST_BASE_PATH})"
        )


def containerPath(path: str) -> str:
    """Translate a path on the shared volume to the path in the filer.

    Args:
        path: The path, as in the URLs of tasks.

    Returns:
        The path below `CONTAINER_BASE_PATH`.

    Raises:
        InvalidHostPath: The path is not `HOST_BASE_PATH` or below it.
        ValueError: `CONTAINER_BASE_PATH` is not set.
    """
    validatePath(path)
    if CONTAINER_BASE_PATH is None:
        raise ValueError("'CONTAINER_BASE_PATH' is not set")

    relPath = relpath(path, HOST_BASE_PATH)

    return os.path.normpath(os.path.join(CONTAINER_BASE_PATH, relPath))
