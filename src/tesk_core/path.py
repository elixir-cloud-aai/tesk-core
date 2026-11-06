"""Paths of the shared volume for `file:` URLs.

The shared volume is mounted at `CONTAINER_BASE_PATH` in the filer, and is
the path `HOST_BASE_PATH` for the URLs of tasks.
"""

import os
from os.path import relpath
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
    """
    validatePath(path)

    relPath = relpath(path, HOST_BASE_PATH)

    return os.path.normpath(os.path.join(CONTAINER_BASE_PATH or "", relPath))
