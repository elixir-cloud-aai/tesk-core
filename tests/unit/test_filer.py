"""Tests for the transfers of the filer and its helpers."""

from collections.abc import Iterator
from io import StringIO
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import fsspec  # type: ignore[import-untyped]  # fsspec ships no type information
import pytest

from tesk_core.exception import FileProtocolDisabled, InvalidHostPath, UnknownProtocol
from tesk_core.filer import (
    FileTransput,
    FTPTransput,
    HTTPTransput,
    copyDir,
    ftp_check_directory,
    getPath,
    logConfig,
    newTransput,
    process_file,
    subfolders_in,
)
from tesk_core.filer_s3 import S3Transput
from tesk_core.path import containerPath

RESOURCES = Path(__file__).parent / "resources"


def getTree(rootDir: str) -> str:
    """List the directories and files below a root directory.

    Args:
        rootDir: The root directory.

    Returns:
        One line per root, directory and file.
    """
    fs, base_path = fsspec.core.url_to_fs(rootDir)
    out = StringIO()

    for root, dirs, files in fs.walk(base_path):
        out.write(f"{root or base_path}\n")
        for d in dirs:
            out.write(f"{d}/\n")
        for f in files:
            out.write(f"{f}\n")

    return out.getvalue()


def normalize_tree(tree_str: str, abs_root: str, prefix: str) -> str:
    """Convert absolute paths from getTree into relative paths.

    Args:
        tree_str: The tree as returned by `getTree`.
        abs_root: The absolute root directory to replace.
        prefix: The prefix to put in its place.

    Returns:
        The tree with relative paths.
    """
    lines = []
    for line in tree_str.splitlines():
        stripped = prefix + line[len(abs_root) :] if line.startswith(abs_root) else line
        stripped = stripped.lstrip("/")
        lines.append(stripped)
    return "\n".join(lines)


@patch("tesk_core.path.HOST_BASE_PATH", "/home/tfga/workspace/cwl-tes")
@patch("tesk_core.path.CONTAINER_BASE_PATH", "/transfer")
class TestFiler:
    """Tests with the base paths set."""

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def log_config(cls) -> Iterator[None]:
        """Configure the logging of the filer for the tests of the class.

        Yields:
            Nothing, while the tests run.
        """
        logConfig(logging.DEBUG)  # Doesn't work...
        yield

    @patch("tesk_core.filer.copyDir")
    @patch("tesk_core.filer.shutil.copy")
    def test_download_file(self, copyMock: MagicMock, copyDirMock: MagicMock) -> None:
        """Test that a `file:` input is copied as a file."""
        filedata = {
            "url": "file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/md5",
            "path": "/var/lib/cwl/stgda974802-fa81-4f0b-8fe4-341d5655af4b/md5",
            "type": "FILE",  # File      = 'FILE'
            # Directory = 'DIRECTORY'
            "name": "md5",
            "description": "cwl_input:md5",
        }

        process_file("inputs", filedata)

        copyDirMock.assert_not_called()

        copyMock.assert_called_once_with(
            "/transfer/tmphrtip1o8/md5",
            "/tesk/task/var/lib/cwl/stgda974802-fa81-4f0b-8fe4-341d5655af4b/md5",
        )

    @patch("tesk_core.filer.copyDir")
    @patch("tesk_core.filer.shutil.copy")
    def test_download_dir(self, copyMock: MagicMock, copyDirMock: MagicMock) -> None:
        """Test that a `file:` input is copied as a directory."""
        filedata = {
            "url": "file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/",
            "path": "/TclSZU",
            "type": "DIRECTORY",
            "name": "workdir",
        }

        process_file("inputs", filedata)

        copyMock.assert_not_called()

        copyDirMock.assert_called_once_with(
            "/transfer/tmphrtip1o8", "/tesk/task/TclSZU"
        )

    @patch("tesk_core.filer.copyDir")
    @patch("tesk_core.filer.shutil.copy")
    def test_upload_dir(self, copyMock: MagicMock, copyDirMock: MagicMock) -> None:
        """Test that a `file:` output is copied as a directory."""
        filedata = {
            "url": "file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/",
            "path": "/TclSZU",
            "type": "DIRECTORY",
            "name": "workdir",
        }

        process_file("outputs", filedata)

        copyMock.assert_not_called()

        copyDirMock.assert_called_once_with(
            "/tesk/task/TclSZU", "/transfer/tmphrtip1o8"
        )

    @patch("tesk_core.filer.copyDir")
    @patch("tesk_core.filer.copyFile")
    def test_upload_file(self, copyFileMock: MagicMock, copyDirMock: MagicMock) -> None:
        """Test that a `file:` output is copied as a file."""
        filedata = {
            "url": "file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/md5",
            "path": "/TclSZU/md5",
            "type": "FILE",
            "name": "stdout",
        }

        process_file("outputs", filedata)

        copyDirMock.assert_not_called()

        copyFileMock.assert_called_once_with(
            "/tesk/task/TclSZU/md5", "/transfer/tmphrtip1o8/md5"
        )

    @patch("tesk_core.filer.copyDir")
    @patch("tesk_core.filer.copyFile")
    def test_upload_file_glob(
        self, copyFileMock: MagicMock, copyDirMock: MagicMock
    ) -> None:
        """Test that a `file:` output with a glob is copied as a file."""
        filedata = {
            "url": "file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/md5*",
            "path": "/TclSZU/md5*",
            "type": "FILE",
            "name": "stdout",
        }

        process_file("outputs", filedata)

        copyDirMock.assert_not_called()

        copyFileMock.assert_called_once_with(
            "/tesk/task/TclSZU/md5*", "/transfer/tmphrtip1o8/md5*"
        )

    def test_copyDir(self, tmp_path: Path) -> None:
        """Test copying a directory to an existing and a new destination."""
        src = str(RESOURCES / "copyDirTest/src")
        dst1 = os.path.join(tmp_path, "dst1")
        dst2 = os.path.join(tmp_path, "dst2")

        assert os.path.exists(src)  # src should exist
        assert not os.path.exists(dst1)  # dst1 shouldn't
        assert not os.path.exists(dst2)  # dst2 shouldn't

        # Copying to existing dst ---------------------------------------------
        # Let's create dst1
        os.mkdir(dst1)
        assert os.path.exists(dst1)  # Now dst1 should exist

        # Let's try to copy
        copyDir(src, dst1)
        tree = getTree(dst1)
        abs_dst1 = os.path.abspath(dst1)
        normalizedTree = normalize_tree(tree, abs_dst1, "dist1")

        expected = "dist1\na/\n3.txt\ndist1/a\n2.txt\n1.txt".strip()

        normalized_lines = sorted(normalizedTree.splitlines())
        expected_lines = sorted(expected.splitlines())

        assert normalized_lines == expected_lines

        # Copying to non-existing dst -----------------------------------------
        # # Let's try to copy
        copyDir(src, dst2)
        tree = getTree(dst2)
        abs_dst2 = os.path.abspath(dst2)
        normalizedTree = normalize_tree(tree, abs_dst2, "dist2")

        expected = "dist2\na/\n3.txt\ndist2/a\n2.txt\n1.txt".strip()

        normalized_lines = sorted(normalizedTree.splitlines())
        expected_lines = sorted(expected.splitlines())

        assert normalized_lines == expected_lines

    def test_getPath(self) -> None:
        """Test that the path is taken from a `file:` URL."""
        assert (
            getPath("file:///home/tfga/workspace/cwl-tes/tmphrtip1o8/md5")
            == "/home/tfga/workspace/cwl-tes/tmphrtip1o8/md5"
        )

    def test_getPathNoScheme(self) -> None:
        """Test that a path without scheme is kept and mapped to the container."""
        assert (
            getPath("/home/tfga/workspace/cwl-tes/tmphrtip1o8/md5")
            == "/home/tfga/workspace/cwl-tes/tmphrtip1o8/md5"
        )

        assert (
            containerPath("/home/tfga/workspace/cwl-tes/tmphrtip1o8/md5")
            == "/transfer/tmphrtip1o8/md5"
        )

    def test_containerPath(self) -> None:
        """Test the container path, and that paths outside the base fail."""
        assert (
            containerPath("/home/tfga/workspace/cwl-tes/tmphrtip1o8/md5")
            == "/transfer/tmphrtip1o8/md5"
        )

        # What happens if 'path' is not a descendant of HOST_BASE_PATH?
        with pytest.raises(InvalidHostPath) as excinfo:
            containerPath("/someOtherFolder")
        assert str(excinfo.value) == (
            "'/someOtherFolder' is not a descendant of "
            "'HOST_BASE_PATH' (/home/tfga/workspace/cwl-tes)"
        )

    def test_newTransput(self) -> None:
        """Test that the transput class is chosen by the protocol."""
        assert newTransput("ftp", "test.com") == FTPTransput
        assert newTransput("http", "test.com") == HTTPTransput
        assert newTransput("https", "test.com") == HTTPTransput
        assert newTransput("file", "/home/tfga/workspace/") == FileTransput
        assert newTransput("s3", "/home/tfga/workspace/") == S3Transput
        assert newTransput("http", "s3.aws.com") == HTTPTransput
        # Only `s3://` URLs go over S3; host names that contain "s3", e.g.
        # virtual-hosted or path-style S3 endpoints, stay HTTP (issue #45)
        assert newTransput("https", "mys3data.s3.amazonaws.com") == HTTPTransput
        assert newTransput("https", "s3.eu-west-1.amazonaws.com") == HTTPTransput
        assert newTransput("https", "s3bucket.example.org") == HTTPTransput
        assert newTransput("s3", "mys3data") == S3Transput

        with pytest.raises(UnknownProtocol) as excinfo:
            newTransput("svn", "example.com")
        assert str(excinfo.value) == "Unknown protocol: 'svn'"

    @patch("ftplib.FTP")
    def test_ftp_check_directory(self, conn: MagicMock) -> None:
        """Test that an existing directory returns 0."""
        path = os.path.curdir
        assert ftp_check_directory(conn, path) == 0

    def test_subfolders_in(self) -> None:
        """Test that all the subfolders of a path are properly returned."""
        path = "/this/is/a/path"
        subfldrs = ["/this", "/this/is", "/this/is/a", "/this/is/a/path"]
        assert subfolders_in(path) == subfldrs


@patch("tesk_core.path.HOST_BASE_PATH", None)
@patch("tesk_core.path.CONTAINER_BASE_PATH", None)
class TestFilerNoEnv:
    """Tests without the base paths, whatever the environment of the run."""

    def test_newTransput_file_disabled(self) -> None:
        """Test that the `file:` protocol is disabled without base paths."""
        with pytest.raises(FileProtocolDisabled) as excinfo:
            newTransput("file", "/home/user/test")
        assert str(excinfo.value) == (
            "'file:' protocol disabled\n"
            "To enable it, both 'HOST_BASE_PATH' and 'CONTAINER_BASE_PATH' "
            "environment variables must be defined."
        )
