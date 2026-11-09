"""Tests that outputs that are or contain symbolic links aren't uploaded."""

from collections.abc import Iterator
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws
import pytest

from tesk_core.filer import FileTransput, FTPTransput, HTTPTransput, process_file
from tesk_core.filer_class import Filer
from tesk_core.filer_s3 import S3Transput
from tesk_core.pvc import PVC
from tesk_core.transput import Type, open_for_upload, symlink_in

OUTSIDE = b"a file outside the work directory\n"


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Create a work directory and a file outside it.

    The work directory has a regular file, a link to the outside file, a link to
    the outside file's directory, and a directory with a link inside.

    Args:
        tmp_path: The temporary directory of the test.

    Returns:
        The work directory, without links in its own path.
    """
    root = Path(os.path.realpath(tmp_path))
    outside = root / "outside"
    outside.mkdir()
    (outside / "target").write_bytes(OUTSIDE)
    work = root / "work"
    work.mkdir()
    (work / "regular.txt").write_text("data\n")
    (work / "file_link").symlink_to(outside / "target")
    (work / "dir_link").symlink_to(outside)
    (work / "nested" / "deeper").mkdir(parents=True)
    (work / "nested" / "ok.txt").write_text("ok\n")
    (work / "nested" / "deeper" / "inner_link").symlink_to(outside / "target")
    (work / "clean").mkdir()
    (work / "clean" / "a.txt").write_text("a\n")
    return work


class TestSymlinkIn:
    """Tests of finding links in paths."""

    def test_regular_file(self, tree: Path) -> None:
        """Test that a regular file has no link."""
        assert symlink_in(str(tree / "regular.txt")) is None

    def test_missing_path(self, tree: Path) -> None:
        """Test that a path that doesn't exist has no link."""
        assert symlink_in(str(tree / "missing" / "file")) is None
        assert symlink_in(str(tree / "missing"), recursive=True) is None

    def test_link(self, tree: Path) -> None:
        """Test that a link is found."""
        assert symlink_in(str(tree / "file_link")) == str(tree / "file_link")

    def test_link_in_parent(self, tree: Path) -> None:
        """Test that a link to a directory in the path is found."""
        path = tree / "dir_link" / "target"
        assert symlink_in(str(path)) == str(tree / "dir_link")

    def test_dot_dot_refused(self, tree: Path) -> None:
        """Test that paths with `..` components are refused.

        The system resolves `..` after the link before it: `dir_link/..` is
        the parent of the link's target, not `tree`.
        """
        with pytest.raises(ValueError, match=r"\.\."):
            symlink_in(f"{tree}/dir_link/../outside/target")
        with pytest.raises(ValueError, match=r"\.\."):
            symlink_in(f"{tree}/clean/../regular.txt")

    def test_link_below_directory(self, tree: Path) -> None:
        """Test that a link below a directory is found only if recursive."""
        nested = tree / "nested"
        assert symlink_in(str(nested)) is None
        assert symlink_in(str(nested), recursive=True) == str(
            nested / "deeper" / "inner_link"
        )

    def test_clean_directory(self, tree: Path) -> None:
        """Test that a directory without links has none."""
        assert symlink_in(str(tree / "clean"), recursive=True) is None


def test_open_for_upload_refuses_link(tree: Path) -> None:
    """Test that opening a link for an upload fails."""
    with pytest.raises(OSError):
        open_for_upload(str(tree / "file_link"))
    with open_for_upload(str(tree / "regular.txt")) as file:
        assert file.read() == b"data\n"


# Each resolves to the outside file, or a directory with it, through a link
LINKED_OUTPUTS = [
    pytest.param("file_link", Type.File, id="file-link"),
    pytest.param("dir_link/../outside/target", Type.File, id="dot-dot-after-link"),
    pytest.param("dir_link/target", Type.File, id="file-in-dir-link"),
    pytest.param("dir_link", Type.Directory, id="dir-link"),
    pytest.param("nested", Type.Directory, id="link-below-dir"),
]


class TestHTTP:
    """Tests of HTTP outputs."""

    @pytest.mark.parametrize(("name", "ftype"), LINKED_OUTPUTS)
    def test_linked_output_not_sent(
        self, tree: Path, name: str, ftype: Type, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that a linked output fails without a request."""
        with patch("tesk_core.filer.requests.put") as put:
            transfer = HTTPTransput(str(tree / name), "https://example.org/out", ftype)
            assert transfer.upload() == 1
        put.assert_not_called()
        assert "outputs can't" in caplog.text

    def test_open_refuses_link_without_check(self, tree: Path) -> None:
        """Test that the upload doesn't follow a link that the check missed."""
        with (
            patch("tesk_core.transput.output_has_symlink", return_value=False),
            patch("tesk_core.filer.requests.put") as put,
        ):
            transfer = HTTPTransput(
                str(tree / "file_link"), "https://example.org/out", Type.File
            )
            assert transfer.upload() == 1
        put.assert_not_called()

    def test_link_to_proc(self, tmp_path: Path) -> None:
        """Test that a link to a file of the filer's process isn't sent."""
        link = Path(os.path.realpath(tmp_path)) / "environ"
        link.symlink_to("/proc/self/environ")
        with patch("tesk_core.filer.requests.put") as put:
            transfer = HTTPTransput(str(link), "https://example.org/out", Type.File)
            assert transfer.upload() == 1
        put.assert_not_called()

    def test_regular_outputs_sent(self, tree: Path) -> None:
        """Test that outputs without links are still uploaded."""
        with patch("tesk_core.filer.requests.put") as put:
            put.return_value = MagicMock(status_code=200)
            file = HTTPTransput(
                str(tree / "regular.txt"), "https://example.org/f", Type.File
            )
            assert file.upload() == 0
            directory = HTTPTransput(
                str(tree / "clean"), "https://example.org/d", Type.Directory
            )
            assert directory.upload() == 0
        assert put.call_count == 2


class TestFTP:
    """Tests of FTP outputs."""

    @pytest.mark.parametrize(("name", "ftype"), LINKED_OUTPUTS)
    def test_linked_output_not_sent(self, tree: Path, name: str, ftype: Type) -> None:
        """Test that a linked output fails without storing anything."""
        conn = MagicMock()
        transfer = FTPTransput(
            str(tree / name), "ftp://ftp.example.org/out", ftype, ftp_conn=conn
        )
        assert transfer.upload() == 1
        conn.storbinary.assert_not_called()

    def test_open_refuses_link_without_check(self, tree: Path) -> None:
        """Test that the upload doesn't follow a link that the check missed."""
        conn = MagicMock()
        transfer = FTPTransput(
            str(tree / "file_link"), "ftp://ftp.example.org/out", Type.File, conn
        )
        # The remote directory exists, and the remote path is no directory
        with (
            patch("tesk_core.transput.output_has_symlink", return_value=False),
            patch("tesk_core.filer.ftp_make_dirs", return_value=0),
            patch("tesk_core.filer.ftp_check_directory", return_value=1),
        ):
            assert transfer.upload() == 1
        conn.storbinary.assert_not_called()

    def test_unreadable_output_fails(self, tree: Path) -> None:
        """Test that an output that can't be opened fails, without a traceback."""
        conn = MagicMock()
        transfer = FTPTransput(
            str(tree / "missing.txt"), "ftp://ftp.example.org/out", Type.File, conn
        )
        with (
            patch("tesk_core.filer.ftp_make_dirs", return_value=0),
            patch("tesk_core.filer.ftp_check_directory", return_value=1),
        ):
            assert transfer.upload() == 1
        conn.storbinary.assert_not_called()


@pytest.fixture
def bucket() -> Iterator[None]:
    """Mock S3 with an empty bucket `tesk`.

    Yields:
        Nothing, while the mock is active.
    """
    with mock_aws():
        boto3.resource("s3", region_name="us-east-1").create_bucket(Bucket="tesk")
        yield


def bucket_keys() -> list[str]:
    """List the keys of the mocked bucket.

    Returns:
        The keys.
    """
    return [obj.key for obj in boto3.resource("s3").Bucket("tesk").objects.all()]


@pytest.mark.usefixtures("bucket")
class TestS3:
    """Tests of S3 outputs."""

    @pytest.mark.parametrize(("name", "ftype"), LINKED_OUTPUTS)
    def test_linked_output_not_sent(self, tree: Path, name: str, ftype: Type) -> None:
        """Test that a linked output fails without uploading anything."""
        with S3Transput(str(tree / name), "s3://tesk/out", ftype) as transfer:
            assert transfer.upload() == 1
        assert bucket_keys() == []

    def test_open_refuses_link_without_check(self, tree: Path) -> None:
        """Test that the upload doesn't follow a link that the check missed."""
        with (
            patch("tesk_core.transput.output_has_symlink", return_value=False),
            S3Transput(str(tree / "file_link"), "s3://tesk/out", Type.File) as t,
        ):
            assert t.upload() == 1
        assert bucket_keys() == []

    def test_regular_output_sent(self, tree: Path) -> None:
        """Test that a directory without links is still uploaded."""
        with S3Transput(str(tree / "clean"), "s3://tesk/out", Type.Directory) as t:
            assert t.upload() == 0
        assert bucket_keys() == ["out/a.txt"]


class TestFile:
    """Tests of `file:` outputs to the shared volume."""

    @pytest.fixture
    def shared(self, tmp_path: Path) -> Iterator[Path]:
        """Set up the shared volume in a temporary directory.

        Args:
            tmp_path: The temporary directory of the test.

        Yields:
            The shared volume, while the base paths point to it.
        """
        volume = Path(os.path.realpath(tmp_path)) / "shared"
        volume.mkdir()
        with (
            patch("tesk_core.path.HOST_BASE_PATH", "/host"),
            patch("tesk_core.path.CONTAINER_BASE_PATH", str(volume)),
        ):
            yield volume

    @pytest.mark.parametrize(("name", "ftype"), LINKED_OUTPUTS)
    def test_linked_output_not_copied(
        self, tree: Path, shared: Path, name: str, ftype: Type
    ) -> None:
        """Test that a linked output fails without copying anything."""
        transfer = FileTransput(str(tree / name), "file:///host/out", ftype)
        assert transfer.upload() == 1
        assert list(shared.iterdir()) == []

    def test_glob_match_link_not_copied(self, tree: Path, shared: Path) -> None:
        """Test that a link that a glob matches fails the output."""
        transfer = FileTransput(str(tree / "file_*"), "file:///host/", Type.File)
        assert transfer.upload() == 1
        assert list(shared.iterdir()) == []

    # Task paths as in the filer (test_filer_paths.py tests the prefix)
    @patch("tesk_core.path.TASK_PATH_PREFIX", "/")
    def test_regular_outputs_copied(self, tree: Path, shared: Path) -> None:
        """Test that outputs without links are still copied."""
        data = {"path": str(tree / "regular.txt"), "type": "FILE"}
        assert process_file("outputs", {**data, "url": "file:///host/f.txt"}) == 0
        data = {"path": str(tree / "clean"), "type": "DIRECTORY"}
        assert process_file("outputs", {**data, "url": "file:///host/d"}) == 0
        assert (shared / "f.txt").read_text() == "data\n"
        assert (shared / "d" / "a.txt").read_text() == "a\n"


def test_outputs_filer_mounts_task_paths_read_only() -> None:
    """Test that only the outputs filer mounts the task's paths read-only."""
    filer = Filer("name", {"a": 1})
    pvc = MagicMock(
        spec=PVC, volume_mounts=[{"name": "task-volume", "mountPath": "/data"}]
    )
    pvc.name = "task-pvc"
    filer.add_volume_mount(pvc)

    def task_mounts(mode: str) -> list[dict[str, object]]:
        """Get the mounts of the task's paths in a mode.

        Args:
            mode: `inputs` or `outputs`.

        Returns:
            The mounts.
        """
        mounts = filer.get_spec(mode)["spec"]["template"]["spec"]["containers"][0][
            "volumeMounts"
        ]
        return [mount for mount in mounts if mount["name"] == "task-volume"]

    mount = {"name": "task-volume", "mountPath": "/tesk/task/data"}
    assert task_mounts("inputs") == [mount]
    assert task_mounts("outputs") == [{**mount, "readOnly": True}]
    # The claim's mounts, which the executors use, stay as they are and writable
    assert task_mounts("inputs") == [mount]
    assert pvc.volume_mounts == [{"name": "task-volume", "mountPath": "/data"}]
