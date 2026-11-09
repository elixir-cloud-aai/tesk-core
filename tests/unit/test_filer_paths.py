"""Tests that the filer mounts and stages the task's paths below a fixed prefix."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tesk_core import path
from tesk_core.filer import process_file
from tesk_core.filer_class import Filer
from tesk_core.path import TASK_PATH_PREFIX, filerPath, isDescendant, taskPath
from tesk_core.pvc import PVC
from tesk_core.taskmaster import generate_mounts


@pytest.mark.parametrize(
    ("task_path", "expected"),
    [
        ("/data/in.txt", "/tesk/task/data/in.txt"),
        ("/data/", "/tesk/task/data"),
        ("/", "/tesk/task"),
        ("", ""),
        ("//data", "/tesk/task/data"),
        ("/data/../out", "/tesk/task/out"),
        ("/../../etc/passwd", "/tesk/task/etc/passwd"),
        ("relative/file", "/tesk/task/relative/file"),
        ("/data/out*", "/tesk/task/data/out*"),
    ],
)
def test_filer_path(task_path: str, expected: str) -> None:
    """Test that task paths are translated to paths below the prefix."""
    assert filerPath(task_path) == expected


# Task paths that are, contain or are inside the filer's own paths
OVERLAPPING_TASK_PATHS = [
    "/",
    "/usr/local/bin",
    "/usr/bin",
    "/opt",
    "/opt/home",
    "/aws",
    "/jsoninput",
    "/transfer",
    "/transfer/sub",
    "/var/run/secrets",
    "/tesk",
    "/../etc",
]


def task_volume_mounts(task_paths: list[str]) -> list[dict[str, Any]]:
    """Describe the claim's mounts of task paths, as the taskmaster does.

    Args:
        task_paths: The task paths.

    Returns:
        One mount per path, each on its own subpath.
    """
    return [
        {"name": "task-volume", "mountPath": task_path, "subPath": f"dir{index}"}
        for index, task_path in enumerate(task_paths)
    ]


@patch("tesk_core.path.HOST_BASE_PATH", "/host")
@patch("tesk_core.path.CONTAINER_BASE_PATH", "/transfer")
@patch("tesk_core.path.TRANSFER_PVC_NAME", "transfer-pvc")
class TestFilerMounts:
    """Tests of the filer pod's mounts, with the shared volume."""

    def filer(self, task_paths: list[str]) -> tuple[Filer, MagicMock]:
        """Create a filer with the task paths and its other mounts.

        Args:
            task_paths: The task paths.

        Returns:
            The filer, and the claim, whose mounts the executors use.
        """
        filer = Filer("name", {"a": 1}, json_pvc="task")
        pvc = MagicMock(spec=PVC, volume_mounts=task_volume_mounts(task_paths))
        pvc.name = "task-pvc"
        filer.add_volume_mount(pvc)
        filer.add_netrc_mount("netrc")
        return filer, pvc

    def test_task_paths_below_prefix(self) -> None:
        """Test that no task path overlays or contains another filer mount."""
        filer, _ = self.filer(OVERLAPPING_TASK_PATHS)
        mounts = filer.getVolumeMounts()
        task = [m["mountPath"] for m in mounts if m["name"] == "task-volume"]
        others = [m["mountPath"] for m in mounts if m["name"] != "task-volume"]
        assert len(task) == len(OVERLAPPING_TASK_PATHS)
        assert {"/aws", "/jsoninput", "/transfer", "/opt/home/.netrc"} <= set(others)
        for task_mount in task:
            assert isDescendant(TASK_PATH_PREFIX, task_mount)
            for other in others:
                assert not isDescendant(task_mount, other)
                assert not isDescendant(other, task_mount)

    def test_executor_mounts_unchanged(self) -> None:
        """Test that the claim's mounts keep the task's paths."""
        _, pvc = self.filer(["/data", "/opt"])
        assert pvc.volume_mounts == task_volume_mounts(["/data", "/opt"])

    def test_subpaths_kept(self) -> None:
        """Test that each task path keeps its subpath of the claim."""
        filer, _ = self.filer(["/data", "/data/in"])
        task = [m for m in filer.getVolumeMounts() if m["name"] == "task-volume"]
        assert task == [
            {"name": "task-volume", "mountPath": "/tesk/task/data", "subPath": "dir0"},
            {
                "name": "task-volume",
                "mountPath": "/tesk/task/data/in",
                "subPath": "dir1",
            },
        ]

    def test_no_service_account_token(self) -> None:
        """Test that the filer pod doesn't mount a service account token."""
        filer, _ = self.filer(["/data"])
        pod = filer.get_spec("outputs")["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False


@pytest.mark.parametrize(
    ("task_path", "expected"),
    [
        ("/out/", "/out"),
        ("/out", "/out"),
        ("//out//sub/./", "/out/sub"),
        ("/data/../x", "/x"),
        ("/..", "/"),
        ("", ""),
    ],
)
def test_task_path(task_path: str, expected: str) -> None:
    """Test that task paths are normalized, and an empty one is kept."""
    assert taskPath(task_path) == expected


@patch("tesk_core.taskmaster.logger")
def test_spellings_of_a_path_share_a_mount(_logger: MagicMock) -> None:
    """Test that spellings of one directory get one mount, in the filer too."""
    pvc = MagicMock(spec=PVC)
    pvc.get_subpath.side_effect = [f"dir{index}" for index in range(10)]
    pvc.name = "task-pvc"
    data = {
        "volumes": ["/data/../x"],
        "inputs": [{"type": "FILE", "path": "/x/in"}],
        "outputs": [
            {"type": "DIRECTORY", "path": "/out/"},
            {"type": "FILE", "path": "/out/x.txt"},
        ],
    }
    pvc.volume_mounts = generate_mounts(data, pvc)
    assert pvc.volume_mounts == [
        {"name": "task-volume", "mountPath": "/x", "subPath": "dir0"},
        {"name": "task-volume", "mountPath": "/out", "subPath": "dir1"},
    ]
    filer = Filer("name", {"a": 1})
    filer.add_volume_mount(pvc)
    paths = [m["mountPath"] for m in filer.getVolumeMounts()]
    assert len(paths) == len(set(paths))


def test_process_file_passes_filer_path() -> None:
    """Test that transfers of URLs get the path in the filer."""
    with patch("tesk_core.filer.newTransput") as new_transput:
        process_file(
            "inputs",
            {"path": "/data/in.txt", "url": "https://example.org/in", "type": "FILE"},
        )
    transput = new_transput.return_value
    assert transput.call_args.args[0] == "/tesk/task/data/in.txt"


@pytest.mark.parametrize("base", ["/transfer", "/tesk/task2", "/tesk/other", None])
def test_shared_volume_outside_prefix(base: str | None) -> None:
    """Test that a shared volume beside the prefix, or none, is accepted."""
    with patch.object(path, "CONTAINER_BASE_PATH", base):
        path.checkBasePaths()


@pytest.mark.parametrize("base", ["/", "/tesk", "/tesk/task", "/tesk/task/shared"])
def test_shared_volume_overlapping_prefix(base: str) -> None:
    """Test that a shared volume inside or around the prefix is refused."""
    with (
        patch.object(path, "HOST_BASE_PATH", "/host"),
        patch.object(path, "CONTAINER_BASE_PATH", base),
        pytest.raises(ValueError, match="CONTAINER_BASE_PATH"),
    ):
        Filer("name", {"a": 1})


def test_process_file_stages_below_prefix(tmp_path: Path) -> None:
    """Test that the filer writes inputs below the prefix, also with `..`."""
    (tmp_path / "data").mkdir()
    with patch.object(path, "TASK_PATH_PREFIX", str(tmp_path)):
        assert process_file("inputs", {"path": "/data/in.txt", "content": "a"}) == 0
        assert process_file("inputs", {"path": "/../../top.txt", "content": "b"}) == 0
    assert (tmp_path / "data" / "in.txt").read_text() == "a"
    assert (tmp_path / "top.txt").read_text() == "b"
