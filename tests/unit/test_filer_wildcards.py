"""Tests of outputs with wildcards and `path_prefix` (TES 1.1)."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws
import pytest

from tesk_core.filer import process_file, url_join
from tesk_core.path import hasWildcards, wildcardRoot
from tesk_core.taskmaster import dirname, generate_mounts
from tesk_core.transput import Type


@pytest.fixture
def bucket() -> Iterator[None]:
    """Mock S3 with an empty bucket `tesk`.

    Yields:
        Nothing, while the mock is active.
    """
    with mock_aws():
        boto3.resource("s3", endpoint_url="http://s3.amazonaws.com").create_bucket(
            Bucket="tesk"
        )
        yield


def keys() -> list[str]:
    """List the keys in the bucket `tesk`.

    Returns:
        The keys, sorted.
    """
    s3 = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    return sorted(obj.key for obj in s3.Bucket("tesk").objects.all())


def body(key: str) -> bytes:
    """Read an object of the bucket `tesk`.

    Args:
        key: The key.

    Returns:
        The object's content.
    """
    s3 = boto3.resource("s3", endpoint_url="http://s3.amazonaws.com")
    return s3.Object("tesk", key).get()["Body"].read()


def write(path: Path, text: str = "x") -> None:
    """Write a file, creating its directory.

    Args:
        path: The file.
        text: Its content.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def output(path: str, prefix: str | None, url: str = "s3://tesk/out") -> dict[str, str]:
    """Describe a FILE output.

    Args:
        path: Its path, with wildcards.
        prefix: Its `path_prefix`, if any.
        url: Its URL.

    Returns:
        The output, as the filer gets it.
    """
    data = {"path": path, "url": url, "type": Type.File.value}
    if prefix is not None:
        data["path_prefix"] = prefix
    return data


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/*.txt", True),
        ("/data/?.txt", True),
        ("/data/out.tx[*t]", True),
        ("/data/out.txt", False),
        ("/data/", False),
    ],
)
def test_has_wildcards(path: str, expected: bool) -> None:
    """Test that `*`, `?` and `[` make a pattern."""
    assert hasWildcards(path) is expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/*.txt", "/data"),
        ("/data/*/out.txt", "/data"),
        ("/data/a/b/*", "/data/a/b"),
        ("//data/./a/../*.bam", "/data"),
        ("/data/out.tx[*t]", "/data"),
        ("/*.txt", ""),
    ],
)
def test_wildcard_root(path: str, expected: str) -> None:
    """Test that the root is the deepest directory without wildcards."""
    assert wildcardRoot(path) == expected


def test_dirname_of_pattern_is_its_root() -> None:
    """Test that an output with wildcards is mounted at its root.

    Before, `/data/*/out.txt` was mounted at `/data/*`, a directory that
    the executor doesn't write to.
    """
    assert dirname(output("/data/*/out.txt", "/data")) == "/data"
    assert dirname(output("/data/*.txt", "/data")) == "/data"
    assert dirname({"path": "/data/out", "type": "DIRECTORY"}) == "/data/out"
    assert dirname({"path": "/data/out.txt", "type": "FILE"}) == "/data"


def test_generate_mounts_shares_the_root() -> None:
    """Test that an input and a pattern in one directory share one mount."""
    pvc = MagicMock()
    pvc.get_subpath.side_effect = ["dir0", "dir1"]
    data = {
        "volumes": [],
        "inputs": [{"path": "/data/in.txt", "type": "FILE"}],
        "outputs": [output("/data/*/out.txt", "/data")],
    }
    assert generate_mounts(data, pvc) == [
        {"name": "task-volume", "mountPath": "/data", "subPath": "dir0"}
    ]


@pytest.mark.parametrize(
    "url, relative, expected",
    [
        ("s3://tesk/out", "a/b.txt", "s3://tesk/out/a/b.txt"),
        ("s3://tesk/out/", "b.txt", "s3://tesk/out/b.txt"),
        ("ftp://host/out", "a/b.txt", "ftp://host/out/a/b.txt"),
        (
            "https://host/out?token=t",
            "a b/c#1.txt",
            "https://host/out/a%20b/c%231.txt?token=t",
        ),
    ],
)
def test_url_join(url: str, relative: str, expected: str) -> None:
    """Test that HTTP(S) URLs get escaped components and keep their query."""
    assert url_join(url, relative) == expected


# Task paths as in the filer (test_filer_paths.py tests the prefix)
@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_matches_are_uploaded_below_the_url(tmp_path: Path) -> None:
    """Test that each match lands at the URL plus its path below the prefix."""
    write(tmp_path / "a.bam", "a")
    write(tmp_path / "b.bam", "b")
    write(tmp_path / "c.txt")
    write(tmp_path / "sub" / "d.bam", "d")

    assert process_file("outputs", output(f"{tmp_path}/*.bam", str(tmp_path))) == 0

    assert keys() == ["out/a.bam", "out/b.bam"]
    assert body("out/a.bam") == b"a"


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_pattern_with_directories(tmp_path: Path) -> None:
    """Test that wildcards in directories keep the directories in the URL."""
    write(tmp_path / "s1" / "out.txt", "1")
    write(tmp_path / "s2" / "out.txt", "2")
    write(tmp_path / "s3" / "other.txt")

    filedata = output(f"{tmp_path}/*/out.txt", f"{tmp_path}/")
    assert process_file("outputs", filedata) == 0

    assert keys() == ["out/s1/out.txt", "out/s2/out.txt"]
    assert body("out/s2/out.txt") == b"2"


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_matched_directory_is_uploaded_whole(tmp_path: Path) -> None:
    """Test that a directory that matches is uploaded with its contents."""
    write(tmp_path / "res_1" / "a.txt")
    write(tmp_path / "res_1" / "sub" / "b.txt")

    assert process_file("outputs", output(f"{tmp_path}/res_*", str(tmp_path))) == 0

    assert keys() == ["out/res_1/a.txt", "out/res_1/sub/b.txt"]


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_hidden_files_match_patterns_starting_with_a_dot(tmp_path: Path) -> None:
    """Test that `*` skips hidden files, and `.name*` matches them, as in POSIX."""
    write(tmp_path / ".command.sh")
    write(tmp_path / ".command.log")
    write(tmp_path / "visible.txt")

    assert process_file("outputs", output(f"{tmp_path}/.command.*", str(tmp_path))) == 0
    assert keys() == ["out/.command.log", "out/.command.sh"]


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_no_match_uploads_nothing_and_succeeds(tmp_path: Path) -> None:
    """Test that a pattern that matches nothing is no error."""
    write(tmp_path / "a.txt")

    assert process_file("outputs", output(f"{tmp_path}/*.bam", str(tmp_path))) == 0
    assert keys() == []


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_single_file_class_makes_an_optional_output(tmp_path: Path) -> None:
    """Test that a class matching one name uploads that file if it exists.

    Clients use it for outputs that a task may not create: `out.tx[*t]`
    matches only `out.txt` (and `out.tx*`).
    """
    filedata = output(f"{tmp_path}/out.tx[*t]", str(tmp_path), "s3://tesk/dir")
    assert process_file("outputs", filedata) == 0
    assert keys() == []

    write(tmp_path / "out.txt", "result")
    assert process_file("outputs", filedata) == 0
    assert keys() == ["dir/out.txt"]


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_match_outside_the_prefix_fails(tmp_path: Path) -> None:
    """Test that a prefix that isn't above the matches is refused."""
    write(tmp_path / "a" / "x.txt")

    filedata = output(f"{tmp_path}/a/*.txt", f"{tmp_path}/b")
    assert process_file("outputs", filedata) == 1
    assert keys() == []


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_symbolic_link_match_fails(tmp_path: Path) -> None:
    """Test that a match that is a symbolic link is refused, as other outputs."""
    write(tmp_path / "real.txt")
    (tmp_path / "link.txt").symlink_to(tmp_path / "real.txt")
    write(tmp_path / "a.txt")

    assert process_file("outputs", output(f"{tmp_path}/*.txt", str(tmp_path))) == 1
    # `a.txt` sorts before the link, and isn't uploaded either
    assert keys() == []


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_pattern_without_prefix_is_taken_as_before(tmp_path: Path) -> None:
    """Test that without `path_prefix` the path is uploaded as a plain file."""
    write(tmp_path / "a.txt")
    write(tmp_path / "out[1].txt", "literal")

    # TES 1.0 tasks: `[` and `*` are part of the name
    filedata = output(f"{tmp_path}/out[1].txt", None, "s3://tesk/out[1].txt")
    assert process_file("outputs", filedata) == 0
    assert keys() == ["out[1].txt"]
    assert body("out[1].txt") == b"literal"
