"""Tests of outputs with wildcards and `path_prefix` (TES 1.1)."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws
import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import process_file, url_join
from tesk_core.path import hasWildcards, wildcardRoot
from tesk_core.taskmaster import dirname, generate_mounts, output_mount
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


def test_output_with_wildcards_is_mounted_at_its_root() -> None:
    """Test that an output with wildcards and a prefix is mounted at its root.

    Before, `/data/*/out.txt` was mounted at `/data/*`, a directory that
    the executor doesn't write to.
    """
    assert output_mount(output("/data/*/out.txt", "/data")) == "/data"
    assert output_mount(output("/data/*.txt", "/data")) == "/data"
    dir_pattern = {**output("/data/res_*", "/data"), "type": "DIRECTORY"}
    assert output_mount(dir_pattern) == "/data"


def test_other_paths_are_mounted_as_before() -> None:
    """Test that TES 1.0 outputs and all inputs keep their mounts.

    `[`, `*` and `?` are only wildcards with `path_prefix`; otherwise they
    are part of a name.
    """
    assert output_mount(output("/data/[1]/out.txt", None)) == "/data/[1]"
    assert output_mount({"path": "/data/out", "type": "DIRECTORY"}) == "/data/out"
    assert dirname({"path": "/data/[1]/in.txt", "type": "FILE"}) == "/data/[1]"


def test_pattern_with_wildcards_in_its_first_component_is_refused() -> None:
    """Test that `/*.txt` is refused rather than mounted at an empty path."""
    with pytest.raises(ValueError, match="first component"):
        output_mount(output("/*.txt", "/"))


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


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
def test_invalid_url_fails_the_output(
    tmp_path: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a URL the transfer refuses fails the output, without connecting."""
    ftp = mocker.patch("tesk_core.filer.FTP")
    write(tmp_path / "a.txt")

    filedata = output(
        f"{tmp_path}/*.txt", str(tmp_path), "ftp://h.example.org:99999/out"
    )
    assert process_file("outputs", filedata) == 1
    ftp.return_value.connect.assert_not_called()
    assert "Invalid host or port" in caplog.text


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_failed_upload_fails_the_output(tmp_path: Path, mocker: MockerFixture) -> None:
    """Test that a match whose upload fails fails the output, and stops there."""
    upload = mocker.patch("tesk_core.filer.S3Transput.upload", return_value=1)
    write(tmp_path / "a.txt")
    write(tmp_path / "b.txt")

    assert process_file("outputs", output(f"{tmp_path}/*.txt", str(tmp_path))) == 1
    upload.assert_called_once_with(check_links=False)


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@pytest.mark.usefixtures("bucket")
def test_names_with_hash_or_question_mark_are_refused_for_s3(tmp_path: Path) -> None:
    """Test that a name an S3 URL can't carry fails the output, uploading nothing.

    S3 transfers take the URL's path as it is, where `#` and `?` would end
    the key.
    """
    write(tmp_path / "a.txt")
    write(tmp_path / "b#1.txt")

    assert process_file("outputs", output(f"{tmp_path}/*.txt", str(tmp_path))) == 1
    assert keys() == []


@pytest.fixture
def shared_volume(tmp_path: Path) -> Iterator[Path]:
    """Enable `file:` URLs, with the shared volume in `tmp_path/shared`.

    Yields:
        The shared volume's directory.
    """
    shared = tmp_path / "shared"
    shared.mkdir()
    with (
        patch("tesk_core.path.HOST_BASE_PATH", str(shared)),
        patch("tesk_core.path.CONTAINER_BASE_PATH", str(shared)),
    ):
        yield shared


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
def test_file_matches_are_copied_below_the_url(
    tmp_path: Path, shared_volume: Path
) -> None:
    """Test that `file:` matches land below the URL, also in new subdirectories.

    A match whose name has `[` is a file, not a pattern, and is copied too.
    """
    work = tmp_path / "work"
    write(work / "s1" / "out.txt", "1")
    write(work / "s2" / "out[1].txt", "2")
    write(work / "res_1" / "sub" / "c.txt", "3")

    url = f"file://{shared_volume}/run"
    assert process_file("outputs", output(f"{work}/*/out*.txt", str(work), url)) == 0
    assert (shared_volume / "run" / "s1" / "out.txt").read_text() == "1"
    assert (shared_volume / "run" / "s2" / "out[1].txt").read_text() == "2"

    filedata = {**output(f"{work}/res_*", str(work), url), "type": Type.Directory.value}
    assert process_file("outputs", filedata) == 0
    assert (shared_volume / "run" / "res_1" / "sub" / "c.txt").read_text() == "3"


@patch("tesk_core.path.TASK_PATH_PREFIX", "/")
@patch("tesk_core.path.HOST_BASE_PATH", None)
@patch("tesk_core.path.CONTAINER_BASE_PATH", None)
def test_no_match_succeeds_without_the_shared_volume(tmp_path: Path) -> None:
    """Test that a `file:` pattern matching nothing succeeds with `file:` disabled."""
    filedata = output(f"{tmp_path}/*.bam", str(tmp_path), "file:///shared/out")
    assert process_file("outputs", filedata) == 0
