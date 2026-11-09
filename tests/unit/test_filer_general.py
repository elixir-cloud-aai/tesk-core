"""Tests for 'filer.py' general purpose functionalities using 'pytest'."""

# Note: In tests such as 'test_process_file_with_scheme', only the outer
# function of each unit under testing is checked, since mocking a function
# apparently affects its output. Maybe there's a way to bypass that issue and
# test deeper down the call tree.

from pathlib import Path
import stat

import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import FileProtocolDisabled, copyDir, process_file


def test_process_file_no_scheme() -> None:
    """Test that a file without scheme and base paths is rejected.

    Without a scheme and without the 'HOST_BASE_PATH' and
    'CONTAINER_BASE_PATH' environment variables, 'process_file' raises
    the appropriate error.
    """
    filedata = {"url": "www.foo.bar"}

    with pytest.raises(FileProtocolDisabled):
        process_file("upload", filedata)


def test_process_file_with_scheme(mocker: MockerFixture) -> None:
    """Test that 'process_file' handles a file with a scheme.

    In this example, the scheme is 'http', filedata:type is 'FILE' and
    ttype is 'inputs'.
    """
    filedata = {
        "url": "http://www.foo.bar",
        "path": ".",
        "type": "FILE",
    }
    mock_new_Trans = mocker.patch("tesk_core.filer.newTransput")
    process_file("inputs", filedata)

    mock_new_Trans.assert_called_once_with("http", "www.foo.bar")


def test_process_file_from_content(tmp_path: Path) -> None:
    """Test that 'process_file' draws the contents from the content field."""
    filedata = {
        "path": str(tmp_path) + "/testfile",
        "content": "This is some test content",
    }
    process_file("inputs", filedata)

    assert (tmp_path / "testfile").read_text() == filedata["content"]


def test_copyDir_merges_existing_directories(tmp_path: Path) -> None:
    """Test that a copy into existing nested directories merges them.

    Directories that exist keep their mode; new ones are copied whole.
    """
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "new.txt").write_text("new")
    (src / "top.txt").write_text("top")
    (src / "added").mkdir()
    (src / "added" / "b.txt").write_text("b")
    src.chmod(0o700)
    (src / "sub").chmod(0o750)
    dst = tmp_path / "dst"
    (dst / "sub").mkdir(parents=True)
    (dst / "sub" / "old.txt").write_text("old")
    (dst / "top.txt").write_text("replaced")
    dst.chmod(0o777)
    (dst / "sub").chmod(0o777)

    copyDir(str(src), str(dst))

    assert (dst / "sub" / "old.txt").read_text() == "old"
    assert (dst / "sub" / "new.txt").read_text() == "new"
    assert (dst / "top.txt").read_text() == "top"
    assert (dst / "added" / "b.txt").read_text() == "b"
    assert stat.S_IMODE(dst.stat().st_mode) == 0o777
    assert stat.S_IMODE((dst / "sub").stat().st_mode) == 0o777


def test_copyDir_replaces_links(tmp_path: Path) -> None:
    """Test that links in the destination are replaced, not written through."""
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "f.txt").write_text("f")
    (src / "x.txt").write_text("new")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "x.txt").write_text("old")
    dst = tmp_path / "dst"
    dst.mkdir()
    (dst / "x.txt").symlink_to(elsewhere / "x.txt")
    (dst / "sub").symlink_to(elsewhere)

    copyDir(str(src), str(dst))

    assert not (dst / "x.txt").is_symlink()
    assert (dst / "x.txt").read_text() == "new"
    assert not (dst / "sub").is_symlink()
    assert (dst / "sub" / "f.txt").read_text() == "f"
    assert sorted(p.name for p in elsewhere.iterdir()) == ["x.txt"]
    assert (elsewhere / "x.txt").read_text() == "old"


def test_copyDir_replaces_a_linked_destination(tmp_path: Path) -> None:
    """Test that a destination that is a link is replaced, not merged into."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "f.txt").write_text("f")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    dst = tmp_path / "dst"
    dst.symlink_to(elsewhere)

    copyDir(str(src), str(dst))

    assert not dst.is_symlink()
    assert (dst / "f.txt").read_text() == "f"
    assert list(elsewhere.iterdir()) == []


def test_copyDir_file_over_directory(tmp_path: Path) -> None:
    """Test that a file doesn't land inside a directory of its name."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a").write_text("file")
    dst = tmp_path / "dst"
    (dst / "a").mkdir(parents=True)

    with pytest.raises(IsADirectoryError):
        copyDir(str(src), str(dst))
    assert list((dst / "a").iterdir()) == []
