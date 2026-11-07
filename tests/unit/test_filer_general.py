"""Tests for 'filer.py' general purpose functionalities using 'pytest'."""

# Note: In tests such as 'test_process_file_with_scheme' or
# 'test_copyContent_dir', only the outer function of each unit under testing is
# checked, since mocking a function apparently affects its output. Maybe
# there's a way to bypass that issue and test deeper down the call tree.

from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import FileProtocolDisabled, copyContent, process_file


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


def test_copyContent_dir(mocker: MockerFixture) -> None:
    """Test that 'os.listdir' is called when 'copyContent' is called."""
    mock_os_listdir = mocker.patch("os.listdir")
    copyContent(".", "/test_dst")

    mock_os_listdir.assert_called_once_with(".")
