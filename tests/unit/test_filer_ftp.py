"""Tests for 'filer.py' FTP functionalities using 'pytest'."""

from collections.abc import Iterator
import ftplib
import logging
import netrc
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from tesk_core.filer import (
    FTPTransput,
    Type,
    ftp_check_directory,
    ftp_download_file,
    ftp_host_allowed,
    ftp_login,
    ftp_make_dirs,
    ftp_upload_file,
    log_invalid_ftp_hosts,
    log_missing_ftp_hosts,
    netloc_host,
    process_file,
)

CREDENTIALS = {"TESK_FTP_USERNAME": "test", "TESK_FTP_PASSWORD": "test_pass"}


@pytest.fixture(autouse=True)
def _reset_missing_hosts_log() -> Iterator[None]:
    """Let each test log the error about missing `TESK_FTP_HOSTS` again.

    Yields:
        Nothing; resets before and after the test.
    """
    log_missing_ftp_hosts.cache_clear()
    log_invalid_ftp_hosts.cache_clear()
    yield
    log_missing_ftp_hosts.cache_clear()
    log_invalid_ftp_hosts.cache_clear()


@pytest.mark.parametrize(
    ("hosts", "netloc"),
    [
        ("ftp.example.org", "ftp.example.org"),
        ("FTP.Example.org", "ftp.example.ORG"),
        ("ftp.example.org", "ftp.example.org:21"),
        ("ftp.example.org:2121", "ftp.example.org:2121"),
        ("ftp.example.org:21", "ftp.example.org"),
        ("ftp.example.org", "user:secret@ftp.example.org"),
        ("other.example.org, ftp.example.org ,", "ftp.example.org"),
        ("[::1]", "[::1]"),
        ("[::1]:2121", "[::1]:2121"),
        ("192.0.2.1", "192.0.2.1"),
    ],
)
def test_ftp_host_allowed(hosts: str, netloc: str) -> None:
    """Test that listed hosts match, ignoring case, with any or their port.

    Args:
        hosts: The value of `TESK_FTP_HOSTS`.
        netloc: The network location of the URL.
    """
    assert ftp_host_allowed(netloc, hosts)


@pytest.mark.parametrize(
    ("hosts", "netloc"),
    [
        ("ftp.example.org", "other.example.org"),
        ("ftp.example.org", "ftp.example.org.attacker.example"),
        ("example.org", "ftp.example.org"),
        ("ftp.example.org:2121", "ftp.example.org"),
        ("ftp.example.org", "ftp.example.org:2121"),
        ("[::1]", "[::1]:2121"),
        ("ftp.example.org:2121", "ftp.example.org:2122"),
        ("ftp.example.org", "ftp.example.org@other.example.org"),
        ("ftp.example.org", "ftp.example.org:abc"),
        ("*.example.org", "ftp.example.org"),
        ("ftp.example.org:abc", "ftp.example.org"),
        ("user@ftp.example.org", "ftp.example.org"),
        ("ftp.example.org/path", "ftp.example.org"),
        ("", "ftp.example.org"),
        (" , ", "ftp.example.org"),
        ("ftp.example.org", ""),
        ("[::1", "[::1]"),
        ("host]", "host"),
        ("[ftp.example.org]", "ftp.example.org"),
        ("\uff03x", "x"),
        ("ftp://ftp.example.org", "ftp.example.org"),
        ("ftp.example.org", "[::1"),
        ("ftp example.org", "ftp example.org"),
        ("h\x00", "h\x00"),
        ("ftp.example.org:0", "ftp.example.org:0"),
        ("ftp.example.org:", "ftp.example.org:2121"),
        ("[::1]:", "[::1]:2121"),
        ("\tftp.example.org", "ftp.example.org"),
        ("ftp.example.org\n", "ftp.example.org"),
    ],
)
def test_ftp_host_not_allowed(hosts: str, netloc: str) -> None:
    """Test that other hosts, ports and invalid entries don't match.

    Args:
        hosts: The value of `TESK_FTP_HOSTS`.
        netloc: The network location of the URL.
    """
    assert not ftp_host_allowed(netloc, hosts)


def test_ftp_login_env_credentials_listed_host(mocker: MockerFixture) -> None:
    """Test that the env credentials are sent to a listed host.

    Args:
        mocker: The mocker fixture.
    """
    conn = mocker.MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", None)
    conn.login.assert_called_once_with("test", "test_pass")


def test_ftp_login_env_credentials_other_host(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the env credentials aren't sent to a host that isn't listed.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
    """
    conn = mocker.MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True), caplog.at_level("DEBUG"):
        ftp_login(conn, "user:pw@other.example.org:2121", None)
    conn.login.assert_called_once_with()
    assert caplog.messages == [
        "FTP host other.example.org:2121 is not in TESK_FTP_HOSTS; "
        "logging in anonymously"
    ]
    assert "pw@" not in caplog.text
    assert "test_pass" not in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_ftp_login_env_credentials_without_hosts(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the env credentials aren't sent without `TESK_FTP_HOSTS`.

    The filer logs in anonymously, and logs one error per process.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
    """
    conn = mocker.MagicMock()
    with mock.patch.dict("os.environ", CREDENTIALS, clear=True):
        ftp_login(conn, "ftp.example.org", None)
        ftp_login(conn, "other.example.org", None)
    assert conn.login.call_args_list == [mock.call(), mock.call()]
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "TESK_FTP_HOSTS" in errors[0].getMessage()
    assert ".netrc" in errors[0].getMessage()
    assert "test_pass" not in caplog.text


@pytest.mark.parametrize("hosts", ["", " ", ",", " , ,"])
def test_ftp_login_env_credentials_empty_hosts(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture, hosts: str
) -> None:
    """Test that a `TESK_FTP_HOSTS` without entries counts as missing.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
        hosts: The value of `TESK_FTP_HOSTS`.
    """
    conn = mocker.MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": hosts}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", None)
    conn.login.assert_called_once_with()
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "lists no hosts" in errors[0].getMessage()


def test_ftp_login_invalid_hosts(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that invalid entries are logged once, without their text.

    Valid entries still match.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
    """
    conn = mocker.MagicMock()
    hosts = "ftp://u:secret@h, [::1, a b, ftp.example.org"
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": hosts}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", None)
        ftp_login(conn, "ftp.example.org", None)
    assert conn.login.call_args_list == [mock.call("test", "test_pass")] * 2
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "3 invalid entries" in errors[0].getMessage()
    assert "secret" not in caplog.text


def test_ftp_login_without_credentials(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the filer logs in anonymously, without errors, without creds.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
    """
    conn = mocker.MagicMock()
    with mock.patch.dict("os.environ", {}, clear=True):
        ftp_login(conn, "ftp.example.org", None)
    conn.login.assert_called_once_with()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.fixture
def netrc_file(tmp_path: Path) -> netrc.netrc:
    """Read a `.netrc` file with credentials for `FTP.example.org` only.

    Args:
        tmp_path: The temporary directory of the test.

    Returns:
        The parsed file.
    """
    path = tmp_path / ".netrc"
    path.write_text("machine FTP.example.org login netrc_user password netrc_pass\n")
    path.chmod(0o600)
    return netrc.netrc(str(path))


@pytest.mark.parametrize(
    "netloc", ["FTP.example.org", "FTP.example.org:21", "user@FTP.example.org"]
)
def test_ftp_login_netrc_entry(netrc_file: netrc.netrc, netloc: str) -> None:
    """Test that the `.netrc` entry for the host wins over the environment.

    The port and user information of the URL, if any, aren't part of the
    host.

    Args:
        netrc_file: The `.netrc` file.
        netloc: The network location of the URL.
    """
    conn = MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, netloc, netrc_file)
    conn.login.assert_called_once_with("netrc_user", "netrc_pass")


def test_ftp_login_netrc_without_hosts(
    netrc_file: netrc.netrc, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a `.netrc` entry is used, without errors, without hosts.

    Args:
        netrc_file: The `.netrc` file.
        caplog: The log capture fixture.
    """
    conn = MagicMock()
    with mock.patch.dict("os.environ", CREDENTIALS, clear=True):
        ftp_login(conn, "FTP.example.org", netrc_file)
    conn.login.assert_called_once_with("netrc_user", "netrc_pass")
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize(
    ("netloc", "host"),
    [
        ("FTP.example.org", "FTP.example.org"),
        ("u:p@FTP.example.org:2121", "FTP.example.org"),
        ("[::1]", "::1"),
        ("u@[2001:db8::1]:2121", "2001:db8::1"),
    ],
)
def test_netloc_host(netloc: str, host: str) -> None:
    """Test that the host keeps its case, without user, port and brackets.

    Args:
        netloc: The network location of the URL.
        host: The expected host.
    """
    assert netloc_host(netloc) == host


def test_ftp_login_netrc_case(netrc_file: netrc.netrc) -> None:
    """Test that `.netrc` entries match hosts as written, including case.

    The allowlist ignores case, so a listed host gets the env credentials.

    Args:
        netrc_file: The `.netrc` file, with an entry for `FTP.example.org`.
    """
    conn = MagicMock()
    with mock.patch.dict("os.environ", CREDENTIALS, clear=True):
        ftp_login(conn, "ftp.example.org", netrc_file)
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "FTP.EXAMPLE.ORG"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", netrc_file)
    assert conn.login.call_args_list == [mock.call(), mock.call("test", "test_pass")]


def test_ftp_login_netrc_default_wins(tmp_path: Path) -> None:
    """Test that the `default` entry wins over the env credentials.

    Also for a host in `TESK_FTP_HOSTS`.

    Args:
        tmp_path: The temporary directory of the test.
    """
    path = tmp_path / ".netrc"
    path.write_text("default login default_user password default_pass\n")
    path.chmod(0o600)
    conn = MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", netrc.netrc(str(path)))
    conn.login.assert_called_once_with("default_user", "default_pass")


def test_ftp_login_netrc_other_port(netrc_file: netrc.netrc) -> None:
    """Test that an entry for the host isn't used for other ports.

    Args:
        netrc_file: The `.netrc` file, with an entry for `FTP.example.org`.
    """
    conn = MagicMock()
    with mock.patch.dict("os.environ", {}, clear=True):
        ftp_login(conn, "FTP.example.org:2121", netrc_file)
    conn.login.assert_called_once_with()


@pytest.mark.parametrize(
    "netloc", ["FTP.example.org:[2001:db8::1]", "FTP.example.org:21[v1.x]"]
)
def test_ftp_login_unparsable_netloc(
    netrc_file: netrc.netrc, caplog: pytest.LogCaptureFixture, netloc: str
) -> None:
    """Test that no credentials are sent for a host that can't be parsed.

    The `.netrc` lookup and the connection must agree on the host; Python
    versions without strict checks of brackets read these differently.

    Args:
        netrc_file: The `.netrc` file, with an entry for `FTP.example.org`.
        caplog: The log capture fixture.
        netloc: A network location with brackets in the port.
    """
    conn = MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, netloc, netrc_file)
    conn.login.assert_called_once_with()
    assert "secret" not in caplog.text


@pytest.mark.parametrize(
    ("url", "host", "port"),
    [
        ("ftp://h.example.org/x", "h.example.org", 0),
        ("ftp://h.example.org:2121/x", "h.example.org", 2121),
        ("ftp://u:p@h.example.org:2121/x", "h.example.org", 2121),
        ("ftp://[::1]:2121/x", "::1", 2121),
        ("ftp://H.Example.org/x", "h.example.org", 0),
    ],
)
def test_ftp_connect_host_port(
    mocker: MockerFixture, url: str, host: str, port: int
) -> None:
    """Test that the filer connects to the URL's host and port.

    Without user information; port 0 makes `ftplib` use its default port.

    Args:
        mocker: The mocker fixture.
        url: The URL.
        host: The expected host.
        port: The expected port.
    """
    conn = mocker.MagicMock()
    mocker.patch("tesk_core.filer.FTP", return_value=conn)
    mocker.patch("tesk_core.filer.ftp_login")
    with FTPTransput("/tmp/x", url, Type.File):
        pass
    conn.connect.assert_called_once_with(host, port)


@pytest.mark.parametrize(
    "url", ["ftp://h.example.org:99999/x", "ftp://h.example.org:abc/x", "ftp:///x"]
)
def test_process_file_invalid_ftp_url(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture, url: str
) -> None:
    """Test that an FTP URL without a valid host or port fails the transfer.

    With a logged error, without connecting.

    Args:
        mocker: The mocker fixture.
        caplog: The log capture fixture.
        url: The URL.
    """
    ftp = mocker.patch("tesk_core.filer.FTP")
    filedata = {"path": "/tmp/x", "url": url, "type": "FILE"}
    assert process_file("inputs", filedata) == 1
    ftp.return_value.connect.assert_not_called()
    assert "Invalid host or port" in caplog.text


def test_ftp_login_netrc_entry_with_port(tmp_path: Path) -> None:
    """Test that an entry for the host and port wins over the host's.

    The `default` entry is used only without an entry for either.

    Args:
        tmp_path: The temporary directory of the test.
    """
    path = tmp_path / ".netrc"
    path.write_text(
        "machine ftp.example.org:2121 login port_user password port_pass\n"
        "machine ftp.example.org login host_user password host_pass\n"
        "default login default_user password default_pass\n"
    )
    path.chmod(0o600)
    netrc_file = netrc.netrc(str(path))
    conn = MagicMock()
    with mock.patch.dict("os.environ", {}, clear=True):
        ftp_login(conn, "user@ftp.example.org:2121", netrc_file)
        ftp_login(conn, "ftp.example.org", netrc_file)
        ftp_login(conn, "ftp.example.org:21", netrc_file)
        ftp_login(conn, "ftp.example.org:2122", netrc_file)
        ftp_login(conn, "other.example.org", netrc_file)
    assert conn.login.call_args_list == [
        mock.call("port_user", "port_pass"),
        mock.call("host_user", "host_pass"),
        mock.call("host_user", "host_pass"),
        mock.call("default_user", "default_pass"),
        mock.call("default_user", "default_pass"),
    ]


def test_ftp_login_netrc_without_entry_listed_host(
    netrc_file: netrc.netrc,
) -> None:
    """Test that the env credentials are used for a listed host.

    The `.netrc` file has no entry for the host.

    Args:
        netrc_file: The `.netrc` file.
    """
    conn = MagicMock()
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "other.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "other.example.org", netrc_file)
    conn.login.assert_called_once_with("test", "test_pass")


@pytest.mark.parametrize("hosts", [None, "", "ftp.example.org"])
def test_ftp_login_netrc_without_entry_other_host(
    netrc_file: netrc.netrc, hosts: str | None
) -> None:
    """Test that the env credentials aren't used for other hosts.

    Neither without `TESK_FTP_HOSTS` nor for a host that isn't listed,
    when the `.netrc` file has no entry for the host.

    Args:
        netrc_file: The `.netrc` file.
        hosts: The value of `TESK_FTP_HOSTS`, or `None` if it is unset.
    """
    conn = MagicMock()
    env = dict(CREDENTIALS)
    if hosts is not None:
        env["TESK_FTP_HOSTS"] = hosts
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "other.example.org", netrc_file)
    conn.login.assert_called_once_with()


@pytest.mark.parametrize("hosts", [None, "other.example.org"])
def test_ftp_login_empty_username(
    netrc_file: netrc.netrc, caplog: pytest.LogCaptureFixture, hosts: str | None
) -> None:
    """Test that an empty `TESK_FTP_USERNAME` counts as no credentials.

    TESK's chart creates the FTP secret by default, with an empty user name
    unless one is set; that is no reason for the error about
    `TESK_FTP_HOSTS`.

    Args:
        netrc_file: The `.netrc` file.
        caplog: The log capture fixture.
        hosts: The value of `TESK_FTP_HOSTS`, or `None` if it is unset.
    """
    conn = MagicMock()
    env = {"TESK_FTP_USERNAME": "", "TESK_FTP_PASSWORD": "secret"}
    if hosts is not None:
        env["TESK_FTP_HOSTS"] = hosts
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "other.example.org", netrc_file)
        ftp_login(conn, "other.example.org", None)
    assert conn.login.call_args_list == [mock.call(), mock.call()]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_ftp_login_refused(mocker: MockerFixture) -> None:
    """Test that refused credentials lead to an anonymous login.

    Args:
        mocker: The mocker fixture.
    """
    conn = mocker.MagicMock()
    # Mocked: nothing is sent
    conn.login.side_effect = [ftplib.error_perm("530"), None]  # noqa: S321
    env = {**CREDENTIALS, "TESK_FTP_HOSTS": "ftp.example.org"}
    with mock.patch.dict("os.environ", env, clear=True):
        ftp_login(conn, "ftp.example.org", None)
    assert conn.login.call_args_list == [
        mock.call("test", "test_pass"),
        mock.call(),
    ]


def test_ftp_upload_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_upload_file behaves correctly upon upload error."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.storbinary", side_effect=ftplib.error_reply)
    assert ftp_upload_file(conn, __file__, "/home/tesk/test_copy.py") == 1
    assert "Unable to upload file" in caplog.text


def test_ftp_download_file_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_download_file behaves correctly upon download error."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.retrbinary", side_effect=ftplib.error_perm)
    with mock.patch("builtins.open", mock.mock_open(), create=False):
        assert ftp_download_file(conn, "remote.txt", "test_copy.py") == 1
        assert "Unable to download file" in caplog.text


def test_ftp_download_file_success(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a successful download creates the local destination file."""
    conn = mocker.patch("ftplib.FTP")
    mock_retrbin = mocker.patch("ftplib.FTP.retrbinary")
    with mock.patch("builtins.open", mock.mock_open(), create=False) as m:
        assert ftp_download_file(conn, "remote.txt", "test_copy.py") == 0

        mock_retrbin.assert_called_with("RETR remote.txt", mock.ANY)

        m.assert_called_with("test_copy.py", "w+b")

        # Since we want to avoid file creation in testing and we're using
        # 'create=False', we cannot check whether a file exists or not (but
        # it's not really necessary since we can assert that the necessary
        # functions have been invoked.
        # assert os.path.exists('test_copy.py')


def test_ftp_check_directory_error(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that ftp_check_directory logs the proper error message."""
    conn = mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP.cwd", side_effect=ftplib.error_reply)
    assert ftp_check_directory(conn, "/folder/file") == 1
    assert "Could not check if path" in caplog.text


def test_ftp_make_dirs(mocker: MockerFixture) -> None:
    """Test that ftp_make_dirs exits with 0 for an existing directory."""
    conn = mocker.patch("ftplib.FTP")
    assert ftp_make_dirs(conn, os.curdir) == 0


def test_ftp_upload_dir_make_dirs_error(
    mocker: MockerFixture, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a directory upload fails if the remote directory can't be made."""
    mocker.patch("tesk_core.filer.ftp_make_dirs", return_value=1)
    transfer = FTPTransput(
        str(tmp_path), "ftp://ftp.example.org/dir", Type.Directory, MagicMock()
    )
    assert transfer.upload_dir() == 1
    assert "Unable to create remote directory" in caplog.text


def test_ftp_upload_dir_missing_source(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a missing source creates no remote directory."""
    make_dirs = mocker.patch("tesk_core.filer.ftp_make_dirs", return_value=0)
    transfer = FTPTransput(
        str(tmp_path / "missing"),
        "ftp://ftp.example.org/dir",
        Type.Directory,
        MagicMock(),
    )
    with pytest.raises(FileNotFoundError):
        transfer.upload_dir()
    make_dirs.assert_not_called()


def test_ftp_download_dir_missing_source(tmp_path: Path) -> None:
    """Test that a remote directory that can't be entered creates no local one."""
    conn = MagicMock()
    conn.cwd.side_effect = ftplib.error_perm("550 No such directory")  # noqa: S321 -- an error, no connection
    transfer = FTPTransput(
        str(tmp_path / "local"), "ftp://ftp.example.org/dir", Type.Directory, conn
    )
    with pytest.raises(ftplib.error_perm):
        transfer.download_dir()
    assert not (tmp_path / "local").exists()


def test_ftp_download_dir_local_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a local directory that can't be created fails the download."""
    (tmp_path / "local").write_text("a file, not a directory")
    transfer = FTPTransput(
        str(tmp_path / "local"),
        "ftp://ftp.example.org/dir",
        Type.Directory,
        MagicMock(),
    )
    assert transfer.download_dir() == 1
    assert "Unable to create local directory" in caplog.text


@pytest.mark.parametrize("dirbit", ["d", "-"])
@pytest.mark.parametrize("name", ["../outside", "a/b", "a\\b", "..", "."])
def test_ftp_download_dir_invalid_names(
    mocker: MockerFixture,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    dirbit: str,
    name: str,
) -> None:
    """Test that listing entries with invalid names are skipped.

    For directories and files alike; the valid entries are downloaded.
    """
    listing = [
        f"{dirbit}rw-r--r--    1 owner group     7 Nov 07 05:00 {name}",
        "-rw-r--r--    1 owner group     7 Nov 07 05:00 ok.txt",
    ]
    conn = MagicMock()
    conn.retrlines.side_effect = lambda command, callback: [
        callback(line) for line in listing
    ]
    download = mocker.patch("tesk_core.filer.ftp_download_file", return_value=0)
    dest = tmp_path / "dest" / "local"
    transfer = FTPTransput(str(dest), "ftp://ftp.example.org/dir", Type.Directory, conn)

    assert transfer.download_dir() == 0

    download.assert_called_once_with(conn, "/dir/ok.txt", str(dest) + "/ok.txt")
    assert sorted(path.name for path in tmp_path.rglob("*")) == ["dest", "local"]
    assert "Skipping listing entry" in caplog.text
