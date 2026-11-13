#!/usr/bin/env python3
"""The filer: stages the inputs and outputs of a task.

Run as `filer {inputs|outputs} DATA`, with the task as JSON, or a gzipped
JSON file (`*.gz`).
"""

import argparse
import base64
from collections.abc import Callable, Iterator
import contextlib
import errno
import ftplib
from ftplib import FTP
import functools
from glob import glob
import gzip
import json
import logging
import math
import netrc
import os
import re
import shutil
import sys
from typing import Any
from urllib.parse import quote, urlparse, urlsplit, urlunsplit

import requests

from tesk_core.exception import FileProtocolDisabled, InvalidURL, UnknownProtocol
from tesk_core.filer_s3 import S3Transput
from tesk_core.path import (
    containerPath,
    fileEnabled,
    filerPath,
    getPath,
    hasWildcards,
    taskPath,
)
from tesk_core.transput import (
    Transput,
    Type,
    body_for_log,
    open_for_upload,
    output_has_symlink,
    url_for_log,
    within,
)
from tesk_core.Util import configure_logging

# Prefix of the line on which the outputs filer prints the uploaded files as
# JSON (see `output_file_logs`), which the taskmaster reads from its log
OUTPUT_LOGS_MARKER = "TESK_OUTPUT_FILE_LOGS"
# The most bytes of that JSON; the taskmaster stores at most 200,000 bytes,
# with the URLs added
OUTPUT_LOGS_MAX_BYTES = 100_000


def http_join(url: str, name: str) -> str:
    """Append a file name to the path of an HTTP(S) URL.

    The name is escaped, so characters such as `#` and `?` stay part of it,
    and the URL's query, which may carry a token, is kept.

    Args:
        url: The URL of a directory.
        name: The file name, as on disk.

    Returns:
        The URL of the file.
    """
    parts = urlsplit(url)
    # Quote the name's bytes on disk: os.listdir returns a name that isn't
    # valid UTF-8 with surrogate escapes, which quote can't encode as text.
    path = parts.path.rstrip("/") + "/" + quote(os.fsencode(name), safe="")
    return urlunsplit(parts._replace(path=path))


# Seconds that the filer waits to connect to an HTTP(S) server, and for each
# read from it: not a limit on the whole transfer, so large files work
HTTP_TIMEOUT_ENV = "FILER_HTTP_TIMEOUT"
HTTP_TIMEOUT_DEFAULT = 300.0
# Longer timeouts (about 11 days) are invalid: sockets can't take any length
HTTP_TIMEOUT_MAX = 1_000_000.0
# Bytes of an HTTP(S) download held in memory at a time
HTTP_CHUNK_SIZE = 1024 * 1024
# Bytes of the body of an HTTP(S) error response read for the log
ERROR_BODY_BYTES = 64 * 1024


@functools.cache
def log_invalid_http_timeout() -> None:
    """Log, once per process, that the HTTP timeout is invalid."""
    logging.error(
        "%s must be a positive number of seconds; using %g",
        HTTP_TIMEOUT_ENV,
        HTTP_TIMEOUT_DEFAULT,
    )


def http_timeout() -> float:
    """Get the timeout of HTTP(S) requests.

    From `FILER_HTTP_TIMEOUT`, in seconds, for connecting and for each read;
    the default if it is unset, empty or invalid (logged once).

    Returns:
        The timeout, in seconds.
    """
    value = os.environ.get(HTTP_TIMEOUT_ENV, "").strip()
    if not value:
        return HTTP_TIMEOUT_DEFAULT
    try:
        timeout = float(value)
    except ValueError:
        timeout = math.nan
    if not math.isfinite(timeout) or not 0 < timeout <= HTTP_TIMEOUT_MAX:
        log_invalid_http_timeout()
        return HTTP_TIMEOUT_DEFAULT
    return timeout


def netrc_secrets(url: str) -> list[str]:
    """Get the `.netrc` credentials that `requests` sends to a URL.

    For redacting them from log messages: the user name, the password, and
    both as a Basic authorization value.

    Args:
        url: The URL.

    Returns:
        The credentials, as sent; empty without any.
    """
    try:
        auth = requests.utils.get_netrc_auth(url)
    except Exception:
        return []
    if not auth:
        return []
    user, password = auth
    basic = base64.b64encode(f"{user}:{password}".encode()).decode()
    return [user, password, basic]


def error_body(req: requests.Response) -> str:
    """Read the start of the body of an error response, for log messages.

    Reads at most `ERROR_BODY_BYTES`, so that a large error body of a
    streamed response isn't read into memory whole. That is far more than
    the characters that `body_for_log` keeps (at most 4 bytes each), so
    that it can redact values that start among them and end after them.

    Args:
        req: The response.

    Returns:
        The start of the body, decoded with the response's encoding (UTF-8
        if it has none or an unknown one), with undecodable bytes replaced.
    """
    limit = ERROR_BODY_BYTES
    data = b""
    for chunk in req.iter_content(chunk_size=limit):
        data += chunk
        if len(data) >= limit:
            break
    try:
        return data[:limit].decode(req.encoding or "utf-8", errors="replace")
    except LookupError:
        return data[:limit].decode("utf-8", errors="replace")


class HTTPTransput(Transput):
    """Transfer of an input or output over HTTP(S)."""

    def _status_ok(self, req: requests.Response) -> bool:
        """Check the status of a response, and log it if it isn't 2xx.

        The body of an error is logged prepared by `body_for_log`.

        Args:
            req: The response.

        Returns:
            Whether the status is 2xx.
        """
        if req.status_code < 200 or req.status_code >= 300:
            logging.error("Got status code: %d", req.status_code)
            text = error_body(req)
            if text:
                sent = [req.url, *(item.url for item in req.history)]
                urls = [self.url, *(url for url in sent if url)]
                logging.error(
                    "Response body: %s",
                    body_for_log(
                        text,
                        *urls,
                        secrets=[s for url in urls for s in netrc_secrets(url)],
                    ),
                )
            return False
        logging.debug("OK, got status code: %d", req.status_code)
        return True

    def _request_failed(self, err: Exception) -> int:
        """Log a failed request.

        The message of a requests exception contains the full URL, so only
        its type is logged, with the URL as in other log messages.

        Args:
            err: The exception of the request.

        Returns:
            1, as the transfer failed.
        """
        logging.error("%s for %s", type(err).__name__, url_for_log(self.url))
        return 1

    def download_file(self) -> int:
        """Download the file with `GET`, streamed to disk.

        The body is written in chunks of `HTTP_CHUNK_SIZE` bytes, so that
        large files don't have to fit in memory. If the body breaks off, or
        the file can't be written (e.g., a full volume), the partial file is
        removed.

        Returns:
            0 on success, otherwise 1.
        """
        opened = False
        try:
            with requests.get(self.url, timeout=http_timeout(), stream=True) as req:
                if not self._status_ok(req):
                    return 1
                with open(self.path, "wb") as file:
                    opened = True
                    for chunk in req.iter_content(chunk_size=HTTP_CHUNK_SIZE):
                        file.write(chunk)
        # Before `OSError`, which `RequestException` derives from
        except requests.exceptions.RequestException as err:
            self._remove_partial(opened)
            return self._request_failed(err)
        except OSError as err:
            self._remove_partial(opened)
            logging.error("Unable to write input %s: %s", self.path, err.strerror)
            return 1
        return 0

    def _remove_partial(self, opened: bool) -> None:
        """Remove the file of a failed download, if it was opened.

        Args:
            opened: Whether the download opened (and so created) the file.
        """
        if opened:
            with contextlib.suppress(OSError):
                os.remove(self.path)

    def upload_file(self) -> int:
        """Upload the file with `PUT`, as bytes.

        Returns:
            0 on success, otherwise 1.
        """
        # Send the file as bytes, streamed from disk: decoding it as text fails
        # for binary files and changes some text. An empty file is sent as b''
        # because requests sends an empty file object with chunked encoding,
        # which presigned upload URLs such as S3's reject.
        try:
            file = open_for_upload(self.path)
        except OSError as err:
            logging.error("Unable to open output %s: %s", self.path, err.strerror)
            return 1
        with file:
            data = file if os.fstat(file.fileno()).st_size > 0 else b""
            try:
                req = requests.put(self.url, data=data, timeout=http_timeout())
            except requests.exceptions.RequestException as err:
                return self._request_failed(err)

        if not self._status_ok(req):
            return 1
        return 0

    def upload_dir(self) -> int:
        """Upload the files below the directory, recursively.

        Returns:
            0 on success, otherwise 1.
        """
        to_upload = []
        for listing in os.listdir(self.path):
            file_path = self.path + "/" + listing
            if os.path.isdir(file_path):
                ftype = Type.Directory
            elif os.path.isfile(file_path):
                ftype = Type.File
            else:
                return 1
            to_upload.append(
                HTTPTransput(file_path, http_join(self.url, listing), ftype)
            )

        # Stop at the first upload that fails, as for the entries of a task
        for transput in to_upload:
            if transput.upload(check_links=False):
                return 1
        return 0

    def download_dir(self) -> int:
        """Refuse to download a directory, which HTTP can't list.

        Returns:
            1, as the transfer failed.
        """
        logging.error(
            "Won't crawl http directory, so unable to download url: %s",
            url_for_log(self.url),
        )
        return 1


def copyDir(src: str, dst: str) -> None:
    """Copy a directory, also into an existing one.

    Directories that don't exist yet are copied whole (`shutil.copytree`);
    existing ones are merged, entry by entry, and keep their own mode
    (e.g., of a mount point the filer doesn't own); existing files are
    replaced. Links in the destination, and `dst` if it is one, are
    replaced, not followed.

    Args:
        src: The source directory.
        dst: The destination directory.

    Raises:
        IsADirectoryError: If a file of the source meets a directory of the
            same name in the destination.
    """
    if os.path.islink(dst):
        os.unlink(dst)
    if not os.path.isdir(dst):
        shutil.copytree(src, dst)
        return
    for entry in os.scandir(src):
        target = os.path.join(dst, entry.name)
        if entry.is_dir():
            # Replaces a link at `target`, as for `dst`
            copyDir(entry.path, target)
        elif os.path.islink(target):
            os.unlink(target)
            shutil.copy2(entry.path, target)
        elif os.path.isdir(target):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", target)
        else:
            shutil.copy2(entry.path, target)


def copyFile(src: str, dst: str) -> None:
    """Copy the files that match a glob pattern.

    `shutil.copy` takes `*` as a character, not as a glob. If `dst` has a
    `*`, the files are copied into its directory.

    Args:
        src: The source, a glob pattern.
        dst: The destination.
    """
    # If there is any * in 'dst', use only the dirname (base path)
    p = re.compile(r".*\*.*")
    if p.match(dst):
        dst = os.path.dirname(dst)

    for file in glob(src):
        shutil.copy(file, dst)


class FileTransput(Transput):
    """Transfer of an input or output from or to the shared volume (`file:`).

    Attributes:
        urlContainerPath: The URL's path in the filer's container.
    """

    def __init__(self, path: str, url: str, ftype: Type) -> None:
        """Describe the transfer.

        Args:
            path: Path in the filer's container.
            url: URL of the input or output.
            ftype: Whether it is a file or a directory.

        Raises:
            InvalidHostPath: The URL's path is not on the shared volume.
        """
        Transput.__init__(self, path, url, ftype)

        self.urlContainerPath = containerPath(getPath(self.url))

    def transfer(self, copyFn: Callable[[str, str], object], src: str, dst: str) -> int:
        """Copy with a function.

        Args:
            copyFn: The function, called with `src` and `dst`.
            src: The source.
            dst: The destination.

        Returns:
            0: errors are raised by `copyFn`.
        """
        logging.debug("Copying %s to %s", src, dst)
        copyFn(src, dst)
        return 0

    def download_file(self) -> int:
        """Copy the file from the shared volume.

        Returns:
            0: errors are raised.
        """
        return self.transfer(shutil.copy, self.urlContainerPath, self.path)

    def download_dir(self) -> int:
        """Copy the directory from the shared volume.

        Returns:
            0: errors are raised.
        """
        return self.transfer(copyDir, self.urlContainerPath, self.path)

    def upload_file(self) -> int:
        """Copy the files that match the path to the shared volume.

        Returns:
            0 on success, 1 if a match is or contains a symbolic link; other
            errors are raised.
        """
        # `upload` checked the pattern; this checks what it matches
        if any(output_has_symlink(match) for match in glob(self.path)):
            return 1
        return self.transfer(copyFile, self.path, self.urlContainerPath)

    def upload_dir(self) -> int:
        """Copy the directory to the shared volume.

        Returns:
            0: errors are raised.
        """
        return self.transfer(copyDir, self.path, self.urlContainerPath)


class FTPTransput(Transput):
    """Transfer of an input or output over FTP.

    Attributes:
        connection_owner: Whether the transfer opened the connection (and
            closes it on exit), or got it from the transfer of a parent
            directory.
        ftp_connection: The FTP connection.
    """

    def __init__(
        self, path: str, url: str, ftype: Type, ftp_conn: FTP | None = None
    ) -> None:
        """Describe the transfer; entering it as a context connects.

        Args:
            path: Path in the filer's container.
            url: URL of the input or output.
            ftype: Whether it is a file or a directory.
            ftp_conn: A connection to reuse, if any.
        """
        Transput.__init__(self, path, url, ftype)

        self.connection_owner = ftp_conn is None
        # FTP is a protocol of TES tasks' URLs, insecure or not
        self.ftp_connection = (
            FTP() if ftp_conn is None else ftp_conn  # noqa: S321 -- see above
        )

    def __enter__(self) -> "FTPTransput":
        """Connect and log in, unless the connection is reused.

        Connects to the URL's host and port (the default port, 21, if the URL
        has none), without its user information: the host and port that the
        credentials are looked up for (see `ftp_login`).

        Returns:
            The transfer itself.

        Raises:
            InvalidURL: The URL has no valid host or port.
        """
        if self.connection_owner:
            target = split_host_port(self.netloc)
            if target is None:
                raise InvalidURL("Invalid host or port in FTP URL")
            host, port = target
            # Port 0 makes `ftplib` use the default port
            self.ftp_connection.connect(host, port or 0)
            ftp_login(self.ftp_connection, self.netloc, self.netrc_file)
        return self

    def upload_dir(self) -> int:
        """Upload the directory and everything below it, recursively.

        Creates the remote directory and its parents, so that empty
        directories are kept.

        Returns:
            0 on success, otherwise 1.
        """
        # The source first, so that a missing one creates nothing
        entries = os.listdir(self.path)
        if ftp_make_dirs(self.ftp_connection, self.url_path):
            logging.error("Unable to create remote directory %s", url_for_log(self.url))
            return 1

        for file in entries:
            file_path = self.path + "/" + file
            file_url = self.url + "/" + file

            if os.path.isdir(file_path):
                ftype = Type.Directory
            elif os.path.isfile(file_path):
                ftype = Type.File
            else:
                logging.error(
                    'Directory listing in is neither file nor directory: "%s"',
                    url_for_log(file_url),
                )
                return 1

            logging.debug('Uploading %s\t"%s"', ftype.value, file_path)

            # Recurse into new transfers, ending with files, which are
            # uploaded; on this connection, as downloads do, so that a
            # directory needs one login, not one per entry. Remote paths must
            # stay absolute: entries share the connection's working directory
            with FTPTransput(
                file_path, file_url, ftype, self.ftp_connection
            ) as transfer:
                if transfer.upload(check_links=False):
                    return 1
        return 0

    def upload_file(self) -> int:
        """Upload the file, creating the remote directories.

        Returns:
            0 on success, otherwise 1.
        """
        error = ftp_make_dirs(self.ftp_connection, os.path.dirname(self.url_path))
        if error:
            logging.error(
                "Unable to create remote directories needed for %s",
                url_for_log(self.url),
            )
            return 1

        if not ftp_check_directory(self.ftp_connection, self.url_path):
            return 1

        return ftp_upload_file(self.ftp_connection, self.path, self.url_path)

    def download_dir(self) -> int:
        """Download the directory and everything below it, recursively.

        Creates the local directory and its parents, so that empty
        directories are kept.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            "Processing ftp dir: %s target: %s", url_for_log(self.url), self.path
        )
        # The source first, so that a missing one creates nothing
        self.ftp_connection.cwd(self.url_path)
        try:
            os.makedirs(self.path, exist_ok=True)
        except OSError:
            logging.exception("Unable to create local directory %s", self.path)
            return 1

        # This is horrible and I'm sorry but it works flawlessly.
        # Credit to Chris Haas for writing this
        # See https://stackoverflow.com/questions/966578/parse-response-from-ftp-list-command-syntax-variations
        # for attribution
        ftp_command = re.compile(
            r"^(?P<dir>[\-ld])(?P<permission>([\-r][\-w][\-xs]){3})\s+"
            r"(?P<filecode>\d+)\s+(?P<owner>\w+)\s+(?P<group>\w+)\s+"
            r"(?P<size>\d+)\s+(?P<timestamp>((\w{3})\s+(\d{2})\s+(\d{1,2}):(\d{2}))"
            r"|((\w{3})\s+(\d{1,2})\s+(\d{4})))\s+(?P<name>.+)$"
        )

        lines: list[str] = []
        self.ftp_connection.retrlines("LIST", lines.append)

        for line in lines:
            matches = ftp_command.match(line)
            # Lines that don't match (`None`) raise `AttributeError`, as before
            dirbit = matches.group("dir")  # type: ignore[union-attr]
            name = matches.group("name")  # type: ignore[union-attr]

            file_path = self.path + "/" + name
            if not valid_entry_name(name, self.path, file_path):
                logging.warning("Skipping listing entry %r", name)
                continue
            file_url = self.url + "/" + name

            ftype = Type.Directory if dirbit == "d" else Type.File

            # Recurse into new transfers, ending with files, which are
            # downloaded
            with FTPTransput(
                file_path, file_url, ftype, self.ftp_connection
            ) as transfer:
                if transfer.download():
                    return 1
        return 0

    def download_file(self) -> int:
        """Download the file, creating its local directory.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            'Downloading ftp file: "%s" Target: %s', url_for_log(self.url), self.path
        )
        basedir = os.path.dirname(self.path)
        if basedir and not os.path.exists(basedir):
            os.makedirs(basedir, exist_ok=True)

        return ftp_download_file(self.ftp_connection, self.url_path, self.path)

    def delete(self) -> None:
        """Close the connection, if the transfer opened it."""
        if self.connection_owner:
            self.ftp_connection.close()


def valid_entry_name(name: str, parent: str, path: str) -> bool:
    """Tell whether a name from a directory listing is safe to use.

    Args:
        name: The name of the entry.
        parent: The local directory the entry is downloaded into.
        path: The local path of the entry.

    Returns:
        Whether the name is a single path component other than `.` and
        `..`, and the path stays below `parent`.
    """
    if name in {"", ".", ".."} or "/" in name or "\\" in name:
        return False
    return within(parent, path)


FTP_HOSTS_ENV = "TESK_FTP_HOSTS"
"""Environment variable with the hosts that get the env FTP credentials."""


def split_host_port(netloc: str) -> tuple[str, int | None] | None:
    """Split a network location into its host and port.

    Args:
        netloc: `host` or `host:port`, with user information, if any; IPv6
            addresses in brackets.

    Returns:
        The host in lower case (IPv6 addresses without brackets) and the
        port, or `None` if there is none; `None` if the network location
        has no host or an invalid port.
    """
    try:
        parts = urlsplit("//" + netloc)
        port = parts.port
    except ValueError:
        return None
    if not parts.hostname:
        return None
    return parts.hostname, port


def ftp_host_entries(hosts: str) -> list[str]:
    """Split `TESK_FTP_HOSTS` into its entries.

    Args:
        hosts: Comma-separated entries.

    Returns:
        The entries, without the spaces around them; empty entries are left
        out. Other whitespace (e.g., tabs) is kept, which makes an entry
        invalid.
    """
    return [entry.strip(" ") for entry in hosts.split(",") if entry.strip(" ")]


def ftp_host_allowed(netloc: str, hosts: str) -> bool:
    """Check whether a URL's host is in a list of hosts.

    Each entry of the list is `host` or `host:port` (IPv6 addresses in
    brackets). Hosts match exactly, ignoring case. An entry without a port
    matches the default port, 21, only, and one with a port only that port;
    URLs without a port have port 21. Invalid entries (see
    `ftp_host_entry_valid`) match nothing.

    Args:
        netloc: The network location of the URL.
        hosts: Comma-separated entries, as in `TESK_FTP_HOSTS`.

    Returns:
        Whether an entry matches the URL's host and port.
    """
    target = split_host_port(netloc)
    if target is None:
        return False
    host, port = target
    for entry in ftp_host_entries(hosts):
        allowed = split_host_port(entry) if ftp_host_entry_valid(entry) else None
        if allowed is None or allowed[0] != host:
            continue
        if (allowed[1] or ftplib.FTP_PORT) == (port or ftplib.FTP_PORT):
            return True
    return False


def ftp_host_entry_valid(entry: str) -> bool:
    """Check whether an entry of `TESK_FTP_HOSTS` is `host` or `host:port`.

    Args:
        entry: The entry, without surrounding spaces.

    Returns:
        Whether the entry is valid: no user information, path, query,
        fragment, whitespace or control characters, and no empty, invalid
        or zero port.
    """
    # `urlsplit` would split off user information, a path, a query or a
    # fragment, and read an empty port (`host:`) as none
    if (
        not entry.isprintable()
        or any(char in entry for char in "@/?# ")
        or entry.endswith(":")
    ):
        return False
    parts = split_host_port(entry)
    return parts is not None and parts[1] != 0


@functools.cache
def log_invalid_ftp_hosts(count: int) -> None:
    """Log, once per process, that entries of `TESK_FTP_HOSTS` are invalid.

    The entries aren't logged, in case one contains credentials.

    Args:
        count: The number of invalid entries.
    """
    logging.error(
        "%s has %d invalid entries, which match no host: use host or "
        "host:port, separated by commas.",
        FTP_HOSTS_ENV,
        count,
    )


@functools.cache
def log_missing_ftp_hosts() -> None:
    """Log, once per process, that the env FTP credentials are not used."""
    # Names the variables, not their values (semgrep: false positive)
    logging.error(  # nosemgrep
        "TESK_FTP_USERNAME and TESK_FTP_PASSWORD are set, but %s lists no "
        "hosts: "
        "the credentials are not sent to any FTP host, and the filer logs "
        "in anonymously. Set %s to the hosts of the FTP account, or use a "
        ".netrc file.",
        FTP_HOSTS_ENV,
        FTP_HOSTS_ENV,
    )


def ftp_env_credentials(netloc: str) -> tuple[str, str] | None:
    """Get the env FTP credentials, if they may be sent to the URL's host.

    `TESK_FTP_USERNAME` (if not empty) and `TESK_FTP_PASSWORD` are sent
    only to the hosts in `TESK_FTP_HOSTS` (see `ftp_host_allowed`). Without
    entries in `TESK_FTP_HOSTS`, they are sent nowhere, and an error is
    logged once.

    Args:
        netloc: The network location of the URL.

    Returns:
        The user name and password, or `None`.
    """
    user = os.environ.get("TESK_FTP_USERNAME")
    password = os.environ.get("TESK_FTP_PASSWORD")
    # An empty user name is no account (e.g., from a secret without one)
    if not user or password is None:
        return None
    hosts = os.environ.get(FTP_HOSTS_ENV, "")
    entries = ftp_host_entries(hosts)
    if not entries:
        log_missing_ftp_hosts()
        return None
    invalid = sum(not ftp_host_entry_valid(entry) for entry in entries)
    if invalid:
        log_invalid_ftp_hosts(invalid)
    if not ftp_host_allowed(netloc, hosts):
        logging.debug(
            "FTP host %s is not in %s; logging in anonymously",
            netloc.rpartition("@")[2],
            FTP_HOSTS_ENV,
        )
        return None
    return user, password


def netloc_host(netloc: str) -> str:
    """Get the host of a URL's network location, as written.

    Unlike `urlsplit(...).hostname`, keeps the case, as `.netrc` entries
    match hosts case-sensitively.

    Args:
        netloc: The network location, with user information and port, if any.

    Returns:
        The host, without user information, port and IPv6 brackets.
    """
    host = netloc.rpartition("@")[2]
    if host.startswith("["):
        return host[1:].partition("]")[0]
    return host.partition(":")[0]


def ftp_login(ftp_connection: FTP, netloc: str, netrc_file: netrc.netrc | None) -> None:
    """Log in to an FTP server.

    With the first credentials found: the `.netrc` entry for the host and
    port, the one for the host (only for the default port, 21), or the
    `default` entry, if `.netrc` was read; otherwise `TESK_FTP_USERNAME` and
    `TESK_FTP_PASSWORD`, if both are set and the host is in
    `TESK_FTP_HOSTS` (see `ftp_env_credentials`). Without credentials, if
    they are refused, or if the host can't be parsed the same way as for
    the connection, anonymously.

    Args:
        ftp_connection: The connection.
        netloc: The network location of the URL: host, and port and user
            information, if any.
        netrc_file: The `.netrc` file, if it was read.
    """
    user = None
    password: str | None = None
    creds: tuple[str, str | None, str | None] | None = None
    target = split_host_port(netloc)
    host = netloc_host(netloc)
    # `FTPTransput` connects to the host of `split_host_port`; the `.netrc`
    # lookup keeps the host's case, but must otherwise agree with it (callers
    # may pass any network location), or no credentials are sent
    if target is None or target[0] != host.lower():
        logging.error("Invalid host in FTP URL; logging in anonymously")
        ftp_connection.login()
        return
    if netrc_file is not None:
        # An entry for the host and port as written; for the default port,
        # one for the host with `:21`, or without a port; then the `default`
        # entry
        written = netloc.rpartition("@")[2]
        creds = netrc_file.hosts.get(written)
        if not creds and (target[1] or ftplib.FTP_PORT) == ftplib.FTP_PORT:
            bare = written[: written.index("]") + 1] if "]" in written else host
            creds = netrc_file.hosts.get(f"{bare}:{ftplib.FTP_PORT}")
            creds = creds or netrc_file.hosts.get(bare) or netrc_file.hosts.get(host)
        creds = creds or netrc_file.hosts.get("default")
    if creds:
        user, _, password = creds
    else:
        env_creds = ftp_env_credentials(netloc)
        if env_creds:
            user, password = env_creds

    if user:
        try:
            ftp_connection.login(user, password or "")
        except ftplib.error_perm:
            ftp_connection.login()
    else:
        ftp_connection.login()


def ftp_check_directory(ftp_connection: FTP, path: str) -> int:
    """Check whether a remote path is a directory.

    Following the convention of the rest of the code, an error code.

    Args:
        ftp_connection: The connection.
        path: The remote path.

    Returns:
        0 if it is a directory, 1 if it is not or the check failed.
    """
    response = ftp_connection.pwd()
    if response == "":
        return 1
    original_directory = response

    # We are NOT scp, so we won't create a file when filename is not
    # specified (mirrors input behaviour)
    try:
        ftp_connection.cwd(path)
        logging.error(
            'Path "%s" at "%s" already exists and is a folder. \
            Please specify a target filename and retry',
            path,
            ftp_connection.host,
        )
        is_directory = True
    except ftplib.error_perm:
        is_directory = False
    except (ftplib.error_reply, ftplib.error_temp):
        logging.exception(
            'Could not check if path "%s" in "%s" is directory',
            path,
            ftp_connection.host,
        )
        return 1
    try:
        ftp_connection.cwd(original_directory)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception(
            'Error when checking if "%s" in "%s" was a directory',
            path,
            ftp_connection.host,
        )
        return 1

    return 0 if is_directory else 1


def ftp_upload_file(
    ftp_connection: FTP, local_source_path: str, remote_destination_path: str
) -> int:
    """Upload a file.

    Args:
        ftp_connection: The connection.
        local_source_path: The local file.
        remote_destination_path: The remote path.

    Returns:
        0 on success, otherwise 1.
    """
    try:
        with open_for_upload(local_source_path) as file:
            ftp_connection.storbinary("STOR /" + remote_destination_path, file)
    # `OSError`: the file can't be opened or read, or the connection failed
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp, OSError):
        logging.exception(
            'Unable to upload file "%s" to "%s" as "%s"',
            local_source_path,
            ftp_connection.host,
            remote_destination_path,
        )
        return 1
    return 0


def ftp_download_file(
    ftp_connection: FTP, remote_source_path: str, local_destination_path: str
) -> int:
    """Download a file.

    Args:
        ftp_connection: The connection.
        remote_source_path: The remote path.
        local_destination_path: The local file.

    Returns:
        0 on success, otherwise 1.
    """
    try:
        with open(local_destination_path, "w+b") as file:
            ftp_connection.retrbinary("RETR " + remote_source_path, file.write)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception(
            'Unable to download file "%s" from "%s" as "%s"',
            remote_source_path,
            ftp_connection.host,
            local_destination_path,
        )
        return 1
    return 0


def subfolders_in(whole_path: str) -> list[str]:
    """Get all directories of a path, in order.

    >>> subfolders_in('/')
    ['/']

    >>> subfolders_in('/this/is/a/path')
    ['/this', '/this/is', '/this/is/a', '/this/is/a/path']

    >>> subfolders_in('this/is/a/path')
    ['this', 'this/is', 'this/is/a', 'this/is/a/path']

    Args:
        whole_path: The path.

    Returns:
        The path and each of its parents, shortest first.
    """
    path_fragments = whole_path.lstrip("/").split("/")
    if whole_path.startswith("/"):
        path_fragments[0] = "/" + path_fragments[0]
    path = path_fragments[0]
    subfolders = [path]
    for fragment in path_fragments[1:]:
        path += "/" + fragment
        subfolders.append(path)
    return subfolders


def ftp_make_dirs(  # noqa: PLR0911 -- an error code for each step that can fail
    ftp_connection: FTP, path: str
) -> int:
    """Create a remote directory and its parents, unless it exists.

    Args:
        ftp_connection: The connection.
        path: The remote directory.

    Returns:
        0 on success, otherwise 1.
    """
    response = ftp_connection.pwd()
    if response == "":
        return 1
    original_directory = response

    # if directory exists do not do anything else
    try:
        ftp_connection.cwd(path)
        return 0
    except (ftplib.error_perm, ftplib.error_temp):
        pass
    except ftplib.error_reply:
        logging.exception(
            'Unable to create directory "%s" at "%s"', path, ftp_connection.host
        )
        return 1

    for subfolder in subfolders_in(path):
        try:
            ftp_connection.cwd(subfolder)
        except (ftplib.error_perm, ftplib.error_temp):
            try:
                ftp_connection.mkd(subfolder)
            except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
                logging.exception(
                    'Unable to create directory "%s" at "%s"',
                    subfolder,
                    ftp_connection.host,
                )
                return 1
        except ftplib.error_reply:
            logging.exception(
                'Unable to create directory "%s" at "%s"', path, ftp_connection.host
            )
            return 1

    try:
        ftp_connection.cwd(original_directory)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception(
            'Unable to create directory "%s" at "%s"', path, ftp_connection.host
        )
        return 1
    return 0


def file_from_content(filedata: dict[str, Any]) -> int:
    """Write the `content` of an input to its `path`.

    Args:
        filedata: The input.

    Returns:
        0: errors are raised.
    """
    with open(filedata["path"], "w") as file:
        file.write(str(filedata["content"]))
    return 0


def newTransput(scheme: str, netloc: str) -> type[Transput]:
    """Select the transfer class for a URL scheme.

    Args:
        scheme: The scheme of the URL.
        netloc: The network location of the URL (unused).

    Returns:
        The transfer class.

    Raises:
        FileProtocolDisabled: `file:` URLs are used, but no shared volume is
            configured.
        UnknownProtocol: The scheme is not supported.
    """

    def fileTransputIfEnabled() -> type[Transput]:
        if fileEnabled():
            return FileTransput
        raise FileProtocolDisabled(
            "'file:' protocol disabled\n"
            "To enable it, both 'HOST_BASE_PATH' and 'CONTAINER_BASE_PATH' "
            "environment variables must be defined."
        )

    if scheme == "ftp":
        return FTPTransput
    if scheme == "file":
        return fileTransputIfEnabled()
    if scheme in ["http", "https"]:
        return HTTPTransput
    if scheme == "s3":
        return S3Transput
    raise UnknownProtocol(f"Unknown protocol: '{scheme}'")


def url_join(url: str, relative: str) -> str:
    """Append a relative path to the URL of a directory.

    Each component of an HTTP(S) URL's path is escaped (see `http_join`);
    other schemes take the path as it is.

    Args:
        url: The URL of the directory.
        relative: The path below it, with `/` between components.

    Returns:
        The URL of the path.
    """
    if urlsplit(url).scheme in {"http", "https"}:
        for part in relative.split("/"):
            url = http_join(url, part)
        return url
    return url.rstrip("/") + "/" + relative


def upload_matches(filedata: dict[str, Any]) -> int:
    """Upload the files and directories that match an output's wildcards.

    As in TES 1.1, `path_prefix` is removed from the path of each match,
    and the rest is appended to the output's URL, a directory: with
    `path` `/data/*.txt`, `path_prefix` `/data` and URL `s3://b/out`,
    `/data/a.txt` is uploaded to `s3://b/out/a.txt`. A pattern that matches
    nothing uploads nothing, and succeeds.

    Args:
        filedata: The output, with `path_prefix`.

    Returns:
        0 on success, 1 if a match is not below `path_prefix`, is or
        contains a symbolic link, can't be named in the URL's scheme, or
        its upload failed; nothing is uploaded unless an upload failed.
    """
    pattern = filerPath(filedata["path"])
    prefix = filerPath(filedata["path_prefix"])
    matches = sorted(glob(pattern))
    if not matches:
        logging.info("No files match %s, nothing to upload", filedata["path"])
        return 0
    parsed_url = urlparse(filedata["url"])
    scheme = parsed_url.scheme or "file"
    trans = newTransput(scheme, parsed_url.netloc)
    uploads = checked_matches(filedata, matches, prefix, scheme)
    if uploads is None:
        return 1
    for match, relative in uploads:
        ftype = Type.Directory if os.path.isdir(match) else Type.File
        url = url_join(filedata["url"], relative)
        try:
            with trans(match, url, ftype) as transfer:
                if isinstance(transfer, FileTransput):
                    copy_match(match, transfer.urlContainerPath, ftype)
                elif transfer.upload(check_links=False):
                    return 1
        except InvalidURL as err:
            logging.error("%s: %s", err, url_for_log(filedata["url"]))
            return 1
    return 0


def checked_matches(
    filedata: dict[str, Any], matches: list[str], prefix: str, scheme: str
) -> list[tuple[str, str]] | None:
    """Check the matches of an output before any of them is uploaded.

    Args:
        filedata: The output.
        matches: The matches, in the filer.
        prefix: The output's `path_prefix`, in the filer.
        scheme: The scheme of the output's URL.

    Returns:
        Each match with its path below `prefix`; `None` (logged) if a match
        is not below `prefix`, is or contains a symbolic link, or has a name
        that a URL of the scheme can't carry.
    """
    uploads = []
    for match in matches:
        if not within(prefix, match):
            logging.error(
                "Output %s matches %s, which is not below its path_prefix %s",
                filedata["path"],
                match,
                filedata["path_prefix"],
            )
            return None
        if output_has_symlink(match, recursive=True):
            return None
        relative = os.path.relpath(match, prefix).replace(os.sep, "/")
        if scheme not in {"http", "https"} and any(c in relative for c in "#?"):
            # Only HTTP(S) URLs are escaped (`http_join`): other transfers
            # read the URL's path as it is, where `#` and `?` would end it
            logging.error(
                "Output %s matches %s, whose name can't be part of a %s: URL",
                filedata["path"],
                match,
                scheme,
            )
            return None
        uploads.append((match, relative))
    return uploads


def copy_match(src: str, dst: str, ftype: Type) -> None:
    """Copy a match of an output with wildcards to the shared volume.

    Unlike `FileTransput.upload_file`, the match is a concrete path, so it
    isn't taken as a pattern again (a name with `[` would match nothing),
    and the destination's directory is created for matches below
    subdirectories.

    Args:
        src: The match, in the filer.
        dst: Its destination on the shared volume, in the filer.
        ftype: Whether it is a file or a directory.
    """
    if ftype == Type.Directory:
        copyDir(src, dst)
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)


def process_file(ttype: str, filedata: dict[str, Any]) -> int:
    """Stage an input or output.

    The task's `path` is staged at its place in the filer, below
    `TASK_PATH_PREFIX` (see `filerPath`), where the taskmaster mounts it.

    Args:
        ttype: `inputs` or `outputs`.
        filedata: The input or output, with the task's path.

    Returns:
        0 on success, otherwise 1.
    """
    if "content" in filedata:
        return file_from_content({**filedata, "path": filerPath(filedata["path"])})
    parsed_url = urlparse(filedata["url"])
    scheme = parsed_url.scheme
    netloc = parsed_url.netloc
    if scheme == "":
        logging.info(
            'Could not determine protocol for url: "%s", assuming "file"',
            url_for_log(filedata["url"]),
        )
        scheme = "file"

    # Outputs with wildcards and a prefix to remove, as defined in TES 1.1;
    # without `path_prefix`, the path is taken as before
    if (
        ttype == "outputs"
        and filedata.get("path_prefix")
        and hasWildcards(filedata["path"])
    ):
        return upload_matches(filedata)

    trans = newTransput(scheme, netloc)
    path = filerPath(filedata["path"])

    try:
        with trans(path, filedata["url"], Type(filedata["type"])) as transfer:
            if ttype == "inputs":
                return transfer.download()
            if ttype == "outputs":
                return transfer.upload()
    except InvalidURL as err:
        logging.error("%s: %s", err, url_for_log(filedata["url"]))
        return 1

    logging.info("There was no action to do with %s", filedata["path"])
    return 0


def output_file_logs(outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Describe the uploaded files of a task's outputs (TES `tesOutputFileLog`).

    Each file of a `FILE` output, every file below a `DIRECTORY` output,
    and every file that the wildcards of an output with `path_prefix`
    matched (or that is below a matched directory). Symbolic links, which
    an upload refuses, and empty directories give none. The entries hold
    no URLs, which may carry credentials and would then be in the pod's
    log: the taskmaster derives them from the task (see `output_file_url`).
    Entries beyond `OUTPUT_LOGS_MAX_BYTES` of JSON are left out, so that
    the list stays small however many files there are.

    Args:
        outputs: The task's outputs, uploaded.

    Returns:
        For each file, in the order of the outputs, of the wildcard matches
        by path, and of the files of a directory by name before those of
        its subdirectories: `output`, the index of the output; `relative`,
        the file's path below the output's (empty for a file output) or,
        for wildcards, below its `path_prefix`; and `size_bytes`, its size
        (a string, as in TES).
    """
    logs: list[dict[str, Any]] = []
    size = len("[]")
    for index, output in enumerate(outputs):
        for relative, file_size in output_files(output):
            entry = {
                "output": index,
                "relative": relative,
                "size_bytes": str(file_size),
            }
            size += len(json.dumps(entry, separators=(",", ":"))) + 1
            if size > OUTPUT_LOGS_MAX_BYTES:
                logging.warning(
                    "Reporting only the first %d uploaded files: the others exceed "
                    "%d bytes",
                    len(logs),
                    OUTPUT_LOGS_MAX_BYTES,
                )
                return logs
            logs.append(entry)
    return logs


def output_files(filedata: dict[str, Any]) -> Iterator[tuple[str, int]]:
    """Find the uploaded files of an output.

    Args:
        filedata: The output.

    Yields:
        For each file (see `output_file_logs`), its path below the output's
        (below `path_prefix` for wildcards; empty for a file output) and its
        size.
    """
    if filedata.get("path_prefix") and hasWildcards(filedata["path"]):
        prefix = filerPath(taskPath(filedata["path_prefix"]))
        for match in sorted(glob(filerPath(filedata["path"]))):
            relative = os.path.relpath(match, prefix).replace(os.sep, "/")
            for below, size in files_below(match):
                yield (relative + "/" + below if below else relative), size
    else:
        yield from files_below(filerPath(filedata["path"]))


def files_below(local: str) -> Iterator[tuple[str, int]]:
    """Find a file, or every file below a directory.

    Args:
        local: The file or directory, in the filer.

    Yields:
        The path below `local` (empty for a file) and the size of each
        regular file; none for a symbolic link or a path that doesn't exist.
    """
    if os.path.islink(local):
        return
    if os.path.isfile(local):
        yield "", os.path.getsize(local)
        return
    for root, dirs, files in os.walk(local):
        # Files by name, then the subdirectories by name, depth first
        dirs.sort()
        for name in sorted(files):
            full = os.path.join(root, name)
            if not os.path.islink(full) and os.path.isfile(full):
                relative = os.path.relpath(full, local).replace(os.sep, "/")
                yield relative, os.path.getsize(full)


def output_file_url(output: dict[str, Any], relative: str) -> tuple[str, str]:
    """Get the URL and the path in the executors of an uploaded file.

    As the upload derives them (see `url_join` and `upload_matches`).

    Args:
        output: The output.
        relative: The file's path below the output's, as in
            `output_file_logs`.

    Returns:
        The file's URL and its path in the executors.
    """
    if output.get("path_prefix") and hasWildcards(output["path"]):
        base = taskPath(output["path_prefix"])
    else:
        base = taskPath(output["path"])
    if not relative:
        return output["url"], base
    return url_join(output["url"], relative), base.rstrip("/") + "/" + relative


def logConfig(loglevel: int) -> None:
    """Configure logging to standard output.

    Args:
        loglevel: The log level.
    """
    configure_logging(loglevel, sys.stdout)


def main() -> int:
    """Run the filer on the command line.

    Returns:
        The exit code: 0 on success, 1 if an input or output failed.
    """
    parser = argparse.ArgumentParser(
        description="Filer script for down- and uploading files"
    )
    parser.add_argument(
        "transputtype", help="transput to handle, either 'inputs' or 'outputs' "
    )
    parser.add_argument("data", help="file description data, see docs for structure")
    parser.add_argument("--debug", "-d", help="debug logging", action="store_true")
    args = parser.parse_args()

    loglevel = logging.DEBUG if args.debug else logging.ERROR

    logConfig(loglevel)

    logging.info("Starting %s filer...", args.transputtype)

    if args.data.endswith(".gz"):
        with gzip.open(args.data, "rb") as fh:
            data = json.loads(fh.read())
    else:
        data = json.loads(args.data)

    for afile in data[args.transputtype]:
        logging.debug("Processing file: %s", afile["path"])
        if process_file(args.transputtype, afile):
            logging.error("Unable to process file, aborting")
            return 1
        logging.debug("Processed file: %s", afile["path"])

    if args.transputtype == "outputs":
        print_output_logs(data["outputs"])
    return 0


def print_output_logs(outputs: list[dict[str, Any]]) -> None:
    """Print the uploaded files for the taskmaster, after `OUTPUT_LOGS_MARKER`.

    Whatever the log level, as the taskmaster reads them from the pod's
    log. The outputs are uploaded: an error here is logged and prints
    nothing, rather than failing the filer.

    Args:
        outputs: The task's outputs, uploaded.
    """
    try:
        logs = json.dumps(output_file_logs(outputs), separators=(",", ":"))
    except Exception as err:  # the outputs are uploaded
        logging.warning("Can't list the uploaded files: %s", err)
        return
    print(OUTPUT_LOGS_MARKER, logs, flush=True)


if __name__ == "__main__":
    sys.exit(main())
