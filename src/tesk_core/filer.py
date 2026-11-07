#!/usr/bin/env python3
"""The filer: stages the inputs and outputs of a task.

Run as `filer {inputs|outputs} DATA`, with the task as JSON, or a gzipped
JSON file (`*.gz`).
"""

import argparse
from collections.abc import Callable
import ftplib
from ftplib import FTP
from glob import glob
import gzip
import json
import logging
import netrc
import os
import re
import shutil
import sys
from typing import Any
from urllib.parse import quote, urlparse, urlsplit, urlunsplit

import requests

from tesk_core.exception import FileProtocolDisabled, UnknownProtocol
from tesk_core.filer_s3 import S3Transput
from tesk_core.path import containerPath, fileEnabled, getPath
from tesk_core.transput import Transput, Type, url_for_log


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


class HTTPTransput(Transput):
    """Transfer of an input or output over HTTP(S)."""

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
        """Download the file with `GET`.

        Returns:
            0 on success, otherwise 1.
        """
        try:
            # No timeout, as before; transfers of large files can take long
            req = requests.get(self.url)  # noqa: S113 -- see above
        except requests.exceptions.RequestException as err:
            return self._request_failed(err)

        if req.status_code < 200 or req.status_code >= 300:
            logging.error("Got status code: %d", req.status_code)
            logging.error(req.text)
            return 1
        logging.debug("OK, got status code: %d", req.status_code)

        with open(self.path, "wb") as file:
            file.write(req.content)
        return 0

    def upload_file(self) -> int:
        """Upload the file with `PUT`, as bytes.

        Returns:
            0 on success, otherwise 1.
        """
        # Send the file as bytes, streamed from disk: decoding it as text fails
        # for binary files and changes some text. An empty file is sent as b''
        # because requests sends an empty file object with chunked encoding,
        # which presigned upload URLs such as S3's reject.
        with open(self.path, "rb") as file:
            data = file if os.fstat(file.fileno()).st_size > 0 else b""
            try:
                # No timeout, as before; uploads of large files can take long
                req = requests.put(self.url, data=data)  # noqa: S113 -- see above
            except requests.exceptions.RequestException as err:
                return self._request_failed(err)

        if req.status_code < 200 or req.status_code >= 300:
            logging.error("Got status code: %d", req.status_code)
            logging.error(req.text)
            return 1
        logging.debug("OK, got status code: %d", req.status_code)

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
            if transput.upload():
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


def copyContent(
    src: str,
    dst: str,
    symlinks: bool = False,
    ignore: Callable[[str, list[str]], list[str]] | None = None,
) -> None:
    """Copy the content of a directory into an existing directory.

    See https://stackoverflow.com/a/12514470/1553043.

    Args:
        src: The source directory.
        dst: The destination directory.
        symlinks: Whether to copy symbolic links as links.
        ignore: Selects the names to leave out (as for `shutil.copytree`).
    """
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def copyDir(src: str, dst: str) -> None:
    """Copy a directory, also into an existing one.

    `shutil.copytree` needs a destination that doesn't exist yet.

    Args:
        src: The source directory.
        dst: The destination directory.
    """
    if os.path.exists(dst):
        copyContent(src, dst)

    else:
        shutil.copytree(src, dst)


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
            0: errors are raised.
        """
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

        Returns:
            The transfer itself.
        """
        if self.connection_owner:
            self.ftp_connection.connect(self.netloc)
            ftp_login(self.ftp_connection, self.netloc, self.netrc_file)
        return self

    def upload_dir(self) -> int:
        """Upload the directory and everything below it, recursively.

        Creates the remote directory and its parents, so that empty
        directories are kept.

        Returns:
            0 on success, otherwise 1.
        """
        if ftp_make_dirs(self.ftp_connection, self.url_path):
            logging.error("Unable to create remote directory %s", url_for_log(self.url))
            return 1

        for file in os.listdir(self.path):
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

            # Recurse into new transfers, ending with files, which are uploaded
            with FTPTransput(file_path, file_url, ftype) as transfer:
                if transfer.upload():
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
        os.makedirs(self.path, exist_ok=True)
        self.ftp_connection.cwd(self.url_path)

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


def ftp_login(ftp_connection: FTP, netloc: str, netrc_file: netrc.netrc | None) -> None:
    """Log in to an FTP server.

    With the credentials for the host in `.netrc`, if it was read;
    otherwise with `TESK_FTP_USERNAME` and `TESK_FTP_PASSWORD`, if both are
    set. Without credentials, or if they are refused, anonymously.

    Args:
        ftp_connection: The connection.
        netloc: The host (and port) of the server.
        netrc_file: The `.netrc` file, if it was read.
    """
    user = None
    password: str | None = None
    if netrc_file is not None:
        creds = netrc_file.authenticators(netloc)
        if creds:
            user, _, password = creds
    elif "TESK_FTP_USERNAME" in os.environ and "TESK_FTP_PASSWORD" in os.environ:
        user = os.environ["TESK_FTP_USERNAME"]
        password = os.environ["TESK_FTP_PASSWORD"]

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
        with open(local_source_path, "r+b") as file:
            ftp_connection.storbinary("STOR /" + remote_destination_path, file)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
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


def process_file(ttype: str, filedata: dict[str, Any]) -> int:
    """Stage an input or output.

    Args:
        ttype: `inputs` or `outputs`.
        filedata: The input or output.

    Returns:
        0 on success, otherwise 1.
    """
    if "content" in filedata:
        return file_from_content(filedata)
    parsed_url = urlparse(filedata["url"])
    scheme = parsed_url.scheme
    netloc = parsed_url.netloc
    if scheme == "":
        logging.info(
            'Could not determine protocol for url: "%s", assuming "file"',
            url_for_log(filedata["url"]),
        )
        scheme = "file"

    trans = newTransput(scheme, netloc)

    with trans(filedata["path"], filedata["url"], Type(filedata["type"])) as transfer:
        if ttype == "inputs":
            return transfer.download()
        if ttype == "outputs":
            return transfer.upload()

    logging.info("There was no action to do with %s", filedata["path"])
    return 0


def logConfig(loglevel: int) -> None:
    """Configure logging to standard output.

    Args:
        loglevel: The log level.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
        level=loglevel,
        stream=sys.stdout,
    )


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

    return 0


if __name__ == "__main__":
    sys.exit(main())
