"""Base class of the filer's transfers."""

import enum
import logging
import netrc
import os
import re
from types import TracebackType
from typing import Literal
from urllib.parse import unquote_plus, urlparse, urlsplit, urlunsplit


def url_for_log(url: str) -> str:
    """Strip a URL for log messages.

    Leaves out the query string (e.g., the parameters of a presigned URL)
    and the user info.

    Args:
        url: The URL.

    Returns:
        The URL without query string and user info.
    """
    parts = urlsplit(url)
    netloc = parts.netloc.rpartition("@")[2]
    return urlunsplit(parts._replace(netloc=netloc, query=""))


# URLs in free text: a scheme, `://`, and anything up to whitespace, quotes
# or angle brackets
URL_IN_TEXT = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"'<>]+")
# Characters of HTTP error bodies logged at most
BODY_LOG_LIMIT = 1000
# Query values shorter than this are not redacted from bodies: replacing,
# e.g., every "1" would garble the body and hide nothing
MIN_SECRET_LENGTH = 4


def redact_urls(text: str) -> str:
    """Strip the URLs in a text for log messages (see `url_for_log`).

    Args:
        text: The text, e.g., a job manifest or a response body.

    Returns:
        The text with each URL stripped; `<URL>` for URLs that can't be
        parsed.
    """

    def strip(match: re.Match[str]) -> str:
        """Strip one URL.

        Args:
            match: The URL.

        Returns:
            The stripped URL, or `<URL>`.
        """
        try:
            return url_for_log(match.group(0))
        except ValueError:
            return "<URL>"

    return URL_IN_TEXT.sub(strip, text)


def body_for_log(text: str, url: str, limit: int = BODY_LOG_LIMIT) -> str:
    """Prepare the body of an HTTP response for log messages.

    Strips the URLs in the body (see `redact_urls`), redacts the query
    values and user information of the request's URL wherever else they
    appear (e.g., a signature that an error repeats), and cuts the body at
    `limit`.

    Args:
        text: The body.
        url: The URL of the request.
        limit: The number of characters kept.

    Returns:
        The body for log messages.
    """
    text = redact_urls(text)
    parts = urlsplit(url)
    raw = [parts.username or "", parts.password or ""]
    raw += [pair.partition("=")[2] for pair in parts.query.split("&")]
    secrets = set(raw) | {unquote_plus(value) for value in raw}
    # Also as JSON with escaped slashes, as some servers write it
    secrets |= {value.replace("/", "\\/") for value in secrets}
    for secret in sorted(secrets, key=len, reverse=True):
        if len(secret) >= MIN_SECRET_LENGTH:
            text = text.replace(secret, "<redacted>")
    if len(text) > limit:
        return f"{text[:limit]}... ({len(text) - limit} more characters)"
    return text


@enum.unique
class Type(enum.Enum):
    """Type of an input or output of a task."""

    File = "FILE"
    Directory = "DIRECTORY"


class Transput:
    """Transfer of an input or output of a task.

    Subclasses implement the transfers of files and directories for a
    protocol. Used as a context manager, `delete` is called on exit.

    Attributes:
        path: Path in the filer's container.
        url: URL of the input or output.
        ftype: Whether it is a file or a directory.
        netloc: Network location of the URL.
        url_path: Path of the URL.
        netrc_file: Credentials from `$HOME/.netrc` (or `/.netrc`), if it
            can be read.
    """

    def __init__(self, path: str, url: str, ftype: Type) -> None:
        """Describe the transfer and read the `.netrc` file.

        Args:
            path: Path in the filer's container.
            url: URL of the input or output.
            ftype: Whether it is a file or a directory.
        """
        self.path = path
        self.url = url
        self.ftype = ftype

        parsed_url = urlparse(url)
        self.netloc = parsed_url.netloc
        self.url_path = parsed_url.path
        self.netrc_file: netrc.netrc | None = None
        try:
            netrc_path = os.path.join(os.environ["HOME"], ".netrc")
        except KeyError:
            netrc_path = "/.netrc"
        try:
            self.netrc_file = netrc.netrc(netrc_path)
        except OSError as fnfe:
            logging.error(fnfe)
        except netrc.NetrcParseError as err:
            logging.error("netrc.NetrcParseError")
            logging.error(err)
        # Any other error leaves the filer without `.netrc` credentials
        except Exception as er:
            logging.error(er)

    def upload(self) -> int:
        """Upload the file or directory.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            "%s uploading %s %s",
            self.__class__.__name__,
            self.ftype,
            url_for_log(self.url),
        )
        if self.ftype == Type.File:
            return self.upload_file()
        if self.ftype == Type.Directory:
            return self.upload_dir()
        return 1

    def download(self) -> int:
        """Download the file or directory.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            "%s downloading %s %s",
            self.__class__.__name__,
            self.ftype,
            url_for_log(self.url),
        )
        if self.ftype == Type.File:
            return self.download_file()
        if self.ftype == Type.Directory:
            return self.download_dir()
        return 1

    def delete(self) -> None:
        """Clean up after the transfer (e.g., close connections)."""

    def download_file(self) -> int:
        """Download a file.

        Returns:
            0 on success, otherwise 1.
        """
        raise NotImplementedError()

    def download_dir(self) -> int:
        """Download a directory.

        Returns:
            0 on success, otherwise 1.
        """
        raise NotImplementedError()

    def upload_file(self) -> int:
        """Upload a file.

        Returns:
            0 on success, otherwise 1.
        """
        raise NotImplementedError()

    def upload_dir(self) -> int:
        """Upload a directory.

        Returns:
            0 on success, otherwise 1.
        """
        raise NotImplementedError()

    def __enter__(self) -> "Transput":
        """Use the transfer as a context manager.

        Returns:
            The transfer itself.
        """
        return self

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        """Clean up after the transfer (`delete`).

        Args:
            error_type: Type of the exception raised in the context, if any.
            error_value: The exception raised in the context, if any.
            traceback: Its traceback.

        Returns:
            `False`: exceptions are not swallowed.
        """
        self.delete()
        return False
