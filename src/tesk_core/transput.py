"""Base class of the filer's transfers."""

from collections.abc import Iterable
import enum
import logging
import netrc
import os
import re
from types import TracebackType
from typing import BinaryIO, ClassVar, Literal
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


def body_for_log(
    text: str,
    *urls: str,
    secrets: Iterable[str] = (),
    limit: int = BODY_LOG_LIMIT,
) -> str:
    """Prepare the body of an HTTP response for log messages.

    Strips the URLs in the body (see `redact_urls`), redacts the query
    values and user information of the request's URLs, and other secrets
    (e.g., credentials from `.netrc`), wherever else they appear (e.g., a
    signature that an error repeats), and cuts the body at `limit`. Values
    shorter than `MIN_SECRET_LENGTH` are kept.

    Args:
        text: The body.
        *urls: The URLs of the request: as given, as sent (normalized), and
            those of redirects.
        secrets: More values to redact.
        limit: The number of characters kept.

    Returns:
        The body for log messages.
    """
    text = redact_urls(text)
    raw = list(secrets)
    for url in urls:
        try:
            parts = urlsplit(url)
            raw += [parts.username or "", parts.password or ""]
        except ValueError:
            parts = urlsplit("")
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


def within(parent: str, path: str) -> bool:
    """Tell whether a path is below a directory, after resolving both.

    Symbolic links and `..` segments are resolved (`os.path.realpath`), so
    that the check holds for the path that is written.

    Args:
        parent: The directory.
        path: The path, which needn't exist.

    Returns:
        Whether `path` is below `parent` (not `parent` itself).
    """
    parent_real = os.path.realpath(parent)
    return os.path.realpath(path).startswith(parent_real.rstrip(os.sep) + os.sep)


def symlink_in(path: str, recursive: bool = False) -> str | None:
    """Find a symbolic link in a path, or below it.

    Checks each component of the path as written (relative paths below the
    working directory), and, if `recursive`, every file and directory below
    it (without following links). Paths that don't exist contain no link.

    Args:
        path: The path, without `..` components: the system resolves them
            after the links before them, so the path can't be checked
            component by component.
        recursive: Whether to check the entries below a directory, too.

    Returns:
        The first symbolic link found, or `None`.

    Raises:
        ValueError: The path has a `..` component.
    """
    parts = os.path.join(os.getcwd(), path).split(os.sep)
    if ".." in parts:
        raise ValueError(f"Path with '..' component: {path}")
    current = os.sep
    for part in parts:
        if part in {"", "."}:
            continue
        current = os.path.join(current, part)
        if os.path.islink(current):
            return current
    if recursive and os.path.isdir(path):
        for root, dirs, files in os.walk(path, followlinks=False):
            for name in sorted(dirs + files):
                entry = os.path.join(root, name)
                if os.path.islink(entry):
                    return entry
    return None


def output_has_symlink(path: str, recursive: bool = False) -> bool:
    """Check that an output is no symbolic link and contains none; log if it does.

    Paths with `..` components are refused too (see `symlink_in`).

    Args:
        path: The path of the output.
        recursive: Whether to check the entries below a directory, too.

    Returns:
        Whether a symbolic link or a `..` component was found (and logged).
    """
    try:
        link = symlink_in(path, recursive)
    except ValueError:
        logging.error("Output %s has a '..' component: outputs can't have any", path)
        return True
    if link is None:
        return False
    logging.error(
        "Output %s is or contains a symbolic link, %s: outputs can't contain "
        "symbolic links",
        path,
        link,
    )
    return True


def unsafe_entry(path: str, chars: str) -> str | None:
    """Find an entry below a directory whose name has any of some characters.

    Walks the directory top-down, without following links, and each
    directory's entries by name.

    Args:
        path: The directory.
        chars: The characters; none (empty) finds nothing.

    Returns:
        The path of the first such file or directory, or `None` (also if
        the directory doesn't exist).
    """
    if not chars:
        return None
    for root, dirs, files in os.walk(path, followlinks=False):
        dirs.sort()
        for name in sorted(dirs + files):
            if any(char in name for char in chars):
                return os.path.join(root, name)
    return None


def output_has_unsafe_entry(path: str, chars: str) -> bool:
    """Check a directory output for entries with some characters; log one.

    Args:
        path: The directory output.
        chars: The characters its entries' names can't have.

    Returns:
        Whether such an entry was found (and logged).
    """
    entry = unsafe_entry(path, chars)
    if entry is None:
        return False
    logging.error(
        "Output %s contains %s, whose name can't be part of its URL: names "
        "with any of %r are refused for this protocol",
        path,
        entry,
        chars,
    )
    return True


def open_for_upload(path: str) -> BinaryIO:
    """Open a file for reading, refusing a symbolic link.

    Args:
        path: The file.

    Returns:
        The open file, in binary mode.

    Raises:
        OSError: The file can't be opened, or is a symbolic link.
    """
    return os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb")


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
        url_unsafe_chars: Characters that the names of entries below a
            directory output can't have: the protocol's directory uploads
            append each entry's name to the directory's URL, and take the
            URL's path as it is (`urlparse`), which ends at them. None by
            default (protocols that escape the names, or don't build URLs
            per entry).
    """

    url_unsafe_chars: ClassVar[str] = ""

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
        except FileNotFoundError:
            # The default: no `.netrc` mounted
            logging.debug("No .netrc at %s", netrc_path)
        except OSError as err:
            logging.error(err)
        except netrc.NetrcParseError as err:
            logging.error("netrc.NetrcParseError")
            logging.error(err)
        # Any other error leaves the filer without `.netrc` credentials
        except Exception as er:
            logging.error(er)

    def upload(self, check_links: bool = True) -> int:
        """Upload the file or directory.

        Outputs aren't uploaded if their path, or anything below a
        directory, is a symbolic link: links would be resolved in the
        filer's file system, not in the executor's. Nor are directories
        with an entry whose name has one of `url_unsafe_chars`, which its
        URL would lose (see `output_has_unsafe_entry`).

        Args:
            check_links: Whether to check for links and names; the entries
                of a directory skip it, as the check of the directory covers
                them, and so do the matches of outputs with wildcards,
                checked before any of them is uploaded.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            "%s uploading %s %s",
            self.__class__.__name__,
            self.ftype,
            url_for_log(self.url),
        )
        if check_links and output_has_symlink(
            self.path, recursive=self.ftype == Type.Directory
        ):
            return 1
        if (
            check_links
            and self.ftype == Type.Directory
            and output_has_unsafe_entry(self.path, self.url_unsafe_chars)
        ):
            return 1
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
