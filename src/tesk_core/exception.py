"""Exceptions of the filer."""


class UnknownProtocol(Exception):
    """The scheme of a URL is not supported by the filer."""


class FileProtocolDisabled(Exception):
    """`file:` URLs are used, but no shared volume is configured."""


class InvalidHostPath(Exception):
    """A path is not below the base path of the shared volume."""
