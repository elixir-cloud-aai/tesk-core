"""Integration tests of the filer's HTTP uploads, against a local HTTP server."""

from collections.abc import Iterator
from email.message import Message
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pathlib import Path
import threading
from typing import Any
from urllib.parse import unquote, unquote_to_bytes, urlsplit

import pytest

from tesk_core.filer import HTTPTransput, Type

Received = list[tuple[str, Message, bytes]]


@pytest.fixture
def put_server() -> Iterator[tuple[str, Received]]:
    """Run a local HTTP server that records each PUT request it receives.

    Yields:
        The base URL of the server and the list of the received requests,
        each as path, headers and body.
    """
    received: Received = []

    class Handler(BaseHTTPRequestHandler):
        """Request handler that records PUT requests."""

        def do_PUT(self) -> None:  # name fixed by the base class
            """Record the request and answer with status 200."""
            length = int(self.headers.get("Content-Length", 0))
            received.append((self.path, self.headers, self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            """Keep the output of the tests clean.

            Args:
                format: Unused.
                *args: Unused.
            """

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", received
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize(
    "content",
    [
        b"plain text\n",
        b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xff",  # start of a gzip file
        "café\n".encode(),
        "x ≥ 1\n".encode(),
        b"first line\r\nsecond line\r\n",
        b"",
    ],
    ids=["ascii", "gzip", "utf8-accent", "utf8-symbol", "crlf", "empty"],
)
def test_upload_file_sends_exact_bytes(
    put_server: tuple[str, Received], tmp_path: Path, content: bytes
) -> None:
    """Test that a file is uploaded byte for byte.

    The upload has a Content-Length and no chunked encoding, as presigned
    upload URLs require.
    """
    url, received = put_server
    path = tmp_path / "output"
    path.write_bytes(content)

    assert HTTPTransput(str(path), url + "/output", Type.File).upload_file() == 0

    [(_, headers, body)] = received
    assert body == content
    assert headers["Content-Length"] == str(len(content))
    assert "Transfer-Encoding" not in headers


def test_upload_dir_keeps_file_names(
    put_server: tuple[str, Received], tmp_path: Path
) -> None:
    """Test that each file of a directory is uploaded under its own name.

    Whatever characters the name contains, each file reaches the server
    under its name, and the query of the directory's URL, which may carry a
    token, is kept.
    """
    url, received = put_server
    names = ["result#1.txt", "a?b.txt", "50%25.txt", "100%.txt", "a b.txt"]
    for name in names:
        (tmp_path / name).write_bytes(b"data")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "y.txt").write_bytes(b"data")

    http_obj = HTTPTransput(
        str(tmp_path), url + "/upload/dir?token=abc", Type.Directory
    )

    assert http_obj.upload_dir() == 0

    requested = sorted(
        (unquote(urlsplit(path).path), urlsplit(path).query) for path, _, _ in received
    )
    assert requested == sorted(
        [("/upload/dir/" + name, "token=abc") for name in names]
        + [("/upload/dir/sub/y.txt", "token=abc")]
    )


def test_upload_dir_keeps_non_utf8_file_names(
    put_server: tuple[str, Received], tmp_path: Path
) -> None:
    """Test that a file whose name is not valid UTF-8 is still uploaded.

    File systems such as ext4 allow such names. The file is uploaded under
    the bytes of its name rather than stopping the directory upload.
    """
    url, received = put_server
    try:
        with open(os.path.join(os.fsencode(tmp_path), b"caf\xe9.txt"), "wb") as f:
            f.write(b"data")
    except OSError:
        pytest.skip("the file system rejects names that are not valid UTF-8")
    (tmp_path / "ok.txt").write_bytes(b"data")

    http_obj = HTTPTransput(str(tmp_path), url + "/upload/dir", Type.Directory)

    assert http_obj.upload_dir() == 0
    assert sorted(unquote_to_bytes(path) for path, _, _ in received) == [
        b"/upload/dir/caf\xe9.txt",
        b"/upload/dir/ok.txt",
    ]
