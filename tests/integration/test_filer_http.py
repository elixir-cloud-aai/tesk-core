"""Integration tests of the filer's HTTP uploads, against a local HTTP server."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import threading
from urllib.parse import unquote, unquote_to_bytes, urlsplit

import pytest

from tesk_core.filer import HTTPTransput, Type


@pytest.fixture
def put_server():
    """A local HTTP server that records each PUT request it receives."""

    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            length = int(self.headers.get("Content-Length", 0))
            received.append((self.path, self.headers, self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield "http://127.0.0.1:{}".format(server.server_port), received
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize(
    "content",
    [
        b"plain text\n",
        b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xff",  # start of a gzip file
        "caf\u00e9\n".encode("utf-8"),
        "x \u2265 1\n".encode("utf-8"),
        b"first line\r\nsecond line\r\n",
        b"",
    ],
    ids=["ascii", "gzip", "utf8-accent", "utf8-symbol", "crlf", "empty"],
)
def test_upload_file_sends_exact_bytes(put_server, tmp_path, content):
    """Ensure a file is uploaded byte for byte, with a Content-Length and
    without chunked encoding, as presigned upload URLs require."""

    url, received = put_server
    path = tmp_path / "output"
    path.write_bytes(content)

    assert HTTPTransput(str(path), url + "/output", Type.File).upload_file() == 0

    [(_, headers, body)] = received
    assert body == content
    assert headers["Content-Length"] == str(len(content))
    assert "Transfer-Encoding" not in headers


def test_upload_dir_keeps_file_names(put_server, tmp_path):
    """Ensure each file of a directory reaches the server under its own
    name, whatever characters the name contains, and that the query of the
    directory's URL, which may carry a token, is kept."""

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


def test_upload_dir_keeps_non_utf8_file_names(put_server, tmp_path):
    """Ensure a file whose name isn't valid UTF-8, which file systems such
    as ext4 allow, is uploaded under the bytes of its name rather than
    stopping the directory upload."""

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
