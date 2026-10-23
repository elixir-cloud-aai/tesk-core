"""Tests for 'filer.py' HTTP functionalities using 'pytest'."""

from http.server import BaseHTTPRequestHandler, HTTPServer
from requests import Response, put
import os
import threading
from unittest import mock
from urllib.parse import unquote, urlsplit

import pytest

from tesk_core.filer import (
    HTTPTransput,
    Type,
    http_join
)

PATH_DOWN = 'test_download_file.txt'
PATH_UP = 'tests/test_filer_http_pytest.py'
SUCCESS = 200
FAIL = 300
URL = 'http://www.foo.bar'
FTYPE = 'FILE'

resp = Response()
resp._content = b'{ "foo" : "bar" }'


@pytest.fixture
def put_server():
    """A local HTTP server that records each PUT request it receives."""

    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            length = int(self.headers.get('Content-Length', 0))
            received.append((self.path, self.headers, self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield 'http://127.0.0.1:{}'.format(server.server_port), received
    server.shutdown()
    server.server_close()


def test_download_file(mocker):
    """ Ensure a file gets properly downloaded."""

    resp.status_code = SUCCESS
    http_obj = HTTPTransput(PATH_DOWN, URL, FTYPE)
    mocker.patch('requests.get', return_value=resp)

    with mock.patch(
            'builtins.open',
            mock.mock_open(read_data=resp._content),
            create=False
    ) as m:
        assert 0 == http_obj.download_file()
        assert open(PATH_DOWN, 'rb').read() == resp._content


def test_download_file_error(mocker, caplog):
    """ Ensure download error returns the correct value and log message."""

    resp.status_code = FAIL
    http_obj = HTTPTransput(PATH_DOWN, URL, FTYPE)
    mocker.patch('requests.get', return_value=resp)

    assert 1 == http_obj.download_file()
    assert 'Got status code: {}'.format(FAIL) in caplog.text


def test_upload_file(mocker):
    """ Ensure a file gets properly uploaded."""

    resp.status_code = SUCCESS
    http_obj = HTTPTransput(PATH_UP, URL, FTYPE)
    mocker.patch('requests.put', return_value=resp)

    assert 0 == http_obj.upload_file()


@pytest.mark.parametrize('content', [
    b'plain text\n',
    b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xff',  # start of a gzip file
    'caf\u00e9\n'.encode('utf-8'),
    'x \u2265 1\n'.encode('utf-8'),
    b'first line\r\nsecond line\r\n',
    b'',
], ids=['ascii', 'gzip', 'utf8-accent', 'utf8-symbol', 'crlf', 'empty'])
def test_upload_file_sends_exact_bytes(put_server, tmp_path, content):
    """ Ensure a file is uploaded byte for byte, with a Content-Length and
    without chunked encoding, as presigned upload URLs require."""

    url, received = put_server
    path = tmp_path / 'output'
    path.write_bytes(content)

    assert HTTPTransput(str(path), url + '/output', Type.File).upload_file() == 0

    [(_, headers, body)] = received
    assert body == content
    assert headers['Content-Length'] == str(len(content))
    assert 'Transfer-Encoding' not in headers


def test_upload_file_error(mocker, caplog):
    """ Ensure upload error returns the correct value and log message."""

    resp.status_code = FAIL
    http_obj = HTTPTransput(PATH_UP, URL, FTYPE)
    mocker.patch('requests.put', return_value=resp)

    assert 1 == http_obj.upload_file()
    assert 'Got status code: {}'.format(FAIL) in caplog.text


def test_upload_dir(mocker, fs):
    """ Ensure that each file inside nexted directories gets successfully
    uploaded."""
    
    # Tele2 Speedtest Service, free upload /download test server
    endpoint = "http://speedtest.tele2.net/upload.php"
    resp.status_code = 200

    fs.create_dir('dir1')
    fs.create_dir('dir1/dir2')
    fs.create_file('dir1/file1', contents="this is random")
    fs.create_file('dir1/dir2/file2', contents="not really")
    fs.create_file('dir1/dir2/file4.txt', contents="took me a while")


    # Each file is sent as an open file object, so its content is read
    # during the call.
    uploads = {}

    def mock_put(url, data):
        uploads[url] = data if isinstance(data, bytes) else data.read()
        return resp

    mocker.patch('requests.put', side_effect=mock_put)

    http_obj = HTTPTransput(
        "dir1",
        endpoint + "/dir1",
        Type.Directory
    )

    assert http_obj.upload_dir() == 0

    assert uploads == {
        endpoint + '/dir1/dir2/file2': b"not really",
        endpoint + '/dir1/dir2/file4.txt': b"took me a while",
        endpoint + '/dir1/file1': b"this is random",
    }


    def test_upload_dir_error(mocker, fs):
        """ Ensure 'upload_dir' error returns the correct value. """

        fs.create_dir('dir2')

        # Tele2 Speedtest Service, free upload /download test server
        endpoint1 = "http://speedtest.tele2.net/upload.php"

        # Non-existent endpoint
        endpoint2 = "http://somerandomendpoint.fail"

        http_obj1 = HTTPTransput(
            "dir1",
            endpoint1 + "/dir1",
            Type.Directory
        )

        http_obj2 = HTTPTransput(
            "dir2",
            endpoint2 + "/dir1",
            Type.Directory
        )    

        assert http_obj1.upload_dir() == 1
        assert http_obj2.upload_dir() == 1


@pytest.mark.parametrize('url, name, expected', [
    ('http://foo.bar/dir', 'file.txt', 'http://foo.bar/dir/file.txt'),
    ('http://foo.bar/dir/', 'file.txt', 'http://foo.bar/dir/file.txt'),
    ('http://foo.bar/dir', 'result#1.txt', 'http://foo.bar/dir/result%231.txt'),
    ('http://foo.bar/dir', 'a?b.txt', 'http://foo.bar/dir/a%3Fb.txt'),
    ('http://foo.bar/dir', '50%25.txt', 'http://foo.bar/dir/50%2525.txt'),
    ('http://foo.bar/dir?token=a%2Bb', 'file.txt',
     'http://foo.bar/dir/file.txt?token=a%2Bb'),
])
def test_http_join(url, name, expected):
    """ Ensure a file name is escaped and the URL's query is kept."""

    assert http_join(url, name) == expected


def test_upload_dir_keeps_file_names(put_server, tmp_path):
    """ Ensure each file of a directory reaches the server under its own
    name, whatever characters the name contains, and that the query of the
    directory's URL, which may carry a token, is kept."""

    url, received = put_server
    names = ['result#1.txt', 'a?b.txt', '50%25.txt', '100%.txt', 'a b.txt']
    for name in names:
        (tmp_path / name).write_bytes(b'data')
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'y.txt').write_bytes(b'data')

    http_obj = HTTPTransput(
        str(tmp_path),
        url + '/upload/dir?token=abc',
        Type.Directory
    )

    assert http_obj.upload_dir() == 0

    requested = sorted(
        (unquote(urlsplit(path).path), urlsplit(path).query)
        for path, _, _ in received
    )
    assert requested == sorted(
        [('/upload/dir/' + name, 'token=abc') for name in names] +
        [('/upload/dir/sub/y.txt', 'token=abc')]
    )
