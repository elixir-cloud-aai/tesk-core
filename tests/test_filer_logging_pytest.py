"""Tests that the filer's log messages leave out the query strings of URLs."""

import logging

import pytest
from requests import Response

from tesk_core.filer import FTPTransput, HTTPTransput, Type, process_file
from tesk_core.transput import url_for_log

QUERY = 'X-Amz-Signature=abc123'
URL = 'https://example.org/bucket/a.txt?' + QUERY


@pytest.mark.parametrize('url, expected', [
    (URL, 'https://example.org/bucket/a.txt'),
    ('https://example.org/a?b=c#d', 'https://example.org/a#d'),
    ('https://example.org/a', 'https://example.org/a'),
    ('ftp://example.org/dir/', 'ftp://example.org/dir/'),
    ('/tmp/a?b', '/tmp/a'),
    ('', ''),
])
def test_url_for_log(url, expected):
    """The query string is removed, everything else is kept."""

    assert url_for_log(url) == expected


def _response(status):
    """A `requests` response with the given status and no content."""

    resp = Response()
    resp.status_code = status
    resp._content = b''
    return resp


def test_http_download_logs_no_query(mocker, tmp_path, caplog):
    """Downloads log the URL without its query string."""

    mocker.patch('requests.get', return_value=_response(200))
    caplog.set_level(logging.DEBUG)
    HTTPTransput(str(tmp_path / 'a.txt'), URL, Type.File).download()

    assert 'https://example.org/bucket/a.txt' in caplog.text
    assert QUERY not in caplog.text


def test_http_upload_dir_logs_no_query(mocker, tmp_path, caplog):
    """Uploads, also of the files of a directory, log URLs without their
    query strings."""

    (tmp_path / 'f.txt').write_text('x')
    mocker.patch('requests.put', return_value=_response(200))
    caplog.set_level(logging.DEBUG)
    HTTPTransput(str(tmp_path), URL, Type.Directory).upload()

    assert 'https://example.org/bucket/a.txt/f.txt' in caplog.text
    assert QUERY not in caplog.text


def test_http_download_dir_logs_no_query(tmp_path, caplog):
    """The error for HTTP directory inputs logs the URL without its query
    string."""

    caplog.set_level(logging.DEBUG)
    assert HTTPTransput(str(tmp_path), URL, Type.Directory).download() == 1

    assert 'https://example.org/bucket/a.txt' in caplog.text
    assert QUERY not in caplog.text


def test_ftp_logs_no_query(mocker, tmp_path, caplog):
    """FTP transfers log URLs without their query strings."""

    conn = mocker.MagicMock()
    conn.pwd.return_value = '/'
    caplog.set_level(logging.DEBUG)
    url = 'ftp://example.org/dir?' + QUERY
    with FTPTransput(str(tmp_path / 'a'), url, Type.File, conn) as transput:
        transput.download()
    mocker.patch('tesk_core.filer.ftp_make_dirs', return_value=1)
    with FTPTransput(str(tmp_path / 'a'), url, Type.File, conn) as transput:
        transput.upload()

    assert 'ftp://example.org/dir' in caplog.text
    assert QUERY not in caplog.text


def test_process_file_no_scheme_logs_no_query(mocker, caplog):
    """The message for URLs without a scheme leaves out the query string."""

    transput = mocker.MagicMock()
    mocker.patch('tesk_core.filer.newTransput', return_value=transput)
    caplog.set_level(logging.DEBUG)
    process_file('inputs', {
        'path': '/data/a', 'url': '/tmp/a?' + QUERY, 'type': 'FILE'})

    assert '/tmp/a' in caplog.text
    assert QUERY not in caplog.text
