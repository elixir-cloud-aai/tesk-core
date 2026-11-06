"""Tests for the check that `file:` paths lie below `HOST_BASE_PATH`."""

from unittest import mock

import pytest

from tesk_core.exception import InvalidHostPath
from tesk_core.path import containerPath, isDescendant, validatePath

BASE = '/tmp'


@pytest.mark.parametrize('path', [
    '/tmp',
    '/tmp/',
    '/tmp/a',
    '/tmp/a/b',
    '/tmp/a/../b',
    '/tmp/./a',
    '/tmp//a',
    '//tmp/a',
])
def test_isDescendant_inside(path):
    """Paths at or below the base path are descendants."""

    assert isDescendant(BASE, path)


@pytest.mark.parametrize('path', [
    '/tmp2',
    '/tmp2/a',
    '/tmpfoo/a',
    '/tmp/..',
    '/tmp/../etc/passwd',
    '/tmp/a/../../etc',
    '/',
    '/etc',
    'tmp/a',
    'a/b',
    '',
])
def test_isDescendant_outside(path):
    """Siblings that share a prefix with the base path, paths that `..`
    segments lead out of the base path, and relative paths are not
    descendants."""

    assert not isDescendant(BASE, path)


def test_isDescendant_root_base():
    """With `/` as the base path, every absolute path is a descendant."""

    assert isDescendant('/', '/a/b')
    assert not isDescendant('/', 'a/b')


@mock.patch('tesk_core.path.HOST_BASE_PATH', BASE)
@pytest.mark.parametrize('path', ['/tmp2/a', '/tmp/../etc/passwd'])
def test_validatePath_rejects(path):
    """Paths outside the base path raise `InvalidHostPath`."""

    with pytest.raises(InvalidHostPath):
        validatePath(path)


@mock.patch('tesk_core.path.HOST_BASE_PATH', BASE)
@mock.patch('tesk_core.path.CONTAINER_BASE_PATH', '/transfer')
def test_containerPath_normalises():
    """Paths with `..` segments that stay below the base path are mapped
    to their normalised location in the container."""

    assert containerPath('/tmp/a/../b/c') == '/transfer/b/c'
    assert containerPath('/tmp') == '/transfer'
