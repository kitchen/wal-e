import errno
import pytest

from wal_e import worker
from wal_e.worker import PrefetchDirs


class PgXlog(object):
    """Test utility for staging a pg_xlog directory."""

    def __init__(self, cluster):
        self.cluster = cluster

        self.pg_xlog = cluster.join('pg_xlog')
        self.pg_xlog.ensure(dir=True)

    def contains(self, fn):
        return self.pg_xlog.join(fn).check(exists=1)


@pytest.fixture()
def pg_xlog(tmpdir, monkeypatch):
    """Set up xlog utility functions and change directories."""
    monkeypatch.chdir(tmpdir)

    return PgXlog(tmpdir)


@pytest.fixture()
def pd(pg_xlog):
    pd = PrefetchDirs(pg_xlog.pg_xlog.strpath)
    pd.create()
    return pd


def test_create(pg_xlog):
    """Check that directory stucture can be created."""
    pd = PrefetchDirs(pg_xlog.pg_xlog.strpath)
    pd.create()

    # Creating more than once is OK.
    assert pd.create_ok
    pd.create()

    prefetch_place = pg_xlog.pg_xlog.join('wal-e-prefetch')
    assert prefetch_place.check(dir=1, exists=1)

    tmp_place = prefetch_place.join('tmp')
    assert tmp_place.check(dir=1, exists=1)


def test_contains_and_download(pd, pg_xlog):
    segment = worker.WalSegment('00000000' * 3)

    # Starts empty: ensure .contains agrees.
    assert not pd.contains(segment)

    err = StandardError('Nope')

    # Test failed download.
    with pytest.raises(StandardError) as e:
        with pd.download_context(segment) as dc:
            raise err

    assert e.value is err
    assert not pd.contains(segment)

    # Try a failed promotion of a non-existent file.
    promote_dest = str(pg_xlog.pg_xlog.join('PRETEND_RECOVERY_XLOG'))

    with pytest.raises(EnvironmentError) as e:
        pd.promote(segment, promote_dest)

    assert e.value.errno == errno.ENOENT
    assert not pg_xlog.contains('PRETEND_RECOVERY_XLOG')

    # Successfully "download" the segment.
    #
    # In this case, it will just be a zero byte file.
    with pd.download_context(segment) as dc:
        assert dc.dest_name

    assert pd.contains(segment)

    # Test promotion of the "downloaded" segment.
    pd.promote(segment, promote_dest)
    assert pg_xlog.contains('PRETEND_RECOVERY_XLOG')


def test_create_prefetch_fail(monkeypatch, pg_xlog):
    import os

    def raise_eperm(*args, **kwargs):
        e = EnvironmentError('fake EPERM problem')
        e.errno = errno.EPERM
        raise e

    monkeypatch.setattr(os, 'mkdir', raise_eperm)

    pd = PrefetchDirs(pg_xlog.pg_xlog.strpath)
    pd.create()

    assert not pd.create_ok


def test_create_prefetch_fail_making_tmp(pg_xlog):
    pg_xlog.pg_xlog.join('wal-e-prefetch').write('a file, not a directory')

    pd = PrefetchDirs(pg_xlog.pg_xlog.strpath)
    pd.create()

    assert not pd.create_ok


def test_clear(pd, pg_xlog):
    assert pd.create_ok
    pd.clear()

    # Check that the directories have been deleted.
    prefetch_place = pg_xlog.pg_xlog.join('wal-e-prefetch')
    assert prefetch_place.check(exists=0)

    tmp_place = prefetch_place.join('tmp')
    assert tmp_place.check(exists=0)


def test_clear_with_warnings(pg_xlog):
    pd = PrefetchDirs(pg_xlog.pg_xlog.strpath)
    assert not pd.create_ok
    pd.clear()
