import errno
import logging
import os
import shutil
import tempfile

from os import path
from wal_e import log_help

logger = log_help.WalELogger(__name__, level=logging.INFO)


class DownloadContext(object):
    def __init__(self, prefetch_dir, segment):
        self.prefetch_dir = prefetch_dir
        self.segment = segment

    @property
    def dest_name(self):
        return self.tf.name

    def __enter__(self):
        self.tf = tempfile.NamedTemporaryFile(
            dir=self.prefetch_dirs.prefetched_tmp, delete=False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Everything went well: flush to disk so that if a crash
            # occurs a corrupt pre-fetched WAL segment won't be sent
            # to Postgres.
            os.sync(self.tf.fileno())
            os.rename(self.tf.name, path.join(self.prefetch_dir,
                                              self.segment.name))
        else:
            os.unlink(self.tf.name)
            self.tf.close()
            raise exc_type, exc_val, exc_tb


class PrefetchDirs(object):
    def __init__(self, base):
        prefetched_dir = path.join(base, 'wal-e-prefetch')
        prefetched_tmp = path.join(prefetched_dir, 'tmp')

        self.base = base
        self.create_ok = None
        self.prefetched_dir = prefetched_dir
        self.prefetched_tmp = prefetched_tmp

    def create(self):
        """A best-effort attempt to create directories.

        Warnings are issued to the user if those directories could not
        created or if they don't exist.

        The caller should only call this function if the user
        requested prefetching (i.e. concurrency) to avoid spurious
        warnings.
        """

        def lackadaisical_mkdir(place, **kwargs):
            ok = False

            try:
                os.mkdir(place)
                ok = True
            except EnvironmentError, e:
                if e.errno == errno.EEXIST:
                    # Has already been created: this is the most
                    # common situation, and is fine.
                    ok = True
                else:
                    logger.warning(**kwargs)

            return ok

        do_prefetch = lackadaisical_mkdir(
            self.prefetched_dir,
            msg='could not create prefetch directory',
            detail=('Prefetch directory creation target: {0}'
                    .format(self.prefetched_dir)))

        # Early exit: if the parent is not okay, then the child tempfile
        # directory isn't going to be okay either.
        if not do_prefetch:
            self.create_ok = False
            return

        tempfile_place = path.join(self.prefetched_dir, 'tmp')

        do_prefetch = lackadaisical_mkdir(
            tempfile_place,
            msg='could not create prefetch temporary directory',
            detail=('Prefetch temporary directory creation '
                    'target: {0}').format(tempfile_place))

        if not do_prefetch:
            # Temp directory is not ok.
            self.create_ok = False
            return

        # All directories created or already exist: everything went
        # smoothly.
        self.create_ok = True

    def clear(self):
        def warn_on_cant_remove(function, path, excinfo):
            # Not really expected, so until complaints come in, just
            # dump a ton of information.
            logger.warning(
                msg='cannot clear prefetch data',
                detail='{0!r}\n{1!r}\n{2!r}'.format(function, path, excinfo),
                hint=('Report this as a bug: '
                      'a better error message should be written.'))

        shutil.rmtree(self.base, False, warn_on_cant_remove)

    def contains(self, segment):
        return path.isfile(path.join(self.prefetched_dir, segment.name))

    def promote(self, segment, destination):
        source = path.join(self.prefetched_dir, segment.name)
        os.rename(source, destination)

    def download_context(self, segment):
        return DownloadContext(self, segment)
