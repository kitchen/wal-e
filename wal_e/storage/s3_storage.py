#!/usr/bin/env python
"""
S3 Storage Abstraction

This module is used to define and provide accessors to the logical
structure and metadata for an S3-backed WAL-E prefix.

"""

import collections

import wal_e.exception

from urlparse import urlparse


CURRENT_VERSION = '005'

SEGMENT_REGEXP = (r'(?P<filename>(?P<tli>[0-9A-F]{8,8})(?P<log>[0-9A-F]{8,8})'
                  '(?P<seg>[0-9A-F]{8,8}))')

SEGMENT_READY_REGEXP = SEGMENT_REGEXP + r'\.ready'

BASE_BACKUP_REGEXP = (r'base_' + SEGMENT_REGEXP + r'_(?P<offset>[0-9A-F]{8})')

COMPLETE_BASE_BACKUP_REGEXP = (
    r'base_' + SEGMENT_REGEXP +
    r'_(?P<offset>[0-9A-F]{8})_backup_stop_sentinel\.json')

VOLUME_REGEXP = (r'part_(\d+)\.tar\.lzo')


# A representation of a log number and segment, naive of timeline.
# This number always increases, even when diverging into two
# timelines, so it's useful for conservative garbage collection.
class SegmentNumber(collections.namedtuple('SegmentNumber',
                                           ['log', 'seg'])):

    @property
    def as_an_integer(self):
        assert len(self.log) == 8
        assert len(self.seg) == 8
        return int(self.log + self.seg, 16)

    @staticmethod
    def _integer_to_name(integral):
        h = hex(integral)
        assert h.startswith('0x')
        assert len(h) <= 10

        name = hex(integral)[2:].zfill(8)
        assert len(name) == 8

        return name

    def next_smaller(self):
        int_seg = int(self.seg, 16)
        assert int_seg <= 0xFF

        if int_seg == 0:
            # Seg is at the minimum value, carry the decrement from
            # 'log'.
            int_log = int(self.log, 16)
            assert int_log <= 0xFFFFFFFF

            # 0 is a valid int_log, but not when decrementing from a
            # 0-valued segment: negative such values are not valid.
            assert int_seg == 0 and int_log > 0

            # NB: older versions of Postgres won't generate a seg
            # value of 0xFF, so a gap has to be treated
            # conservatively.
            s_log = SegmentNumber._integer_to_name(int_log - 1)
            s_seg = SegmentNumber._integer_to_name(0xFF)

            return self.__class__(log=s_log, seg=s_seg)
        else:
            assert int_seg > 0

            s_log = self.log
            s_seg = SegmentNumber._integer_to_name(int_seg - 1)

            return self.__class__(log=s_log, seg=s_seg)

        assert False

    def next_larger(self):
        int_seg = int(self.seg, 16)
        assert int_seg <= 0xFF

        if int_seg == 0xFF:
            # Seg is at the maximum value, carry the increment into
            # 'log'.
            #
            # NB: older versions of Postgres won't generate a seg
            # value of 0xFF, so a gap has to be treated
            # conservatively.
            int_log = int(self.log, 16)

            # Blow up if int_log would otherwise overflow.  Almost
            # certainly a bug...or Postgres is going to exhaust its
            # XLog addressing, too.
            assert int_log < 0xFFFFFFFF

            l_log = SegmentNumber._integer_to_name(int_log + 1)
            l_seg = SegmentNumber._integer_to_name(0)

            return self.__class__(l_log, l_seg)
        else:
            l_log = self.log
            l_seg = SegmentNumber._integer_to_name(int_seg + 1)

            return self.__class__(l_log, l_seg)

        assert False


# Exhaustively enumerates all possible metadata about a backup.  These
# may not always all be filled depending what access method is used to
# get information, in which case the unfilled items should be given a
# None value.  If an item was intended to be fetch, but could not be
# after some number of retries and timeouts, the field should be
# filled with the string 'timeout'.
BackupInfo = collections.namedtuple('BackupInfo',
                                    ['name',
                                     'last_modified',
                                     'expanded_size_bytes',
                                     'wal_segment_backup_start',
                                     'wal_segment_offset_backup_start',
                                     'wal_segment_backup_stop',
                                     'wal_segment_offset_backup_stop'])

OBSOLETE_VERSIONS = frozenset(('004', '003', '002', '001', '000'))


class StorageLayout(object):
    """
    Encapsulates and defines S3 URL path manipulations for WAL-E

    Without a trailing slash
    >>> sl = StorageLayout('s3://foo/bar')
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.bucket_name()
    'foo'

    With a trailing slash
    >>> sl = StorageLayout('s3://foo/bar/')
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.bucket_name()
    'foo'

    """

    def __init__(self, prefix, version=CURRENT_VERSION):
        self.VERSION = version

        url_tup = urlparse(prefix)

        if url_tup.scheme != 's3':
            raise wal_e.exception.UserException(
                msg='bad S3 URL scheme passed',
                detail='The scheme {0} was passed when "s3" was expected.'
                .format(url_tup.scheme))

        self._url_tup = url_tup

        # S3 api requests absolutely cannot contain a leading slash.
        s3_api_prefix = url_tup.path.lstrip('/')

        # Also canonicalize a trailing slash onto the prefix, should
        # none already exist. This only applies if we actually have a
        # prefix, i.e., our objects are not being created in the bucket's
        # root.
        if s3_api_prefix and s3_api_prefix[-1] != '/':
            self._s3_api_prefix = s3_api_prefix + '/'
        else:
            self._s3_api_prefix = s3_api_prefix

    def _error_on_unexpected_version(self):
        if self.VERSION != CURRENT_VERSION:
            raise ValueError('Backwards compatibility of this '
                             'operator is not implemented')

    def basebackups(self):
        return self._s3_api_prefix + 'basebackups_' + self.VERSION + '/'

    def basebackup_directory(self, backup_info):
        self._error_on_unexpected_version()
        return (self.basebackups() +
                'base_{0}_{1}/'.format(
                backup_info.wal_segment_backup_start,
                backup_info.wal_segment_offset_backup_start))

    def basebackup_sentinel(self, backup_info):
        self._error_on_unexpected_version()
        return (self.basebackup_directory(backup_info) +
                '_backup_stop_sentinel.json')

    def basebackup_tar_partition_directory(self, backup_info):
        self._error_on_unexpected_version()
        return (self.basebackup_directory(backup_info) +
                'tar_partitions/')

    def basebackup_tar_partition(self, backup_info, part_name):
        self._error_on_unexpected_version()
        return (self.basebackup_tar_partition_directory(backup_info) +
                part_name)

    def wal_directory(self):
        return self._s3_api_prefix + 'wal_' + self.VERSION + '/'

    def wal_path(self, wal_file_name):
        self._error_on_unexpected_version()
        return self.wal_directory() + wal_file_name

    def bucket_name(self):
        return self._url_tup.netloc
