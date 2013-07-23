import pytest

from wal_e.storage.s3_storage import SegmentNumber


def test_construction():
    sn = SegmentNumber('0' * 8, '000000FF')
    assert sn.log == '0' * 8
    assert sn.seg == '000000FF'


def test_next_smaller_simple():
    log = SegmentNumber._integer_to_name(0x0)
    seg = SegmentNumber._integer_to_name(0x1)

    sn = SegmentNumber(log=log, seg=seg)
    smaller = sn.next_smaller()

    assert smaller.log == log
    assert smaller.seg == SegmentNumber._integer_to_name(0x0)


def test_next_smaller_carry():
    log = SegmentNumber._integer_to_name(0x1)
    seg = SegmentNumber._integer_to_name(0x0)

    sn = SegmentNumber(log=log, seg=seg)
    smaller = sn.next_smaller()

    assert smaller.log == SegmentNumber._integer_to_name(0x0)
    assert smaller.seg == SegmentNumber._integer_to_name(0xFF)


def test_next_smaller_out_of_range():
    log = SegmentNumber._integer_to_name(0x0)
    seg = SegmentNumber._integer_to_name(0x0)

    sn = SegmentNumber(log=log, seg=seg)

    with pytest.raises(AssertionError):
        sn.next_smaller()


def test_next_larger_simple():
    log = SegmentNumber._integer_to_name(0x0)
    seg = SegmentNumber._integer_to_name(0x0)

    sn = SegmentNumber(log=log, seg=seg)
    larger = sn.next_larger()

    assert larger.log == log
    assert larger.seg == SegmentNumber._integer_to_name(0x1)


def test_next_larger_carry():
    log = SegmentNumber._integer_to_name(0x0)
    seg = SegmentNumber._integer_to_name(0xFF)

    sn = SegmentNumber(log=log, seg=seg)
    larger = sn.next_larger()

    assert larger.log == SegmentNumber._integer_to_name(0x1)
    assert larger.seg == SegmentNumber._integer_to_name(0x0)


def test_next_larger_out_of_range():
    log = SegmentNumber._integer_to_name(0xFFFFFFFF)
    seg = SegmentNumber._integer_to_name(0xFF)
    sn = SegmentNumber(log=log, seg=seg)

    with pytest.raises(AssertionError):
        sn.next_larger()
