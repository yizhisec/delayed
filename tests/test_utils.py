# -*- coding: utf-8 -*-

import io
import os

import delayed.utils
from delayed.utils import drain_out, non_blocking_pipe, read_bytes, retry_time


def test_drain_out():
    buf_size = delayed.utils.BUF_SIZE
    delayed.utils.BUF_SIZE = 1024
    r, w = non_blocking_pipe()
    os.write(w, b'1' * 2048)
    os.close(w)
    assert drain_out(r)
    os.close(r)
    delayed.utils.BUF_SIZE = buf_size


def test_read_bytes():
    r, w = non_blocking_pipe()
    buf = io.BytesIO()

    os.write(w, b'1')
    assert read_bytes(r, 10, buf) == 9
    assert buf.getvalue() == b'1'
    assert read_bytes(r, 9, buf) == 9
    assert buf.getvalue() == b'1'

    os.write(w, b'1' * 9)
    assert read_bytes(r, 9, buf) == 0
    assert buf.getvalue() == b'1' * 10

    os.close(w)
    os.close(r)


def test_retry_time():
    for _ in range(1000):
        assert 1 <= retry_time(1) <= 2
        assert 1 <= retry_time(5) <= 32
        assert 1 <= retry_time(7) <= 128
        assert 1 <= retry_time(10) <= 128
