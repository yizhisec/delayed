# -*- coding: utf-8 -*-

import errno
import fcntl
import os
import time


_BUF_SIZE = 1024


def ignore_signal(signum, frame):
    return


def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def non_blocking_pipe():
    r, w = os.pipe()
    set_non_blocking(r)
    set_non_blocking(w)
    return r, w


def drain_out(fd):
    try:
        while True:
            data = os.read(fd, _BUF_SIZE)
            if not data or len(data) < _BUF_SIZE:
                break
    except OSError:  # ignore EAGAIN and EINTR
        pass


def read_all(fd):
    all_data = b''
    while True:
        try:
            data = os.read(fd, _BUF_SIZE)
            if data:
                all_data += data
                if len(data) < _BUF_SIZE:
                    break
            else:
                break
        except OSError:  # ignore EAGAIN and EINTR
            pass
    return all_data


def write_all(fd, data):
    while data:
        try:
            length = os.write(fd, data)
            data = data[length:]
        except OSError as e:
            if e.errno == errno.EINTR:
                continue


def current_timestamp():
    return int(time.time() * 1000)
