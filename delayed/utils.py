# -*- coding: utf-8 -*-

import errno
import fcntl
import os
import select
import time

from .constants import BUF_SIZE


def ignore_signal(signum, frame):
    """A no-op signal handler."""
    return


def select_ignore_eintr(rlist, wlist, xlist, timeout=None):
    """It calls select.select() and ignores EINTR."""
    while True:
        try:
            return select.select(rlist, wlist, xlist, timeout)
        except select.error as e:
            if e.args[0] != errno.EINTR:  # pragma: no cover
                raise


def wait_pid_ignore_eintr(pid, options):
    """It calls os.waitpid() and ignores EINTR."""
    while True:
        try:
            return os.waitpid(pid, options)
        except OSError as e:  # pragma: no cover
            if e.errno != errno.EINTR:
                raise


def set_non_blocking(fd):
    """Sets a file description as non-blocking.

    Args:
        fd (int): The file description to be set.
    """
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def non_blocking_pipe():
    """Creates a non-blocking pipe.

    Returns:
        (int, int): The non-blocking pipe.
    """
    r, w = os.pipe()
    set_non_blocking(r)
    set_non_blocking(w)
    return r, w


def drain_out(fd):
    """Reads all the data from the file description.

    Args:
        fd (int): The file description to be read.

    Returns:
        bool: Whether any data has been read from the file description.
    """
    has_read = False
    while True:
        try:
            data = os.read(fd, BUF_SIZE)
            if data:
                has_read = True
                if len(data) < BUF_SIZE:
                    return has_read
            else:
                return has_read
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            return has_read


def read1(fd, length=BUF_SIZE):
    """Reads data from the file description with at most one call to the underlying os.read().

    Args:
        fd (int): The file description to be read.
        length (int): The read buffer size.

    Returns:
        bytes: The read data.
    """
    while True:
        try:
            return os.read(fd, length)
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:  # it should be readable
                return b''


def read_bytes(fd, length, buf):
    """Reads data from the file description into a buffer.

    Args:
        fd (int): The file description to be read.
        length (int): The data length to be read.
        buf (io.BytesIO): The read buffer.

    Returns:
        int: The rest data length.
    """
    while True:
        try:
            data = os.read(fd, length)
            if data:
                buf.write(data)
                length -= len(data)
                if length == 0:
                    return length
            else:
                return length
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:
                return length


def try_write(fd, data):
    """Tries to writes all the data to the file description.
    If the fd is blocking, it may be blocked if the write buffer is full.
    If the fd is non-blocking, it returns the rest data that cannot be written.

    Args:
        fd (int): The file description to be write to.
        data (bytes): The data to be write.

    Returns:
        (bytes, int): The rest data that cannot be written and the error number.
    """
    while data:
        try:
            length = os.write(fd, data)
            data = data[length:]
        except OSError as e:
            if e.errno == errno.EINTR:  # pragma: no cover
                continue
            return data, e.errno
    return data, 0


def write_byte(fd, data):
    """Writes a byte to the file description and ignores EPIPE.
    It may be blocked if the write buffer is full.

    Args:
        fd (int): The file description to be write to.
        data (bytes): The data to be write.

    Returns:
        int: The written length.
    """
    while True:
        try:
            return os.write(fd, data)
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EPIPE:
                return 0


def current_timestamp():
    """Gets the current timestamp in millisecond.

    Returns:
        int: Current timestamp.
    """
    return int(time.time() * 1000)
