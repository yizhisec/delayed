# -*- coding: utf-8 -*-

from io import BytesIO
import errno
import fcntl
import os
import struct
import time


_BUF_SIZE = 1024


def ignore_signal(signum, frame):
    """A no-op signal handler."""
    return


def set_non_blocking(fd):
    """Sets a file description as non-blocking."""
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
    """
    while True:
        try:
            data = os.read(fd, _BUF_SIZE)
            if not data or len(data) < _BUF_SIZE:
                break
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:
                break


def read_all(fd):
    """Reads and returns all the data from the file description.

    Args:
        fd (int): The file description to be read.

    Returns:
        bytes: The data read from the file description.
    """
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
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:
                break
    return all_data


def read1(fd, length=_BUF_SIZE):
    while True:
        try:
            return os.read(fd, length)
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:  # it should be readable
                return ''


def read_task_data(fd):
    head_data = read1(fd)
    if not head_data or len(head_data) <= 4:
        return ''

    data_length = struct.unpack('=I', head_data[:4])
    data = head_data[4:]
    read_length = len(data)
    if read_length == data_length:
        return data

    length = data_length - read_length
    buf = BytesIO(data)
    try:
        while True:
            try:
                data = os.read(fd, length)
                if data:
                    buf.write(data)
                    length -= len(data)
                    if length == 0:
                        break
            except OSError as e:  # pragma: no cover
                if e.errno == errno.EINTR:
                    continue
                if e.errno == errno.EAGAIN:
                    break
        return buf.getvalue()
    finally:
        buf.close()


def read_bytes(fd, length, buf):
    while True:
        try:
            data = os.read(fd, length)
            if data:
                buf.write(data)
                length -= len(data)
                if length == 0:
                    return length
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue
            if e.errno == errno.EAGAIN:
                return length


def write_all(fd, data):
    """Writes all the data to the file description.
    It may be blocked if the write buffer is full.

    Args:
        fd (int): The file description to be write to.
        data (bytes): The data to be write.
    """
    while data:
        try:
            length = os.write(fd, data)
            data = data[length:]
        except OSError as e:  # pragma: no cover
            if e.errno == errno.EINTR:
                continue


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
