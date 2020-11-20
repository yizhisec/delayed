# -*- coding: utf-8 -*-


class Status(object):
    (
        STOPPED,
        RUNNING,
        STOPPING
    ) = range(3)


BUF_SIZE = 65536  # Same as the default pipe capacity of Linux and macOS.
SIGNAL_MASK = 0xff
SEP = ':'
MAX_SLEEP_COUNT = 7
MIN_SLEEP_TIME = 1.0
