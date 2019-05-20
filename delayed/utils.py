# -*- coding: utf-8 -*-

import fcntl
import os
import time


def ignore_signal(signum, frame):
    return


def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def current_timestamp():
    return int(time.time() * 1000)
