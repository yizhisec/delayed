# -*- coding: utf-8 -*-


class Status(object):
    (
        STOPPED,
        RUNNING,
        STOPPING
    ) = range(3)


SEP = ':'
DEFAULT_SLEEP_TIME = 1
MAX_SLEEP_TIME = 60
