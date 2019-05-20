# -*- coding: utf-8 -*-

import os

import redis

from delayed.delay import Delay
from delayed.queue import Queue, _ENQUEUED_KEY_SUFFIX, _DEQUEUED_KEY_SUFFIX


QUEUE_NAME = 'default'
ENQUEUED_KEY = QUEUE_NAME + _ENQUEUED_KEY_SUFFIX
DEQUEUED_KEY = QUEUE_NAME + _DEQUEUED_KEY_SUFFIX

CONN = redis.Redis()
QUEUE = Queue(QUEUE_NAME, CONN)
DELAY = Delay(QUEUE)


def func(a, b):
    return a + b


@DELAY
def delayed_func(a, b):
    return a + b


@DELAY
def delayed_write(fd, text):
    os.write(fd, text)
