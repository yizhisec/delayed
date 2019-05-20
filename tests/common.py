# -*- coding: utf-8 -*-

import redis

from delayed.queue import _ENQUEUED_KEY_SUFFIX, _DEQUEUED_KEY_SUFFIX


QUEUE_NAME = 'default'
ENQUEUED_KEY = QUEUE_NAME + _ENQUEUED_KEY_SUFFIX
DEQUEUED_KEY = QUEUE_NAME + _DEQUEUED_KEY_SUFFIX

CONN = redis.Redis()


def func(a, b):
    return a + b
