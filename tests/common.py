# -*- coding: utf-8 -*-

import redis

from delayed.delay import delay
from delayed.queue import Queue, _ENQUEUED_KEY_SUFFIX, _DEQUEUED_KEY_SUFFIX, _NOTI_KEY_SUFFIX


QUEUE_NAME = 'default'
NOTI_KEY = QUEUE_NAME + _NOTI_KEY_SUFFIX
ENQUEUED_KEY = QUEUE_NAME + _ENQUEUED_KEY_SUFFIX
DEQUEUED_KEY = QUEUE_NAME + _DEQUEUED_KEY_SUFFIX

CONN = redis.Redis()
QUEUE = Queue(QUEUE_NAME, CONN)
DELAY = delay(QUEUE)


def func(a, b):
    return a + b
