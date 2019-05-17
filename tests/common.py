# -*- coding: utf-8 -*-

import redis

conn = redis.Redis()


def func(a, b):
    return a + b
