# -*- coding: utf-8 -*-

from .task import Task


class Delay(object):
    def __init__(self, queue):
        self._queue = queue

    def __call__(self, func, args=None, kwargs=None):
        task = Task.create(func, args, kwargs)
        self._queue.enqueue(task)
