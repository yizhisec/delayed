# -*- coding: utf-8 -*-

from .task import Task


class _Wrapper(object):
    def __init__(self, queue, func):
        self._queue = queue
        self._func = func

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def delay(self, *args, **kwargs):
        task = Task.create(self._func, args, kwargs)
        self._queue.enqueue(task)


class Delay(object):
    def __init__(self, queue):
        self._queue = queue

    def __call__(self, func):
        return _Wrapper(self._queue, func)

    def delay(self, func, *args, **kwargs):
        task = Task.create(func, args, kwargs)
        self._queue.enqueue(task)
