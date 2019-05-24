# -*- coding: utf-8 -*-

from .task import Task


def delayed(queue):
    def wrapper(func):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs)
            queue.enqueue(task)
            return delay

        def _timeout(seconds):
            def inner(*args, **kwargs):
                task = Task.create(func, args, kwargs, seconds)
                queue.enqueue(task)
            return inner

        func.delay = _delay
        func.timeout = _timeout
        return func
    return wrapper


def delay(queue):
    def wrapper(func):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs)
            queue.enqueue(task)
            return delay
        return _delay
    return wrapper


def delay_in_time(queue):
    def wrapper(func, timeout):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs, timeout)
            queue.enqueue(task)
            return delay
        return _delay
    return wrapper
