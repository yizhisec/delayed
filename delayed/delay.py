# -*- coding: utf-8 -*-

from .task import Task


def delayed(queue):
    def wrapper(timeout=None):
        def outer(func):
            def _delay(*args, **kwargs):
                task = Task.create(func, args, kwargs, timeout)
                queue.enqueue(task)

            def _timeout(timeout):
                def inner(*args, **kwargs):
                    task = Task.create(func, args, kwargs, timeout)
                    queue.enqueue(task)
                return inner

            func.delay = _delay
            func.timeout = _timeout
            return func
        return outer
    return wrapper


def delay(queue):
    def wrapper(func):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs)
            queue.enqueue(task)
        return _delay
    return wrapper


def delay_in_time(queue):
    def wrapper(func, timeout):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs, timeout)
            queue.enqueue(task)
        return _delay
    return wrapper
