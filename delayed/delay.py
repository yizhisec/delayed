# -*- coding: utf-8 -*-

from .task import Task


def delayed(queue):
    """A decorator for defining task functions.

    Args:
        queue (delayed.queue.Queue): The task queue.

    Returns:
        callable: A decorator.
    """
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
    """A decorator for defining task functions with default timeout.

    Args:
        queue (delayed.queue.Queue): The task queue.

    Returns:
        callable: A decorator.
    """
    def wrapper(func):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs)
            queue.enqueue(task)
        return _delay
    return wrapper


def delay_in_time(queue):
    """A decorator for defining task functions with specified timeout.

    Args:
        queue (delayed.queue.Queue): The task queue.

    Returns:
        callable: A decorator.
    """
    def wrapper(func, timeout):
        def _delay(*args, **kwargs):
            task = Task.create(func, args, kwargs, timeout)
            queue.enqueue(task)
        return _delay
    return wrapper
