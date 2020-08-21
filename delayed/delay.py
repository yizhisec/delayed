# -*- coding: utf-8 -*-

from .task import Task


def delayed(queue):
    """A decorator for defining task functions.

    Args:
        queue (delayed.queue.Queue): The task queue.

    Returns:
        callable: A decorator.
    """
    def outer(timeout=None, prior=False, error_handler=None):
        def wrapper(func):
            def _delay(*args, **kwargs):
                task = Task.create(func, args, kwargs, timeout, prior, error_handler)
                queue.enqueue(task)

            func.delay = _delay
            return func
        return wrapper
    return outer


def delay(queue):
    """A decorator for defining task functions with default params.

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


def delay_with_params(queue):
    """A decorator for defining task functions with specified params.

    Args:
        queue (delayed.queue.Queue): The task queue.

    Returns:
        callable: A decorator.
    """
    def outer(timeout=None, prior=False, error_handler=None):
        def wrapper(func):
            def _delay(*args, **kwargs):
                task = Task.create(func, args, kwargs, timeout, prior, error_handler)
                queue.enqueue(task)
            return _delay
        return wrapper
    return outer
