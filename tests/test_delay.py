# -*- coding: utf-8 -*-

from delayed.delay import delay_with_params, delayed
from .common import CONN, DELAY, error_handler, func, QUEUE, QUEUE_NAME


DELAYED = delayed(QUEUE)
DELAY_WITH_PARAMS = delay_with_params(QUEUE)


@DELAYED()
def delayed_func(a, b):
    return a + b


@DELAYED(timeout=5, prior=True, error_handler=error_handler)
def delayed_func_with_params(a, b):
    return a + b


def test_delayed():
    CONN.delete(QUEUE_NAME)

    assert delayed_func(1, 2) == 3
    assert delayed_func.__name__ == 'delayed_func'

    delayed_func.delay(1, 2)
    assert QUEUE.len() == 1
    task = QUEUE.dequeue()
    assert task.run() == 3
    QUEUE.release(task)

    delayed_func_with_params.delay(1, 2)
    assert QUEUE.len() == 1
    task = QUEUE.dequeue()
    assert task.timeout == 5000
    assert task.run() == 3
    QUEUE.release(task)


def test_delay():
    CONN.delete(QUEUE_NAME)

    DELAY(func)(1, 2)
    assert QUEUE.len() == 1
    task = QUEUE.dequeue()
    assert task.run() == 3
    QUEUE.release(task)


def test_delay_with_params():
    CONN.delete(QUEUE_NAME)

    DELAY_WITH_PARAMS(timeout=10, prior=True, error_handler=error_handler)(func)(1, 2)
    assert QUEUE.len() == 1
    task = QUEUE.dequeue()
    assert task.timeout == 10000
    assert task.run() == 3
    QUEUE.release(task)
