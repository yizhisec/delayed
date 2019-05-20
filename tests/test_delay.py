# -*- coding: utf-8 -*-

from .common import CONN, DELAY, delayed_func, func, QUEUE, QUEUE_NAME


def test_delay():
    CONN.delete(QUEUE_NAME)

    DELAY.delay(func, 1, 2)
    task = QUEUE.dequeue()
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1, 2)
    assert task.kwargs == {}
    assert task.run() == 3
    QUEUE.release(task)


def test_wrapper():
    CONN.delete(QUEUE_NAME)

    assert delayed_func(1, 2) == 3
    delayed_func.delay(1, 2)
    task = QUEUE.dequeue()
    assert task.module_name == 'tests.common'
    assert task.func_name == 'delayed_func'
    assert task.args == (1, 2)
    assert task.kwargs == {}
    assert task.run() == 3
    QUEUE.release(task)
