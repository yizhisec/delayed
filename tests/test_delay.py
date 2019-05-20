# -*- coding: utf-8 -*-

from delayed.delay import Delay
from delayed.queue import Queue

from .common import CONN, func, QUEUE_NAME


def test_delay():
    CONN.delete(QUEUE_NAME)

    queue = Queue(QUEUE_NAME, CONN)
    delay = Delay(queue)
    delay(func, (1, 2))
    task = queue.dequeue()
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1, 2)
    assert task.kwargs == {}
    queue.release(task)
