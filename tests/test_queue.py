# -*- coding: utf-8 -*-

from delayed.queue import Queue
from delayed.task import Task

from .common import CONN, func, DEQUEUED_KEY, ENQUEUED_KEY, QUEUE_NAME


def test_enqueue():
    CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

    queue = Queue(QUEUE_NAME, CONN)
    task = Task.create(func, (1, 2))
    queue.enqueue(task)
    assert task.id > 0
    assert CONN.llen(QUEUE_NAME) == 1
    assert CONN.zcard(ENQUEUED_KEY) == 1

    task2 = Task.create(func, (1, 2))
    queue.enqueue(task2)
    assert task2.id == task.id + 1
    assert CONN.llen(QUEUE_NAME) == 2
    assert CONN.zcard(ENQUEUED_KEY) == 2
    CONN.delete(QUEUE_NAME, ENQUEUED_KEY)


def test_dequeue():
    CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, CONN)
    assert queue.dequeue() is None

    queue.enqueue(task)
    task_id = task.id
    task = queue.dequeue()
    assert CONN.llen(QUEUE_NAME) == 0
    assert task.id == task_id
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1, 2)
    assert task.kwargs == {}
    assert task.data is not None
    CONN.delete(DEQUEUED_KEY)


def test_release():
    CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, CONN)
    queue.enqueue(task)
    task = queue.dequeue()
    queue.release(task)
    assert CONN.zcard(DEQUEUED_KEY) == 0


def test_len():
    CONN.delete(QUEUE_NAME)

    queue = Queue(QUEUE_NAME, CONN)
    assert queue.len() == 0

    task = Task.create(func, (1, 2))
    queue.enqueue(task)
    assert queue.len() == 1

    task = queue.dequeue()
    assert task is not None
    assert queue.len() == 0
    queue.release(task)


def test_requeue_lost():
    CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, CONN)
    queue.enqueue(task)
    CONN.lpop(QUEUE_NAME)
    queue.requeue_lost(0)
    task = queue.dequeue()
    assert task is not None
    queue.release(task)
