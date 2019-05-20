# -*- coding: utf-8 -*-

from delayed.queue import Queue, _ENQUEUED_KEY_SUFFIX, _DEQUEUED_KEY_SUFFIX
from delayed.task import Task

from .common import conn, func


QUEUE_NAME = 'default'
ENQUEUED_KEY = QUEUE_NAME + _ENQUEUED_KEY_SUFFIX
DEQUEUED_KEY = QUEUE_NAME + _DEQUEUED_KEY_SUFFIX


def test_enqueue():
    conn.delete(QUEUE_NAME, ENQUEUED_KEY)

    queue = Queue(QUEUE_NAME, conn)
    task = Task.create(func, (1, 2))
    queue.enqueue(task)
    assert task.id > 0
    assert conn.llen(QUEUE_NAME) == 1
    assert conn.zcard(ENQUEUED_KEY) == 1

    task2 = Task.create(func, (1, 2))
    queue.enqueue(task2)
    assert task2.id == task.id + 1
    assert conn.llen(QUEUE_NAME) == 2
    assert conn.zcard(ENQUEUED_KEY) == 2
    conn.delete(QUEUE_NAME, ENQUEUED_KEY)


def test_dequeue():
    conn.delete(QUEUE_NAME, ENQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, conn)
    assert queue.dequeue() is None

    queue.enqueue(task)
    task_id = task.id
    task = queue.dequeue()
    assert conn.llen(QUEUE_NAME) == 0
    assert task.id == task_id
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1, 2)
    assert task.kwargs == {}
    assert task.data is not None
    conn.delete(DEQUEUED_KEY)


def test_release():
    conn.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, conn)
    queue.enqueue(task)
    task = queue.dequeue()
    queue.release(task)
    assert conn.zcard(DEQUEUED_KEY) == 0


def test_len():
    conn.delete(QUEUE_NAME)

    queue = Queue(QUEUE_NAME, conn)
    assert queue.len() == 0

    task = Task.create(func, (1, 2))
    queue.enqueue(task)
    assert queue.len() == 1

    task = queue.dequeue()
    assert task is not None
    assert queue.len() == 0
    queue.release(task)


def test_requeue_lost():
    conn.delete(QUEUE_NAME, ENQUEUED_KEY)

    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, conn)
    queue.enqueue(task)
    conn.blpop(QUEUE_NAME, 1)
    queue.requeue_lost(0)
    task = queue.dequeue()
    assert task is not None
    queue.release(task)
