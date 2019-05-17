# -*- coding: utf-8 -*-

from delayed.queue import Queue
from delayed.task import Task

from .common import conn, func


QUEUE_NAME = 'default'


def test_enqueue():
    conn.delete(QUEUE_NAME)
    queue = Queue(QUEUE_NAME, conn)
    task = Task.create(func, (1, 2))
    queue.enqueue(task)
    assert task.id is not None
    assert conn.llen(QUEUE_NAME) == 1

    task2 = Task.create(func, (1, 2))
    queue.enqueue(task2)
    assert task2.id > task.id
    assert conn.llen(QUEUE_NAME) == 2
    conn.delete(QUEUE_NAME)


def test_dequeue():
    conn.delete(QUEUE_NAME)
    task = Task.create(func, (1, 2))
    queue = Queue(QUEUE_NAME, conn)
    assert queue.dequeue() is None

    queue.enqueue(task)
    task = queue.dequeue()
    assert conn.llen(QUEUE_NAME) == 0
    assert task._module_name == 'tests.common'
    assert task._func_name == 'func'
    assert task._args == (1, 2)
    assert task._kwargs == {}
