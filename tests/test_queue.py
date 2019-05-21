# -*- coding: utf-8 -*-

from delayed.queue import Queue
from delayed.task import Task

from .common import CONN, func, DEQUEUED_KEY, ENQUEUED_KEY, QUEUE, QUEUE_NAME


class TestQueue(object):
    def test_enqueue(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert task.id > 0
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.zcard(ENQUEUED_KEY) == 1

        task2 = Task.create(func, (1, 2))
        QUEUE.enqueue(task2)
        assert task2.id == task.id + 1
        assert CONN.llen(QUEUE_NAME) == 2
        assert CONN.zcard(ENQUEUED_KEY) == 2
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

    def test_dequeue(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

        task = Task.create(func, (1, 2))
        assert QUEUE.dequeue() is None

        QUEUE.enqueue(task)
        task_id = task.id
        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 0
        assert task.id == task_id
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.data is not None
        CONN.delete(DEQUEUED_KEY)

    def test_release(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        task = QUEUE.dequeue()
        QUEUE.release(task)
        assert CONN.zcard(DEQUEUED_KEY) == 0

    def test_len(self):
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

    def test_requeue_lost(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY)

        task1 = Task.create(func, (1, 2))
        task2 = Task.create(func, (1, 2))
        queue = Queue(QUEUE_NAME, CONN, 1)
        queue.enqueue(task1)
        queue.enqueue(task2)
        assert queue.len() == 2

        CONN.lpop(QUEUE_NAME)
        assert queue.len() == 1

        assert queue.requeue_lost(0) == 0
        assert queue.len() == 1

        task = queue.dequeue()
        assert task is not None
        assert queue.len() == 0
        queue.release(task)
        assert queue.len() == 0

        assert queue.requeue_lost(0) == 1
        assert queue.len() == 1

        task = queue.dequeue()
        assert task is not None
        assert queue.len() == 0
        queue.release(task)
