# -*- coding: utf-8 -*-

from delayed.queue import Queue
from delayed.task import Task

from .common import CONN, DEQUEUED_KEY, ENQUEUED_KEY, func, NOTI_KEY, QUEUE, QUEUE_NAME


class TestQueue(object):
    def test_enqueue(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, ENQUEUED_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert task.id > 0
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.llen(NOTI_KEY) == 1
        assert CONN.zcard(ENQUEUED_KEY) == 1

        task2 = Task.create(func, (1, 2))
        QUEUE.enqueue(task2)
        assert task2.id == task.id + 1
        assert CONN.llen(QUEUE_NAME) == 2
        assert CONN.llen(NOTI_KEY) == 2
        assert CONN.zcard(ENQUEUED_KEY) == 2
        CONN.delete(QUEUE_NAME, NOTI_KEY, ENQUEUED_KEY)

    def test_dequeue(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        assert QUEUE.dequeue() is None

        task1 = Task.create(func, (1, 2))
        task2 = Task.create(func, (3,), {'b': 4})
        QUEUE.enqueue(task1)
        QUEUE.enqueue(task2)

        task_id = task1.id
        task1 = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.llen(NOTI_KEY) == 1
        assert CONN.zcard(ENQUEUED_KEY) == 2
        assert CONN.zcard(DEQUEUED_KEY) == 1
        assert task1.id == task_id
        assert task1.module_name == 'tests.common'
        assert task1.func_name == 'func'
        assert task1.args == (1, 2)
        assert task1.kwargs == {}
        assert task1.data is not None

        task_id = task2.id
        task2 = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 0
        assert CONN.llen(NOTI_KEY) == 0
        assert CONN.zcard(ENQUEUED_KEY) == 2
        assert CONN.zcard(DEQUEUED_KEY) == 2
        assert task2.id == task_id
        assert task2.module_name == 'tests.common'
        assert task2.func_name == 'func'
        assert task2.args == (3,)
        assert task2.kwargs == {'b': 4}
        assert task2.data is not None
        CONN.delete(DEQUEUED_KEY, ENQUEUED_KEY)

    def test_release(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        task = QUEUE.dequeue()
        QUEUE.release(task)
        assert CONN.llen(QUEUE_NAME) == 0
        assert CONN.llen(NOTI_KEY) == 0
        assert CONN.zcard(ENQUEUED_KEY) == 0
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
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        queue = Queue(QUEUE_NAME, CONN, default_timeout=0, requeue_timeout=0, busy_len=2)
        assert queue.requeue_lost() == 0

        task1 = Task.create(func, (1, 2))
        queue.enqueue(task1)
        assert queue.len() == 1
        assert queue.requeue_lost() == 0

        CONN.lpop(NOTI_KEY)
        assert queue.requeue_lost() == 1
        assert CONN.llen(NOTI_KEY) == 1
        task1 = queue.dequeue()
        assert task1 is not None
        assert CONN.llen(NOTI_KEY) == 0

        assert queue.requeue_lost() == 1
        task1 = queue.dequeue()
        assert task1 is not None
        assert CONN.llen(NOTI_KEY) == 0
        queue.release(task1)

        assert CONN.zcard(ENQUEUED_KEY) == 0
        assert CONN.zcard(DEQUEUED_KEY) == 0

        task2 = Task.create(func, (1, 2))
        task3 = Task.create(func, (1, 2))
        task4 = Task.create(func, (1, 2))
        queue.enqueue(task2)
        queue.enqueue(task3)
        queue.enqueue(task4)
        assert queue.len() == 3
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 0

        CONN.lpop(NOTI_KEY)
        assert queue.requeue_lost() == 1
        assert CONN.llen(NOTI_KEY) == 3
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 0

        task2 = queue.dequeue()
        assert task2 is not None
        assert queue.len() == 2
        assert CONN.llen(NOTI_KEY) == 2
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 1

        assert queue.requeue_lost() == 0  # busy
        task3 = queue.dequeue()
        assert task3 is not None
        assert queue.len() == 1
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 2

        queue.release(task3)
        assert queue.len() == 1
        assert CONN.zcard(ENQUEUED_KEY) == 2
        assert CONN.zcard(DEQUEUED_KEY) == 1

        assert queue.requeue_lost() == 1
        assert queue.len() == 2
        assert CONN.zcard(ENQUEUED_KEY) == 2
        assert CONN.zcard(DEQUEUED_KEY) == 0

        task4 = queue.dequeue()
        assert task4 is not None
        queue.release(task4)

        assert queue.requeue_lost() == 0
        task2 = queue.dequeue()
        assert task2 is not None
        queue.release(task2)
