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

        task2 = Task(None, 'tests.common', 'func', (1, 2))
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
        task3 = Task.create(func, kwargs={'a': 5, 'b': 6}, prior=True)
        QUEUE.enqueue(task1)
        QUEUE.enqueue(task2)
        QUEUE.enqueue(task3)

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 2
        assert CONN.llen(NOTI_KEY) == 2
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 1
        assert task3.id == task3.id
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == ()
        assert task.kwargs == {'a': 5, 'b': 6}
        assert task.prior
        assert task.data is not None

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.llen(NOTI_KEY) == 1
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 2
        assert task.id == task1.id
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert not task.prior
        assert task.data is not None

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 0
        assert CONN.llen(NOTI_KEY) == 0
        assert CONN.zcard(ENQUEUED_KEY) == 3
        assert CONN.zcard(DEQUEUED_KEY) == 3
        assert task.id == task2.id
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (3,)
        assert task.kwargs == {'b': 4}
        assert task.data is not None
        assert not task.prior

        CONN.delete(DEQUEUED_KEY, ENQUEUED_KEY)

    def test_requeue(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        task1 = Task.create(func, (1, 2))
        task2 = Task.create(func, (1, 2), prior=True)
        QUEUE.enqueue(task1)
        QUEUE.enqueue(task2)
        assert not QUEUE.requeue(task1)
        assert not QUEUE.requeue(task2)

        task = QUEUE.dequeue()
        data = task.data
        task._data = None
        assert not QUEUE.requeue(task)
        assert CONN.zcard(DEQUEUED_KEY) == 1

        task._data = data
        assert QUEUE.requeue(task)
        assert CONN.zcard(DEQUEUED_KEY) == 0

        task = QUEUE.dequeue()
        assert task.id == task2.id
        task = QUEUE.dequeue()
        assert task.id == task1.id

        QUEUE.requeue(task1)
        QUEUE.requeue(task2)

        task = QUEUE.dequeue()
        assert task.id == task2.id
        task = QUEUE.dequeue()
        assert task.id == task1.id

        QUEUE.release(task1)
        QUEUE.release(task2)

        assert not QUEUE.requeue(task1)
        assert not QUEUE.requeue(task2)

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

        assert QUEUE.len() == 0

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert QUEUE.len() == 1

        task = QUEUE.dequeue()
        assert task is not None
        assert QUEUE.len() == 0
        QUEUE.release(task)

    def test_dequeued_len(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        assert QUEUE.dequeued_len() == 0

        task1 = Task.create(func, (1, 2))
        QUEUE.enqueue(task1)
        assert QUEUE.dequeued_len() == 0

        task = QUEUE.dequeue()
        assert task.id == task1.id
        assert QUEUE.dequeued_len() == 1

        task2 = Task.create(func, (1, 2))
        QUEUE.enqueue(task2)
        assert QUEUE.dequeued_len() == 1

        task = QUEUE.dequeue()
        assert task.id == task2.id
        assert QUEUE.dequeued_len() == 2

        task3 = Task.create(func, (1, 2))
        QUEUE.enqueue(task3)
        assert QUEUE.dequeued_len() == 2

        task = QUEUE.dequeue()
        assert task.id == task3.id
        assert QUEUE.dequeued_len() == 3

        QUEUE.release(task2)
        assert QUEUE.dequeued_len() == 2

        QUEUE.release(task1)
        assert QUEUE.dequeued_len() == 1

        QUEUE.release(task3)
        assert QUEUE.dequeued_len() == 0

    def test_index(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, DEQUEUED_KEY, ENQUEUED_KEY)

        assert QUEUE.index(1) == -1

        task1 = Task.create(func, (1, 2))
        QUEUE.enqueue(task1)
        assert QUEUE.index(task1.id) == 1

        task2 = Task.create(func, (1, 2))
        QUEUE.enqueue(task2)
        assert QUEUE.index(task1.id) == 1
        assert QUEUE.index(task1.id, 1) == 1
        assert QUEUE.index(task1.id, 0) == 1
        assert QUEUE.index(task1.id, -1) == 1
        assert QUEUE.index(task2.id) == 2
        assert QUEUE.index(task2.id, 1) == -1

        task3 = Task.create(func, (1, 2))
        QUEUE.enqueue(task3)
        assert QUEUE.index(task1.id) == 1
        assert QUEUE.index(task2.id) == 2
        assert QUEUE.index(task3.id) == 3
        assert QUEUE.index(task3.id, 1) == -1
        assert QUEUE.index(task3.id, 2) == -1
        assert QUEUE.index(task3.id, 3) == 3

        task = QUEUE.dequeue()
        assert task.id == task1.id
        assert QUEUE.index(task1.id) == 0
        assert QUEUE.index(task2.id) == 1
        assert QUEUE.index(task3.id) == 2

        task = QUEUE.dequeue()
        assert task.id == task2.id
        assert QUEUE.index(task1.id) == 0
        assert QUEUE.index(task2.id) == 0
        assert QUEUE.index(task3.id) == 1

        QUEUE.release(task2)
        assert QUEUE.index(task1.id) == 0
        assert QUEUE.index(task2.id) == -1
        assert QUEUE.index(task3.id) == 1

        QUEUE.release(task1)
        assert QUEUE.index(task1.id) == -1
        assert QUEUE.index(task2.id) == -1
        assert QUEUE.index(task3.id) == 1

        task = QUEUE.dequeue()
        assert task.id == task3.id
        assert QUEUE.index(task1.id) == -1
        assert QUEUE.index(task2.id) == -1
        assert QUEUE.index(task3.id) == 0

        QUEUE.release(task3)
        assert QUEUE.index(task1.id) == -1
        assert QUEUE.index(task2.id) == -1
        assert QUEUE.index(task3.id) == -1

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

        task5 = Task.create(func, (1, 2))
        task6 = Task.create(func, (1, 2))
        queue.enqueue(task5)
        queue.enqueue(task6)
        CONN.lpop(NOTI_KEY)
        CONN.lpop(NOTI_KEY)
        assert queue.requeue_lost() == 2

        task5 = queue.dequeue()
        queue.release(task5)
        task6 = queue.dequeue()
        queue.release(task6)
