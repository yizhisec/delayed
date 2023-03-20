# -*- coding: utf-8 -*-

from delayed.task import Task

from .common import CONN, func, NOTI_KEY, PROCESSING_KEY, QUEUE, QUEUE_NAME


class TestQueue(object):
    def test_enqueue(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert task.id > 0
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.llen(NOTI_KEY) == 1

        task2 = Task(None, 'tests.common:func', (1, 2))
        QUEUE.enqueue(task2)
        assert task2.id == task.id + 1
        assert CONN.llen(QUEUE_NAME) == 2
        assert CONN.llen(NOTI_KEY) == 2
        CONN.delete(QUEUE_NAME, NOTI_KEY)

    def test_dequeue(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)

        assert QUEUE.dequeue() is None

        task1 = Task.create(func, (1, 2))
        task2 = Task.create(func, (3,), {'b': 4})
        task3 = Task.create(func, kwargs={'a': 5, 'b': 6})
        QUEUE.enqueue(task1)
        QUEUE.enqueue(task2)
        QUEUE.enqueue(task3)

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 2
        assert CONN.llen(NOTI_KEY) == 2
        assert CONN.hget(PROCESSING_KEY, QUEUE._worker_id) == task.data
        assert task.id == task1.id
        assert task.func_path == 'tests.common:func'
        assert task.args == [1, 2]
        assert task.kwargs == {}

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 1
        assert CONN.llen(NOTI_KEY) == 1
        assert CONN.hget(PROCESSING_KEY, QUEUE._worker_id) == task.data
        assert task.id == task2.id
        assert task.func_path == 'tests.common:func'
        assert task.args == [3]
        assert task.kwargs == {'b': 4}

        task = QUEUE.dequeue()
        assert CONN.llen(QUEUE_NAME) == 0
        assert CONN.llen(NOTI_KEY) == 0
        assert CONN.hget(PROCESSING_KEY, QUEUE._worker_id) == task.data
        assert task.id == task3.id
        assert task.func_path == 'tests.common:func'
        assert task.args == []
        assert task.kwargs == {'a': 5, 'b': 6}

        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)

    def test_release(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        task = QUEUE.dequeue()
        QUEUE.release()
        assert CONN.llen(QUEUE_NAME) == 0
        assert CONN.llen(NOTI_KEY) == 0
        assert not CONN.hexists(PROCESSING_KEY, QUEUE._worker_id)

    def test_len(self):
        CONN.delete(QUEUE_NAME)

        assert QUEUE.len() == 0

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert QUEUE.len() == 1

        task = QUEUE.dequeue()
        assert task is not None
        assert QUEUE.len() == 0
        QUEUE.release()

    def test_requeue_lost(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)

        assert QUEUE.requeue_lost() == 0

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        assert QUEUE.len() == 1
        assert QUEUE.requeue_lost() == 0

        QUEUE.dequeue()
        assert QUEUE.requeue_lost() == 1
        assert QUEUE.len() == 1

        QUEUE.keep_alive()
        QUEUE.dequeue()
        assert QUEUE.requeue_lost() == 0
        QUEUE._die()
        assert QUEUE.requeue_lost() == 1

        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)
