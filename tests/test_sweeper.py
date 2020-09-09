# -*- coding: utf-8 -*-

import threading
import time

from delayed.queue import Queue, _NOTI_KEY_SUFFIX
from delayed.sweeper import Sweeper
from delayed.task import Task

from .common import CONN, func, QUEUE_NAME, NOTI_KEY


class TestSweeper(object):
    def test_run(self):
        CONN.delete(QUEUE_NAME)

        queue = Queue(QUEUE_NAME, CONN, default_timeout=0.01, requeue_timeout=0)
        queue2 = Queue('test', CONN, default_timeout=0.01, requeue_timeout=0)
        sweeper = Sweeper([queue, queue2], 0.01)
        thread = threading.Thread(target=sweeper.run)
        thread.start()

        task = Task.create(func, (1, 2))
        queue.enqueue(task)
        CONN.lpop(NOTI_KEY)

        task = queue.dequeue()
        assert task is not None

        time.sleep(0.01)
        task = queue.dequeue()
        assert task is not None
        queue.release(task)

        task2 = Task.create(func, (1, 2))
        queue2.enqueue(task2)
        CONN.lpop('test' + _NOTI_KEY_SUFFIX)

        task2 = queue2.dequeue()
        assert task2 is not None

        time.sleep(0.01)
        task2 = queue2.dequeue()
        assert task2 is not None
        queue2.release(task2)

        sweeper.stop()
        thread.join()
