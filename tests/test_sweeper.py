# -*- coding: utf-8 -*-

import threading
import time

from delayed.queue import Queue
from delayed.sweeper import Sweeper
from delayed.task import Task

from .common import CONN, func, QUEUE_NAME, NOTI_KEY


class TestSweeper(object):
    def test_run(self):
        CONN.delete(QUEUE_NAME)

        queue = Queue(QUEUE_NAME, CONN, default_timeout=0.01, requeue_timeout=0)
        sweeper = Sweeper(queue, 0.01)
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

        sweeper.stop()
        thread.join()
