# -*- coding: utf-8 -*-

import threading
import time

from delayed.task import Task
from delayed.sweeper import Sweeper

from .common import CONN, func, QUEUE, QUEUE_NAME


class TestSweeper(object):
    def test_run(self):
        CONN.delete(QUEUE_NAME)

        sweeper = Sweeper(QUEUE, 0.01, 0.01)
        thread = threading.Thread(target=sweeper.run)
        thread.start()

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        CONN.lpop(QUEUE_NAME)
        time.sleep(0.01)
        sweeper.stop()
        thread.join()

        task = QUEUE.dequeue()
        assert task is not None
        QUEUE.release(task)
