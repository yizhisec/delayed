# -*- coding: utf-8 -*-

import threading
import time

from delayed.queue import Queue, _NOTI_KEY_SUFFIX
from delayed.sweeper import Sweeper
from delayed.task import Task

from .common import CONN, func, QUEUE, QUEUE_NAME, NOTI_KEY


class TestSweeper(object):
    def test_run(self):
        noti_key = 'test' + _NOTI_KEY_SUFFIX
        CONN.delete(QUEUE_NAME, NOTI_KEY, noti_key)

        # queue = Queue(QUEUE_NAME, CONN)
        queue = Queue('test', CONN)
        queue._worker_id = 'test'
        sweeper = Sweeper([QUEUE, queue], 0.01)
        thread = threading.Thread(target=sweeper.run)
        thread.start()

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        CONN.lpop(NOTI_KEY)

        task = QUEUE.dequeue()
        assert task is not None

        time.sleep(0.01)
        task = QUEUE.dequeue()
        assert task is not None
        QUEUE.release()

        task2 = Task.create(func, (1, 2))
        queue.enqueue(task2)
        CONN.lpop(noti_key)

        task2 = queue.dequeue()
        assert task2 is not None

        time.sleep(0.01)
        task2 = queue.dequeue()
        assert task2 is not None
        queue.release()

        sweeper.stop()
        thread.join()
