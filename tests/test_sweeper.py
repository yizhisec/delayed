# -*- coding: utf-8 -*-

import os
import signal
import time

from delayed.queue import Queue
from delayed.task import Task
from delayed.sweeper import Sweeper

from .common import conn, func


QUEUE_NAME = 'default'


def test_run_sweeper():
    sweeper = Sweeper(conn, QUEUE_NAME, 1, 1)

    pid = os.fork()
    if pid == 0:
        sweeper.run()
        os._exit(0)
    else:
        conn.delete(QUEUE_NAME)

        task = Task.create(func, (1, 2))
        queue = Queue(QUEUE_NAME, conn)
        queue.enqueue(task)
        conn.lpop(QUEUE_NAME)
        time.sleep(1)

        task = queue.dequeue()
        assert task is not None
        queue.release(task)
        os.kill(pid, signal.SIGKILL)
