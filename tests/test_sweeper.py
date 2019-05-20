# -*- coding: utf-8 -*-

import os
import signal
import time

from delayed.task import Task
from delayed.sweeper import Sweeper

from .common import CONN, func, QUEUE, QUEUE_NAME


def test_run_sweeper():
    sweeper = Sweeper(QUEUE, 0.01, 0.01)

    pid = os.fork()
    if pid == 0:
        sweeper.run()
        os._exit(0)
    else:
        CONN.delete(QUEUE_NAME)

        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        CONN.lpop(QUEUE_NAME)
        time.sleep(0.01)

        task = QUEUE.dequeue()
        assert task is not None
        QUEUE.release(task)
        os.kill(pid, signal.SIGKILL)
