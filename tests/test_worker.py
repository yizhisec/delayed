# -*- coding: utf-8 -*-

import os
import signal

from delayed.task import PyTask

from .common import CONN, NOTI_KEY, PROCESSING_KEY, QUEUE, QUEUE_NAME, WORKER


def stop(pid):
    os.kill(pid, signal.SIGHUP)


class TestWorker(object):
    def test_run(self):
        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)

        task = PyTask.create(stop, (os.getpid(),))
        QUEUE.enqueue(task)
        WORKER.run()

        CONN.delete(QUEUE_NAME, NOTI_KEY, PROCESSING_KEY)
