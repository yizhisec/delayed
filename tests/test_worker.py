# -*- coding: utf-8 -*-

import os
import signal

from delayed.task import Task
from delayed.worker import ForkedWorker, PreforkedWorker

from .common import CONN, DELAY, QUEUE, QUEUE_NAME


TEST_STRING = b'test'
ERROR_STRING = b'error'
COUNT = 0


def error_func():
    raise Exception('test error')


class TestForkedWorker(object):
    def test_run(self):
        def success_handler(task):
            os.kill(pid, signal.SIGHUP)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)
            os.kill(pid, signal.SIGHUP)

        CONN.delete(QUEUE_NAME)
        pid = os.getpid()
        r, w = os.pipe()
        task = Task.create(os.write, (w, TEST_STRING))
        QUEUE.enqueue(task)
        worker = ForkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker.run()
        assert os.read(r, 4) == TEST_STRING

        task = Task.create(error_func)
        QUEUE.enqueue(task)
        worker.run()
        assert os.read(r, 5) == ERROR_STRING

        DELAY(os.write)(w, TEST_STRING)
        worker.run()
        assert os.read(r, 4) == TEST_STRING
        os.close(r)
        os.close(w)


class TestPreforkedWorker(object):
    def test_run(self):
        def success_handler(task):
            global COUNT
            COUNT += 1
            if COUNT % 2 == 0:
                os.kill(pid, signal.SIGHUP)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)
            os.kill(pid, signal.SIGHUP)

        CONN.delete(QUEUE_NAME)
        pid = os.getpid()
        r, w = os.pipe()
        for _ in range(2):
            task = Task.create(os.write, (w, TEST_STRING))
            QUEUE.enqueue(task)
        worker = PreforkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker.run()
        assert os.read(r, 8) == TEST_STRING * 2

        task = Task.create(error_func)
        QUEUE.enqueue(task)
        worker.run()
        assert os.read(r, 5) == ERROR_STRING

        DELAY(os.write)(w, TEST_STRING)
        DELAY(os.write)(w, TEST_STRING)
        worker.run()
        assert os.read(r, 8) == TEST_STRING * 2
        os.close(r)
        os.close(w)
