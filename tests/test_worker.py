# -*- coding: utf-8 -*-

import os

from delayed.queue import Queue
from delayed.task import Task
from delayed.worker import Worker

from .common import CONN, QUEUE_NAME


FAILED_NAME = QUEUE_NAME + '_failed'


def error_func():
    raise Exception('test error')


def test_run_worker():
    def success_handler(task):
        worker.stop()

    def error_handler(task, exit_status, error):
        os.write(w, 'error')
        worker.stop()

    CONN.delete(QUEUE_NAME)
    r, w = os.pipe()
    task = Task.create(os.write, (w, 'test'))
    queue = Queue(QUEUE_NAME, CONN)
    queue.enqueue(task)
    worker = Worker(CONN, QUEUE_NAME, success_handler=success_handler, error_handler=error_handler)
    worker.run()
    assert os.read(r, 4) == 'test'

    CONN.delete(FAILED_NAME)
    task = Task.create(error_func)
    queue.enqueue(task)
    worker.run()
    assert os.read(r, 5) == 'error'
    os.close(r)
    os.close(w)
