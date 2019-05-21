# -*- coding: utf-8 -*-

import logging
import time

from .status import Status


class Sweeper(object):
    def __init__(self, queue, interval=60, timeout=600):
        self._queue = queue
        self._interval = interval
        self._timeout = timeout
        self._status = Status.STOPPED

    def run(self):
        self._status = Status.RUNNING

        while self._status == Status.RUNNING:
            time.sleep(self._interval)
            try:
                self._queue.requeue_lost(self._timeout)
            except Exception:
                logging.exception('requeue lost task failed')

        self._status = Status.STOPPED

    def stop(self):
        self._status = Status.STOPPING
