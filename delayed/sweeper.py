# -*- coding: utf-8 -*-

import logging
import time


class Sweeper(object):
    def __init__(self, queue, interval=60, timeout=600):
        self._queue = queue
        self._interval = interval
        self._timeout = timeout

    def run(self):
        while True:
            time.sleep(self._interval)
            try:
                self._queue.requeue_lost(self._timeout)
            except Exception:
                logging.exception('requeue lost task failed')
