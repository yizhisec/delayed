# -*- coding: utf-8 -*-

import logging
import errno
import select


class Sweeper(object):
    def __init__(self, queue, interval=60, timeout=600):
        self._queue = queue
        self._interval = interval * 1000
        self._timeout = timeout
        self._poll = select.poll()

    def run(self):
        while True:
            try:
                self._poll.poll(self._interval)
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise

            try:
                self._queue.requeue_lost(self._timeout)
            except Exception:
                logging.exception('requeue lost task failed')
