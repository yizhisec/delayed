# -*- coding: utf-8 -*-

import time

from .logger import logger
from .status import Status


class Sweeper(object):
    """Sweeper keeps recovering lost tasks.

    Args:
        queue (delayed.queue.Queue): The task queue to be swept.
        interval (int or float): The sweeping interval in seconds.
            It tries to requeue lost tasks every `interval` seconds.
    """

    def __init__(self, queue, interval=60):
        self._queue = queue
        self._interval = interval
        self._status = Status.STOPPED

    def run(self):
        """Runs the sweeper."""
        self._status = Status.RUNNING
        logger.debug('Started a sweeper of queue %s.', self._queue._name)

        while self._status == Status.RUNNING:
            time.sleep(self._interval)
            try:
                self._queue.requeue_lost()
            except Exception:  # pragma: no cover
                logger.exception('Requeue lost tasks failed.')

        self._status = Status.STOPPED
        logger.debug('Stopped the sweeper of queue %s.', self._queue._name)

    def stop(self):
        """Stops the sweeper."""
        self._status = Status.STOPPING
        logger.debug('Stopping the sweeper of queue %s.', self._queue._name)
