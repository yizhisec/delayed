# -*- coding: utf-8 -*-

import time

from .constants import Status
from .logger import logger


class Sweeper(object):
    """Sweeper keeps recovering lost tasks.

    Args:
        queues (list(delayed.queue.Queue)): The task queue to be swept.
        interval (int or float): The sweeping interval in seconds.
            It tries to requeue lost tasks every `interval` seconds.
    """

    def __init__(self, queues, interval=60):
        self._queues = queues
        self._interval = interval
        self._status = Status.STOPPED

    def run(self):
        """Runs the sweeper."""
        self._status = Status.RUNNING
        logger.debug('Sweeper started.')

        queues = self._queues
        while self._status == Status.RUNNING:
            time.sleep(self._interval)
            for queue in queues:
                try:
                    queue.requeue_lost()
                except Exception:  # pragma: no cover
                    logger.exception('Failed to requeue lost tasks.')

        self._status = Status.STOPPED
        logger.debug('Sweeper stopped.')

    def stop(self):
        """Stops the sweeper."""
        logger.debug('Stopping the sweeper.')
        self._status = Status.STOPPING
