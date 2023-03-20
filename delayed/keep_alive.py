import threading
import time

from .constants import Status
from .logger import logger


class KeepAliveThread(threading.Thread):
    def __init__(self, worker):
        super(KeepAliveThread, self).__init__()
        self._worker = worker

    def run(self):
        worker = self._worker
        queue = worker._queue
        interval = self._worker._keep_alive_interval
        while worker._status != Status.Stopped:  # this thread can eventually see worker._status changed by other thread
            try:
                queue.keep_alive()
            except Exception:
                logger.exception('Failed to keep alive.')
            time.sleep(interval)
