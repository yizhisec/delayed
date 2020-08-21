# -*- coding: utf-8 -*-

from delayed.logger import setup_logger
from delayed.worker import ForkedWorker

from .client import queue


setup_logger()

worker = ForkedWorker(queue=queue)
worker.run()
