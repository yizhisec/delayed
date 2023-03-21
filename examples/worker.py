# -*- coding: utf-8 -*-

from delayed.logger import setup_logger
from delayed.worker import Worker

from .client import queue


setup_logger()

worker = Worker(queue=queue)
worker.run()
