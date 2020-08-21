# -*- coding: utf-8 -*-

from delayed.logger import setup_logger
from delayed.worker import PreforkedWorker

from .client import queue


setup_logger()

worker = PreforkedWorker(queue=queue)
worker.run()
