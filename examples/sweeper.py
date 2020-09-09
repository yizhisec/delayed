# -*- coding: utf-8 -*-

from delayed.logger import setup_logger
from delayed.sweeper import Sweeper

from .client import queue


setup_logger()

sweeper = Sweeper(queues=[queue])
sweeper.run()
