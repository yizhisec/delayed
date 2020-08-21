# -*- coding: utf-8 -*-

import os
import time

from delayed.delay import delay, delayed, delay_with_params
from delayed.logger import logger, setup_logger

from .client import queue


setup_logger()

DELAY = delay(queue)
DELAYED = delayed(queue)
DELAY_WITH_PARAMS = delay_with_params(queue)


def error_handler(task, kill_signal, exc_info):
    if kill_signal:
        logger.error('task %d got killed by signal %d', task.id, kill_signal)
    else:
        logger.exception('task %d failed', task.id, exc_info=exc_info)


def func1(*args, **kwargs):
    logger.info(os.getpid())
    time.sleep(1)


@DELAYED(timeout=0.1, error_handler=error_handler)
def func2(*args, **kwargs):
    logger.info(os.getpid())
    time.sleep(1)
