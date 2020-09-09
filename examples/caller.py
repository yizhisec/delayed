# -*- coding: utf-8 -*-

from delayed.task import Task

from .client import queue
from .tasks import func1, func2, error_handler, DELAY, DELAY_WITH_PARAMS


DELAY(func1)(1, 2, x=3)

func2(1, 2, x=3)
func2.delay(1, 2, x=3)

DELAY_WITH_PARAMS(timeout=0.1, error_handler=error_handler)(func1)(1, 2, x=3)

task = Task(id=None, func_path='examples.tasks:func1', args=(1, 2), kwargs={'x': 3}, timeout=0.1, error_handler_path='examples.tasks:error_handler')
queue.enqueue(task)

task = Task.create(func=func1, args=(1, 2), kwargs={'x': 3}, timeout=0.1, prior=True, error_handler=error_handler)
queue.enqueue(task)
