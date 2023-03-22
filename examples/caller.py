# -*- coding: utf-8 -*-

from delayed.task import PyTask

from .client import queue
from .tasks import func1, func2, DELAY


DELAY(func1)(1, 2, x=3)

func2(1, 2, x=3)
func2.delay(1, 2, x=3)

task = PyTask(id=None, func_path='examples.tasks:func1', args=(1, 2), kwargs={'x': 3})
queue.enqueue(task)

task = PyTask.create(func='examples.tasks:func1', args=(1, 2), kwargs={'x': 3})
queue.enqueue(task)

task = PyTask.create(func=func1, args=(1, 2), kwargs={'x': 3})
queue.enqueue(task)
