# -*- coding: utf-8 -*-

from delayed.task import Task

from .common import func


def test_task_serialize():
    task = Task.create(func, (1, 2))
    data = task.serialize()
    task = Task.deserialize(data)
    assert task._module_name == 'tests.common'
    assert task._func_name == 'func'
    assert task._args == (1, 2)
    assert task._kwargs == {}

    task = Task.create(func, (1,), {'b': 2})
    data = task.serialize()
    task = Task.deserialize(data)
    assert task._module_name == 'tests.common'
    assert task._func_name == 'func'
    assert task._args == (1,)
    assert task._kwargs == {'b': 2}


def test_task_run():
    task = Task.create(func, (1, 2))
    data = task.serialize()
    task = Task.deserialize(data)
    assert task.run() == 3
