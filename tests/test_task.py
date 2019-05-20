# -*- coding: utf-8 -*-

from delayed.task import Task

from .common import func


def test_task_serialize():
    task = Task.create(func, (1, 2))
    data = task.serialize()
    assert data is not None
    assert data == task._data

    task = Task.deserialize(data)
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1, 2)
    assert task.kwargs == {}

    task = Task.create(func, (1,), {'b': 2})
    data = task.serialize()
    task = Task.deserialize(data)
    assert task.module_name == 'tests.common'
    assert task.func_name == 'func'
    assert task.args == (1,)
    assert task.kwargs == {'b': 2}


def test_task_run():
    task = Task.create(func, (1, 2))
    data = task.serialize()
    task = Task.deserialize(data)
    assert task.run() == 3
