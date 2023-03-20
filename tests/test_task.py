# -*- coding: utf-8 -*-

from delayed.task import Task

from .common import func


class TestTask(object):
    def test_create(self):
        task = Task.create(func, (1, 2))
        assert task.func_path == 'tests.common:func'
        assert task.args == (1, 2)
        assert task.kwargs == {}

    def test_serialize_and_deserialize(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = Task.deserialize(data)
        assert task.func_path == 'tests.common:func'
        assert task.args == [1, 2]
        assert task.kwargs == {}

        task = Task.create(func, (1,), {'b': 2})
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.func_path == 'tests.common:func'
        assert task.args == [1]
        assert task.kwargs == {'b': 2}

        task = Task.create(func, kwargs={'a': 1, 'b': 2})
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.func_path == 'tests.common:func'
        assert task.args == []
        assert task.kwargs == {'a': 1, 'b': 2}

    def test_execute(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.execute() == 3
