# -*- coding: utf-8 -*-

from delayed.task import Task

from .common import func


class TestTask(object):
    def test_create(self):
        task = Task.create(func, (1, 2), timeout=10)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout == 10000

    def test_serialize_and_deserialize(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = Task.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None

        task = Task.create(func, (1,), {'b': 2})
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1,)
        assert task.kwargs == {'b': 2}
        assert task.timeout is None

        task = Task.create(func, kwargs={'a': 1, 'b': 2}, timeout=10)
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == ()
        assert task.kwargs == {'a': 1, 'b': 2}
        assert task.timeout == 10000

    def test_run(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.run() == 3
