# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle

from pytest import raises

from delayed.task import get_pickle_protocol_version, set_pickle_protocol_version, JsonTask, PickledTask, ReprTask

from .common import func


class TestPickleTask(object):
    def test_create(self):
        task = PickledTask.create(func, (1, 2), timeout=10)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout == 10000

    def test_serialize_and_deserialize(self):
        task = PickledTask.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = PickledTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None

        task = PickledTask.create(func, (1,), {'b': 2})
        data = task.serialize()
        task = PickledTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1,)
        assert task.kwargs == {'b': 2}
        assert task.timeout is None

        task = PickledTask.create(func, kwargs={'a': 1, 'b': 2}, timeout=10)
        data = task.serialize()
        task = PickledTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == ()
        assert task.kwargs == {'a': 1, 'b': 2}
        assert task.timeout == 10000

    def test_run(self):
        task = PickledTask.create(func, (1, 2))
        data = task.serialize()
        task = PickledTask.deserialize(data)
        assert task.run() == 3


def test_set_pickle_protocol_version():
    pickle_protocol_version = get_pickle_protocol_version()

    task = PickledTask.create(func, (1, 2))
    last_data = None
    for version in range(pickle.HIGHEST_PROTOCOL + 1):
        set_pickle_protocol_version(version)

        data = task.serialize()
        assert data != last_data

        task = PickledTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None
        task._data = None
        last_data = data

    with raises(ValueError):
        set_pickle_protocol_version(pickle.HIGHEST_PROTOCOL + 1)

    set_pickle_protocol_version(pickle_protocol_version)


class TestJsonTask(object):
    def test_create(self):
        task = JsonTask.create(func, (1, 2), timeout=10)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout == 10000

    def test_serialize_and_deserialize(self):
        task = JsonTask.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = JsonTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == [1, 2]
        assert task.kwargs == {}
        assert task.timeout is None

        task = JsonTask.create(func, [1], {'b': 2})
        data = task.serialize()
        task = JsonTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == [1]
        assert task.kwargs == {'b': 2}
        assert task.timeout is None

        task = JsonTask.create(func, kwargs={'a': 1, 'b': 2}, timeout=10)
        data = task.serialize()
        task = JsonTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == []
        assert task.kwargs == {'a': 1, 'b': 2}
        assert task.timeout == 10000

    def test_run(self):
        task = JsonTask.create(func, (1, 2))
        data = task.serialize()
        task = JsonTask.deserialize(data)
        assert task.run() == 3


class TestReprTask(object):
    def test_create(self):
        task = ReprTask.create(func, (1, 2), timeout=10)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout == 10000

    def test_serialize_and_deserialize(self):
        task = ReprTask.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = ReprTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None

        task = ReprTask.create(func, (1,), {'b': 2})
        data = task.serialize()
        task = ReprTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == (1,)
        assert task.kwargs == {'b': 2}
        assert task.timeout is None

        task = ReprTask.create(func, kwargs={'a': 1, 'b': 2}, timeout=10)
        data = task.serialize()
        task = ReprTask.deserialize(data)
        assert task.module_name == 'tests.common'
        assert task.func_name == 'func'
        assert task.args == ()
        assert task.kwargs == {'a': 1, 'b': 2}
        assert task.timeout == 10000

    def test_run(self):
        task = ReprTask.create(func, (1, 2))
        data = task.serialize()
        task = ReprTask.deserialize(data)
        assert task.run() == 3
