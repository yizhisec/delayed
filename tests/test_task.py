# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle

from pytest import raises

from delayed.task import get_pickle_protocol_version, set_pickle_protocol_version, Task

from .common import error_handler, func


class TestTask(object):
    def test_create(self):
        task = Task.create(func, (1, 2), timeout=10, prior=True, error_handler=error_handler)
        assert task.func_path == 'tests.common.func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout == 10000
        assert task.prior
        assert task.error_handler_path == 'tests.common.error_handler'

    def test_serialize_and_deserialize(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        assert data is not None
        assert data == task.data

        task = Task.deserialize(data)
        assert task.func_path == 'tests.common.func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None
        assert not task.prior
        assert task.error_handler_path is None

        task = Task.create(func, (1,), {'b': 2})
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.func_path == 'tests.common.func'
        assert task.args == (1,)
        assert task.kwargs == {'b': 2}
        assert task.timeout is None
        assert not task.prior
        assert task.error_handler_path is None

        task = Task.create(func, kwargs={'a': 1, 'b': 2}, timeout=10, prior=True, error_handler=error_handler)
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.func_path == 'tests.common.func'
        assert task.args == ()
        assert task.kwargs == {'a': 1, 'b': 2}
        assert task.timeout == 10000
        assert task.prior
        assert task.error_handler_path == 'tests.common.error_handler'

    def test_run(self):
        task = Task.create(func, (1, 2))
        data = task.serialize()
        task = Task.deserialize(data)
        assert task.run() == 3


def test_set_pickle_protocol_version():
    pickle_protocol_version = get_pickle_protocol_version()

    task = Task.create(func, (1, 2))
    last_data = None
    for version in range(pickle.HIGHEST_PROTOCOL + 1):
        set_pickle_protocol_version(version)

        data = task.serialize()
        assert data != last_data

        task = Task.deserialize(data)
        assert task.func_path == 'tests.common.func'
        assert task.args == (1, 2)
        assert task.kwargs == {}
        assert task.timeout is None
        assert not task.prior
        assert task.error_handler_path is None
        task._data = None
        last_data = data

    with raises(ValueError):
        set_pickle_protocol_version(pickle.HIGHEST_PROTOCOL + 1)

    set_pickle_protocol_version(pickle_protocol_version)
