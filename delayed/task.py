# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle
import functools
from importlib import import_module


dumps = functools.partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class Task(object):
    __slots__ = ['_id', '_module_name', '_func_name', '_args', '_kwargs', '_timeout', '_data']

    def __init__(self, id, module_name, func_name, args=None, kwargs=None, timeout=None):
        self._id = id
        self._module_name = module_name
        self._func_name = func_name
        self._args = () if args is None else args
        self._kwargs = {} if kwargs is None else kwargs
        self._timeout = timeout
        self._data = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, val):
        self._id = val
        self._data = None

    @property
    def module_name(self):
        return self._module_name

    @property
    def func_name(self):
        return self._func_name

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def timeout(self):
        return self._timeout

    @property
    def data(self):
        return self._data

    @classmethod
    def create(cls, func, args=None, kwargs=None, timeout=None):
        if timeout:
            timeout = timeout * 1000
        return cls(None, func.__module__, func.__name__, args, kwargs, timeout)

    def serialize(self):
        if self._data is None:
            self._data = dumps((self._id, self._module_name, self._func_name, self._args, self._kwargs, self._timeout))
        return self._data

    @classmethod
    def deserialize(cls, data):
        task = cls(*loads(data))
        task._data = data
        return task

    def run(self):
        module = import_module(self._module_name)
        func = getattr(module, self._func_name)
        return func(*self._args, **self._kwargs)
