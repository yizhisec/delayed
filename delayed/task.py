# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:
    import pickle
import functools
from importlib import import_module


dumps = functools.partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class Task(object):
    __slots__ = ['id', '_module_name', '_func_name', '_args', '_kwargs']

    def __init__(self, id, module_name, func_name, args, kwargs):
        self.id = id
        self._module_name = module_name
        self._func_name = func_name
        self._args = args
        self._kwargs = kwargs

    @classmethod
    def create(cls, func, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        return cls(None, func.__module__, func.__name__, args, kwargs)

    def serialize(self):
        return dumps((self.id, self._module_name, self._func_name, self._args, self._kwargs))

    def serialize_with_result(self, exit_status, error):
        return dumps((self.id, self._module_name, self._func_name, self._args, self._kwargs, exit_status, error))

    @classmethod
    def deserialize(cls, data):
        return cls(*loads(data))

    def run(self):
        module = import_module(self._module_name)
        func = getattr(module, self._func_name)
        return func(*self._args, **self._kwargs)
