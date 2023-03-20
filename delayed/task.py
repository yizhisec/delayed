# -*- coding: utf-8 -*-

from importlib import import_module

from msgpack import packb, unpackb

from .constants import SEP
from .logger import logger


class Task(object):
    """Task is the class of a task.

    Args:
        id (int or None): The task id.
        func_path (str): The task function path.
            The format should be "module_path:func_name".
        args (list or tuple): Variable length argument list of the task function.
        kwargs (dict): Arbitrary keyword arguments of the task function. (Python task only.)
    """

    __slots__ = ['_id', '_func_path', '_args', '_kwargs', '_data']

    def __init__(self, id, func_path, args=None, kwargs=None):
        self._id = id
        self._func_path = func_path
        self._args = () if args is None else args
        self._kwargs = {} if kwargs is None else kwargs
        self._data = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, val):
        if self._id != val:
            self._id = val
            self._data = None

    @property
    def func_path(self):
        return self._func_path

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def data(self):
        return self._data

    @classmethod
    def create(cls, func, args=None, kwargs=None):
        """Create a task.

        Args:
            func (callable): The task function.
                It should be defined in module level (except the `__main__` module).
            args (list or tuple): Variable length argument list of the task function.
            kwargs (dict): Arbitrary keyword arguments of the task function.

        Returns:
            Task: The created task.
        """
        return cls(None, func.__module__ + SEP + func.__name__, args, kwargs)

    def serialize(self):
        """Serializes the task to a string.

        Returns:
            str: The serialized data.
        """
        if self._data is None:
            data = self._id, self._func_path, self._args, self._kwargs

            i = 0
            if not self._kwargs:
                i -= 1
                if not self._args:
                    i -= 1
            if i < 0:
                data = data[:i]
            self._data = packb(data)
        return self._data

    @classmethod
    def deserialize(cls, data):
        """Deserialize a task from a string.

        Args:
            data (str): The string to be deserialize.

        Returns:
            Task: The deserialized task.
        """
        task = cls(*unpackb(data))
        task._data = data
        return task

    def execute(self):
        """Executes the task.

        Returns:
            Any: The result of the task function.
        """
        logger.debug('Executing task %d.', self._id)
        module_path, func_name = self._func_path.split(SEP, 1)
        module = import_module(module_path)
        func = getattr(module, func_name)
        return func(*self._args, **self._kwargs)
