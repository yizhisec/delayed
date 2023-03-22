# -*- coding: utf-8 -*-

from importlib import import_module

from msgpack import packb, unpackb

from .constants import SEP
from .logger import logger


class PyTask(object):
    """PyTask is the class of a Python task.

    Args:
        id (int or None): The task id.
        func_path (str): The task function path.
            The format is "module_path:func_name".
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

    @classmethod
    def create(cls, func, args=None, kwargs=None):
        """Create a task.

        Args:
            func (callable or str): The task function or function path.
                The function should be defined in module level (except the `__main__` module).
            args (list or tuple): Variable length argument list of the task function.
            kwargs (dict): Arbitrary keyword arguments of the task function.

        Returns:
            PyTask: The created task.
        """
        if isinstance(func, str):
            return cls(None, func, args, kwargs)
        if callable(func):
            return cls(None, func.__module__ + SEP + func.__name__, args, kwargs)
        raise ValueError('Invalid func %r' % func)

    def serialize(self):
        """Serializes the task to bytes.

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
        """Deserialize a task from the bytes.

        Args:
            data (str): The string to be deserialize.

        Returns:
            PyTask: The deserialized task.
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


class GoTask(object):
    """GoTask is the class of a Go task.

    Args:
        id (int or None): The task id.
        func_path (str): The task function path.
            The format is "package/path.func_name".
        args (any): Arguments of the task function.
    """

    __slots__ = ['_id', '_func_path', '_args', '_payload', '_data']

    def __init__(self, id, func_path, args=None):
        self._id = id
        self._func_path = func_path
        self._args = args
        self._payload = None
        self._data = None

    @classmethod
    def create(cls, func_path, args=None):
        """Create a task.

        Args:
            func_path (str): The task function path.
            args (any): Variable length argument list of the task function.

        Returns:
            PyTask: The created task.
        """
        return cls(None, func_path, args)

    def serialize(self):
        """Serializes the task to bytes.

        Returns:
            str: The serialized data.
        """
        if self._data is None:
            if self._payload is None and self._args is not None:
                logger.info(self._args)
                self._payload = packb(self._args)
                logger.info(len(self._payload))
                data = self._id, self._func_path, self._payload
            else:
                data = self._id, self._func_path
            self._data = packb(data)
        return self._data
