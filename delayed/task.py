# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle
import functools
from importlib import import_module

from .logger import logger


dumps = functools.partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class Task(object):
    """Task is the class of a task.

    Args:
        id (int or None): The task id.
        module_name (str): The module name of the task function.
        func_name (str): The function name of the task function.
        args (list or tuple): Variable length argument list of the task function.
            It should be picklable.
        kwargs (dict): Arbitrary keyword arguments of the task function.
            It should be picklable.
        timeout (int or float): The timeout in seconds of the task.
    """

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
        """Create a task.

        Args:
            func (callable): The task function.
                It should be defined in module level (except the `__main__` module).
            args (list or tuple): Variable length argument list of the task function.
                It should be picklable.
            kwargs (dict): Arbitrary keyword arguments of the task function.
                It should be picklable.
            timeout (int or float): The timeout in seconds of the task.

        Returns:
            Task: The created task.
        """
        if timeout:
            timeout = timeout * 1000
        return cls(None, func.__module__, func.__name__, args, kwargs, timeout)

    def serialize(self):
        """Serializes the task to a string.

        Returns:
            str: The serialized data.
        """
        if self._data is None:
            self._data = dumps((self._id, self._module_name, self._func_name, self._args, self._kwargs, self._timeout))
        return self._data

    @classmethod
    def deserialize(cls, data):
        """Deserialize a task from a string.

        Args:
            data (str): The string to be deserialize.

        Returns:
            Task: The deserialized task.
        """
        task = cls(*loads(data))
        task._data = data
        return task

    def run(self):
        """Runs the task.

        Returns:
            Any: The result of the task function.
        """
        logger.debug('Running task %d.', self.id)
        module = import_module(self._module_name)
        func = getattr(module, self._func_name)
        return func(*self._args, **self._kwargs)
