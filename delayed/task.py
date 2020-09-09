# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle
from importlib import import_module

from .constants import SEP
from .logger import logger


PICKLE_PROTOCOL_VERSION = pickle.HIGHEST_PROTOCOL
loads = pickle.loads


def dumps(obj):
    return pickle.dumps(obj, PICKLE_PROTOCOL_VERSION)


def set_pickle_protocol_version(version):
    """Set pickle protocol version for serializing and deserializing tasks.
    The default version is pickle.HIGHEST_PROTOCOL.
    You can set it to a lower version for compatibility.

    Args:
        version (int): The pickle protocol version to be set.
    """
    global PICKLE_PROTOCOL_VERSION

    if version > pickle.HIGHEST_PROTOCOL:
        raise ValueError('Unsupported pickle protocol version for current Python runtime')
    PICKLE_PROTOCOL_VERSION = version


def get_pickle_protocol_version():
    """Get the current pickle protocol version.


    Returns:
        int: The current pickle protocol version.
    """
    return PICKLE_PROTOCOL_VERSION


class Task(object):
    """Task is the class of a task.

    Args:
        id (int or None): The task id.
        func_path (str): The task function path.
            The format should be "module_path:func_name".
        args (list or tuple): Variable length argument list of the task function.
            It should be picklable.
        kwargs (dict): Arbitrary keyword arguments of the task function.
            It should be picklable.
        timeout (int or float): The timeout in seconds of the task.
        prior (bool): A prior task will be inserted at the first position.
        error_handler_path (str): The error handler path.
            If it's not empty, the error handler will be called after the task failed.
    """

    __slots__ = ['_id', '_func_path', '_args', '_kwargs', '_timeout', '_prior', '_error_handler_path', '_data']

    def __init__(self, id, func_path, args=None, kwargs=None, timeout=None, prior=False, error_handler_path=None):
        self._id = id
        self._func_path = func_path
        self._args = () if args is None else args
        self._kwargs = {} if kwargs is None else kwargs
        self._timeout = timeout
        self._prior = prior
        self._error_handler_path = error_handler_path
        self._data = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, val):
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
    def timeout(self):
        return self._timeout

    @property
    def prior(self):
        return self._prior

    @property
    def error_handler_path(self):
        return self._error_handler_path

    @property
    def data(self):
        return self._data

    @classmethod
    def create(cls, func, args=None, kwargs=None, timeout=None, prior=False, error_handler=None):
        """Create a task.

        Args:
            func (callable): The task function.
                It should be defined in module level (except the `__main__` module).
            args (list or tuple): Variable length argument list of the task function.
                It should be picklable.
            kwargs (dict): Arbitrary keyword arguments of the task function.
                It should be picklable.
            timeout (int or float): The timeout in seconds of the task.
            prior (bool): A prior task will be inserted at the first position.
            error_handler (callable): The error handler.
                If it's not empty, it will be called after the task failed.

        Returns:
            Task: The created task.
        """
        if timeout:
            timeout = timeout * 1000
        if error_handler:
            error_handler_path = error_handler.__module__ + SEP + error_handler.__name__
        else:
            error_handler_path = None
        return cls(None, func.__module__ + SEP + func.__name__, args, kwargs, timeout, prior, error_handler_path)

    def serialize(self):
        """Serializes the task to a string.

        Returns:
            str: The serialized data.
        """
        if self._data is None:
            data = self._id, self._func_path, self._args, self._kwargs, self._timeout, self._prior, self._error_handler_path

            i = 0
            if not self._error_handler_path:
                i -= 1
                if not self._prior:
                    i -= 1
                    if self._timeout is None:
                        i -= 1
                        if not self._kwargs:
                            i -= 1
                            if not self._args:
                                i -= 1
            if i < 0:
                data = data[:i]
            self._data = dumps(data)
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
        module_path, func_name = self._func_path.rsplit(SEP, 1)
        module = import_module(module_path)
        func = getattr(module, func_name)
        return func(*self._args, **self._kwargs)

    def handle_error(self, kill_signal, exc_info):
        """Calls the error handler of the task.

        Args:
            kill_signal (int or None): The kill signal of the worker. None means not be killed.
            exc_info (type, Exception, traceback.Traceback): The exception info if the worker raises an
                uncaught exception.
        """
        if self._error_handler_path:
            logger.debug('Calling the error handler for task %d.', self.id)
            try:
                module_path, func_name = self._error_handler_path.rsplit(SEP, 1)
                module = import_module(module_path)
                func = getattr(module, func_name)
                func(self, kill_signal, exc_info)
            except Exception:  # pragma: no cover
                logger.exception('Call the error handler for task %d failed.', self.id)
            else:
                logger.debug('Called the error handler for task %d.', self.id)
