# -*- coding: utf-8 -*-

import errno
import gc
import os
import logging
import select
import signal
import sys
import time

from .status import Status
from .task import Task
from .utils import drain_out, ignore_signal, non_blocking_pipe, read_all, write_all


_SIGNAL_MASK = 0xff


class Worker(object):
    __slots__ = ['_queue', '_kill_timeout', '_success_handler', '_error_handler', '_status']

    def __init__(self, queue, kill_timeout=5, success_handler=None, error_handler=None):
        self._queue = queue
        self._kill_timeout = kill_timeout
        self._success_handler = success_handler
        self._error_handler = error_handler
        self._status = Status.STOPPED

    def run(self):
        raise NotImplementedError

    def stop(self):
        self._status = Status.STOPPING

    def _handler_success(self, task):
        try:
            self._success_handler(task)
        except Exception:
            logging.exception('handle success failed')

    def _handle_error(self, task, exit_status, exc_info):
        try:
            self._error_handler(task, exit_status, exc_info)
        except Exception:
            logging.exception('handle error failed')

    def _requeue_task(self, task):
        try:
            self._queue.requeue(task)
        except Exception:
            logging.exception('requeue task %d failed', task.id)

    def _release_task(self, task):
        # the task can be released twice by both the monitor and the worker
        try:
            self._queue.release(task)
        except Exception:
            logging.exception('release task %d failed', task.id)

    def _register_signals(self):
        def stop(signum, frame):
            self._status = Status.STOPPING
        signal.signal(signal.SIGHUP, stop)

    def _unregister_signals(self):
        signal.signal(signal.SIGHUP, signal.SIG_DFL)


class ForkedWorker(Worker):
    __slots__ = ['_waker', '_poll']

    def run(self):
        self._status = Status.RUNNING
        self._register_signals()

        try:
            while self._status == Status.RUNNING:
                try:
                    task = self._queue.dequeue()
                except Exception:
                    logging.exception('dequeue task failed')
                else:
                    if task:
                        gc.disable()  # https://bugs.python.org/issue1336
                        try:
                            pid = os.fork()
                        except OSError:
                            gc.enable()
                            logging.exception('fork task worker failed')
                        else:
                            gc.enable()
                            if pid == 0:  # child
                                self._run_task(task)
                            else:  # parent
                                self._monitor_task(task, pid)
        finally:
            self._unregister_signals()
            self._status = Status.STOPPED

    def _register_signals(self):
        super(ForkedWorker, self)._register_signals()
        signal.signal(signal.SIGCHLD, ignore_signal)

        self._waker = r, w = non_blocking_pipe()
        self._poll = select.poll()
        signal.set_wakeup_fd(w)
        self._poll.register(r, select.POLLIN)

    def _unregister_signals(self):
        self._poll.unregister(self._waker[0])
        del self._poll
        signal.set_wakeup_fd(-1)
        os.close(self._waker[0])
        os.close(self._waker[1])
        del self._waker

        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        super(ForkedWorker, self)._unregister_signals()

    def _monitor_task(self, task, pid):
        now = time.time()
        if task.timeout:
            deadline = now + task.timeout / 1000
        else:
            deadline = now + self._queue.default_timeout / 1000
        kill_deadline = deadline + self._kill_timeout
        r = self._waker[0]
        killing = False
        try:
            while True:
                try:
                    if self._poll.poll(100):
                        drain_out(r)
                        p, exit_status = os.waitpid(pid, os.WNOHANG)
                        if p != 0:
                            if exit_status:
                                kill_signal = exit_status & _SIGNAL_MASK
                                if kill_signal:
                                    if self._error_handler:
                                        self._handle_error(task, kill_signal, None)
                                else:  # task hasn't been run
                                    self._requeue_task(task)
                                    return
                            break
                except OSError as e:
                    if e.errno != errno.EINTR:
                        raise
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise

                now = time.time()
                if now >= deadline:
                    if now >= kill_deadline:
                        os.kill(pid, signal.SIGKILL)
                    elif not killing:
                        os.kill(pid, signal.SIGTERM)
                        killing = True
        except Exception:
            logging.exception('monitor task %d error', task.id)
            if self._error_handler:
                self._error_handler(task, None, sys.exc_info())

        self._release_task(task)

    def _run_task(self, task):
        error_code = 1
        try:
            self._unregister_signals()
            error_code = 0
            try:
                task.run()
            except Exception:
                logging.exception('task %d failed', task.id)
                if self._error_handler:
                    self._error_handler(task, None, sys.exc_info())
            else:
                if self._success_handler:
                    self._handler_success(task)
            finally:
                self._release_task(task)
        finally:
            os._exit(error_code)


class PreforkedWorker(Worker):
    __slots__ = ['_waker', '_task_channel', '_result_channel', '_poll', '_child_pid']

    def run(self):
        self._status = Status.RUNNING
        self._register_signals()

        try:
            while self._status == Status.RUNNING:
                try:
                    task = self._queue.dequeue()
                except Exception:
                    logging.exception('dequeue task failed')
                else:
                    if task:
                        if not self._child_pid:
                            gc.disable()  # https://bugs.python.org/issue1336
                            try:
                                pid = os.fork()
                            except OSError:
                                gc.enable()
                                logging.exception('fork task worker failed')
                            else:
                                gc.enable()
                                if pid == 0:  # child
                                    self._run_tasks()
                                else:  # parent
                                    self._child_pid = pid
                        self._monitor_task(task, self._child_pid)
        finally:
            self._unregister_signals()
            self._status = Status.STOPPED

    def _register_signals(self):
        super(PreforkedWorker, self)._register_signals()
        signal.signal(signal.SIGCHLD, ignore_signal)

        self._waker = r, w = non_blocking_pipe()
        self._task_channel = os.pipe()
        self._result_channel = os.pipe()
        self._poll = select.poll()
        self._child_pid = None

        signal.set_wakeup_fd(w)
        self._poll = select.poll()
        self._poll.register(r, select.POLLIN)
        self._poll.register(self._result_channel[0], select.POLLIN)

    def _unregister_signals(self):
        self._poll.unregister(self._result_channel[0])
        self._poll.unregister(self._waker[0])
        signal.set_wakeup_fd(-1)
        del self._poll

        os.close(self._waker[0])
        os.close(self._waker[1])
        del self._waker

        os.close(self._task_channel[0])
        os.close(self._task_channel[1])
        del self._task_channel

        os.close(self._result_channel[0])
        os.close(self._result_channel[1])
        del self._result_channel

        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        super(PreforkedWorker, self)._unregister_signals()

    def _monitor_task(self, task, pid):
        now = time.time()
        if task.timeout:
            deadline = now + task.timeout / 1000
        else:
            deadline = now + self._queue.default_timeout / 1000
        kill_deadline = deadline + self._kill_timeout
        waker_reader = self._waker[0]
        killing = False

        write_all(self._task_channel[1], task.data)

        try:
            while True:
                try:
                    events = self._poll.poll(100)
                    if events:
                        done = False

                        for fd, event in events:
                            if fd == waker_reader:  # catch a signal (maybe SIGCHLD)
                                drain_out(waker_reader)
                                p, exit_status = os.waitpid(pid, os.WNOHANG)
                                if p != 0:
                                    self._child_pid = None
                                    if exit_status:
                                        kill_signal = exit_status & _SIGNAL_MASK
                                        if kill_signal:
                                            if self._error_handler:
                                                self._handle_error(task, kill_signal, None)
                                        else:  # task hasn't been run
                                            self._requeue_task(task)
                                            return
                                    done = True
                                    break
                            else:  # task has finished or child has exited
                                if read_all(fd):  # task has finished
                                    done = True
                                    break
                                # else child has exited

                        if done:
                            break
                except OSError as e:
                    if e.errno != errno.EINTR:
                        raise
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise

                now = time.time()
                if now >= deadline:
                    if now >= kill_deadline:
                        os.kill(pid, signal.SIGKILL)
                    elif not killing:
                        os.kill(pid, signal.SIGTERM)
                        killing = True
        except Exception:
            logging.exception('monitor task %d error', task.id)
            if self._error_handler:
                self._error_handler(task, 0, sys.exc_info())

        self._release_task(task)

    def _run_tasks(self):
        error_code = 1
        try:
            task_reader, task_writer = self._task_channel
            os.close(task_writer)
            del self._task_channel

            result_reader, result_writer = self._result_channel
            self._poll.unregister(result_reader)
            os.close(result_reader)
            del self._result_channel

            self._poll.unregister(self._waker[0])
            signal.set_wakeup_fd(-1)
            os.close(self._waker[0])
            os.close(self._waker[1])
            del self._waker
            del self._poll

            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            super(PreforkedWorker, self)._unregister_signals()

            poll = select.poll()
            poll.register(task_reader, select.POLLIN)

            while True:
                error_code = 1
                try:
                    if poll.poll():
                        task_data = read_all(task_reader)
                        if task_data:
                            try:
                                task = Task.deserialize(task_data)
                            except Exception:
                                logging.exception('deserialize task failed')
                                os.write(result_writer, b'1')
                            else:
                                error_code = 0
                                try:
                                    task.run()
                                except:  # noqa
                                    logging.exception('task %d failed', task.id)
                                    os.write(result_writer, b'1')
                                    if self._error_handler:
                                        self._error_handler(task, 0, sys.exc_info())
                                else:
                                    os.write(result_writer, b'0')
                                    if self._success_handler:
                                        self._handler_success(task)
                                finally:
                                    self._release_task(task)
                        else:  # parent has exited
                            os._exit(error_code)
                except OSError as e:
                    if e.errno != errno.EINTR:
                        raise
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise
        finally:
            os._exit(error_code)
