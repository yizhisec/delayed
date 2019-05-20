# -*- coding: utf-8 -*-

import errno
import gc
import os
import logging
import select
import signal
import time

from .queue import Queue
from .utils import ignore_signal, set_non_blocking


class WorkerStatus(object):
    (
        STOPPED,
        RUNNING,
        STOPPING
    ) = range(3)


class Worker(object):
    def __init__(self, conn, queue_name, timeout=600, kill_timeout=5, success_handler=None, error_handler=None):
        self._queue = Queue(queue_name, conn)
        self._timeout = timeout
        self._kill_timeout = kill_timeout
        self._success_handler = success_handler
        self._error_handler = error_handler
        self._status = WorkerStatus.STOPPED
        r, w = os.pipe()
        set_non_blocking(r)
        set_non_blocking(w)
        self._waker = r, w
        self._poll = select.poll()

    def run(self):
        self._status = WorkerStatus.RUNNING
        self._register_signals()

        try:
            while self._status == WorkerStatus.RUNNING:
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
            self._status = WorkerStatus.STOPPED

    def stop(self):
        self._status = WorkerStatus.STOPPING

    def _register_signals(self):
        signal.signal(signal.SIGCHLD, ignore_signal)
        signal.set_wakeup_fd(self._waker[1])
        self._poll.register(self._waker[0], select.POLLIN)

    def _unregister_signals(self):
        self._poll.unregister(self._waker[0])
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.set_wakeup_fd(-1)

    def _clean_up(self):
        os.close(self._waker[0])
        os.close(self._waker[1])

    def __del__(self):
        self._clean_up()

    def _monitor_task(self, task, pid):
        deadline = time.time() + self._timeout
        kill_deadline = deadline + self._kill_timeout
        try:
            while True:
                try:
                    if self._poll.poll(100):
                        p, exit_status = os.waitpid(pid, os.WNOHANG)
                        if p != 0:
                            if exit_status:
                                if self._error_handler:
                                    self._handle_error(task, exit_status, None)
                            else:
                                if self._success_handler:
                                    self._handler_success(task)
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
                    else:
                        os.kill(pid, signal.SIGTERM)
                    continue
        except Exception as e:
            logging.exception('monitor task %d error', task.id)
            if self._error_handler:
                self._error_handler(task, None, e)
        else:
            self._release_task(task)

    def _handler_success(self, task):
        try:
            self._success_handler(task)
        except Exception:
            logging.exception('handle success failed')

    def _handle_error(self, task, exit_status, error):
        try:
            self._error_handler(task, exit_status, error)
        except Exception:
            logging.exception('handle error failed')

    def _run_task(self, task):
        self._unregister_signals()
        self._clean_up()
        error_code = 0
        try:
            task.run()
        except:
            logging.exception('task %d failed', task.id)
            error_code = 1
        finally:
            self._release_task(task)
            os._exit(error_code)

    def _release_task(self, task):
        # the task can be released twice by both the monitor and the worker
        try:
            self._queue.release(task)
        except Exception:
            logging.exception('release task %d failed', task.id)
