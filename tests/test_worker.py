# -*- coding: utf-8 -*-

import os
import signal
import threading
import time

from delayed.task import Task
from delayed.worker import ForkedWorker, PreforkedWorker

from .common import CONN, DELAY, DEQUEUED_KEY, ENQUEUED_KEY, func, NOTI_KEY, QUEUE, QUEUE_NAME


TEST_STRING = b'test'
ERROR_STRING = b'error'
COUNT = 0


def error_func():
    raise Exception('test error')


def wait(fd):
    os.write(fd, b'1')
    time.sleep(10)


class TestWorker(object):
    def test_requeue_task(self, monkeypatch):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        pid = os.getpid()
        _exit = os._exit
        _close = os.close

        def exit(n):
            assert n == 1
            os.kill(pid, signal.SIGHUP)
            _exit(n)

        def close(fd):
            if os.getpid() == pid:
                _close(fd)
            else:
                raise Exception('close error')

        monkeypatch.setattr(os, '_exit', exit)
        monkeypatch.setattr(os, 'close', close)

        worker = ForkedWorker(QUEUE)
        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        worker.run()

        worker = PreforkedWorker(QUEUE)
        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        worker.run()

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)


class TestForkedWorker(object):
    def test_run(self):
        def success_handler(task):
            os.kill(pid, signal.SIGHUP)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)
            os.kill(pid, signal.SIGHUP)

        def kill_child():
            assert os.read(r, 1) == b'1'
            os.kill(worker._child_pid, signal.SIGTERM)

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        pid = os.getpid()
        r, w = os.pipe()
        task = Task.create(os.write, (w, TEST_STRING), timeout=10)
        QUEUE.enqueue(task)
        worker = ForkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker.run()
        assert os.read(r, 4) == TEST_STRING

        task = Task.create(error_func)
        QUEUE.enqueue(task)
        worker.run()
        assert os.read(r, 5) == ERROR_STRING

        DELAY(os.write)(w, TEST_STRING)
        worker.run()
        assert os.read(r, 4) == TEST_STRING

        task = Task.create(wait, (w,))
        QUEUE.enqueue(task)
        threading.Thread(target=kill_child).start()
        worker.run()

        os.close(r)
        os.close(w)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_run_task(self, monkeypatch):
        def success_handler(task):
            os.write(w, TEST_STRING)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        monkeypatch.setattr(os, '_exit', lambda n: None)

        r, w = os.pipe()
        task1 = Task.create(func, (1, 2))
        QUEUE.enqueue(task1)
        worker = ForkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker._register_signals()
        worker._run_task(task1)
        assert os.read(r, 4) == TEST_STRING

        task2 = Task.create(error_func)
        QUEUE.enqueue(task2)
        worker._register_signals()
        worker._run_task(task2)
        assert os.read(r, 5) == ERROR_STRING

        os.close(r)
        os.close(w)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_term_worker(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        def error_handler(task, kill_signal, exc_info):
            assert kill_signal == signal.SIGTERM
            worker.stop()

        r, w = os.pipe()
        task = Task.create(wait, (w,), timeout=0.01)
        QUEUE.enqueue(task)
        worker = ForkedWorker(QUEUE, kill_timeout=0.1, error_handler=error_handler)
        worker.run()

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_kill_worker(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        def error_handler(task, kill_signal, exc_info):
            assert kill_signal == signal.SIGKILL
            worker.stop()

        r, w = os.pipe()
        task = Task.create(wait, (w,), timeout=0.01)
        QUEUE.enqueue(task)
        worker = ForkedWorker(QUEUE, kill_timeout=0.1, error_handler=error_handler)
        worker.run()

        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)


class TestPreforkedWorker(object):
    def test_run(self):
        def success_handler(task):
            global COUNT
            COUNT += 1
            if COUNT % 2 == 0:
                os.kill(pid, signal.SIGHUP)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)
            os.kill(pid, signal.SIGHUP)

        def kill_child():
            assert os.read(r, 1) == b'1'
            os.kill(worker._child_pid, signal.SIGTERM)

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)
        pid = os.getpid()
        r, w = os.pipe()
        for _ in range(2):
            task = Task.create(os.write, (w, TEST_STRING), timeout=10)
            QUEUE.enqueue(task)
        worker = PreforkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker.run()
        assert os.read(r, 8) == TEST_STRING * 2

        task = Task.create(error_func)
        QUEUE.enqueue(task)
        worker.run()
        assert os.read(r, 5) == ERROR_STRING

        DELAY(os.write)(w, TEST_STRING)
        DELAY(os.write)(w, TEST_STRING)
        worker.run()
        assert os.read(r, 8) == TEST_STRING * 2

        task = Task.create(wait, (w,))
        QUEUE.enqueue(task)
        threading.Thread(target=kill_child).start()
        worker.run()

        os.close(r)
        os.close(w)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_run_tasks(self, monkeypatch):
        close = os.close

        def success_handler(task):
            os.write(w, TEST_STRING)
            os.write(task_writer, task2.data)

        def error_handler(task, kill_signal, exc_info):
            os.write(w, ERROR_STRING)
            close(task_writer)

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        monkeypatch.setattr(os, '_exit', lambda n: None)
        monkeypatch.setattr(os, 'close', lambda n: None)

        r, w = os.pipe()
        task1 = Task.create(func, (1, 2))
        QUEUE.enqueue(task1)
        task2 = Task.create(error_func)
        QUEUE.enqueue(task2)

        worker = PreforkedWorker(QUEUE, success_handler=success_handler, error_handler=error_handler)
        worker._register_signals()
        task_writer = worker._task_channel[1]
        os.write(task_writer, task1.data)
        worker._run_tasks()

        assert os.read(r, 4) == TEST_STRING
        assert os.read(r, 5) == ERROR_STRING

        close(r)
        close(w)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_run_tasks_with_deserialize_error(self, monkeypatch):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        pid = os.getpid()
        _read = os.read

        def read(fd, size):
            if os.getpid() == pid:
                return _read(fd, size)
            os.read = _read
            os.close(fd)
            return 'error'

        def _exit(n):
            assert n == 1

        monkeypatch.setattr(os, 'read', read)
        monkeypatch.setattr(os, '_exit', lambda n: _exit)

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        task_writer = worker._task_channel[1]
        os.write(task_writer, ERROR_STRING)
        worker._run_tasks()

    def test_term_worker(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        def error_handler(task, kill_signal, exc_info):
            assert kill_signal == signal.SIGTERM
            worker.stop()

        r, w = os.pipe()
        task = Task.create(wait, (w,), timeout=0.01)
        QUEUE.enqueue(task)
        worker = PreforkedWorker(QUEUE, kill_timeout=0.1, error_handler=error_handler)
        worker.run()

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_kill_worker(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        def error_handler(task, kill_signal, exc_info):
            assert kill_signal == signal.SIGKILL
            worker.stop()

        r, w = os.pipe()
        task = Task.create(wait, (w,), timeout=0.01)
        QUEUE.enqueue(task)
        worker = PreforkedWorker(QUEUE, kill_timeout=0.1, error_handler=error_handler)
        worker.run()

        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)
