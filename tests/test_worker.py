# -*- coding: utf-8 -*-

import errno
import os
import signal
import struct
import sys
import threading
import time

import pytest

from delayed.constants import BUF_SIZE
from delayed.task import Task
from delayed.utils import non_blocking_pipe, select_ignore_eintr, try_write, wait_pid_ignore_eintr
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
    def test_run(self, monkeypatch):
        def success_handler(task):
            os.kill(pid, signal.SIGHUP)

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)
        pid = os.getpid()

        worker = ForkedWorker(QUEUE, success_handler=success_handler)
        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        worker.run()

        worker = PreforkedWorker(QUEUE, success_handler=success_handler)
        task = Task.create(func, (1, 2))
        QUEUE.enqueue(task)
        worker.run()

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_requeue_task(self, monkeypatch):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        pid = os.getpid()
        _exit = os._exit
        _close = os.close

        def exit(n):
            assert n == 1  # the parent worker will requeue task if its child's exit code is 1
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

        task3 = Task.create(sys.exit)
        QUEUE.enqueue(task3)
        worker._register_signals()
        with pytest.raises(SystemExit) as exc_info:
            worker._run_task(task3)
        assert exc_info.value.code is None
        assert os.read(r, 4) == TEST_STRING

        task4 = Task.create(sys.exit, (1,))
        QUEUE.enqueue(task4)
        worker._register_signals()
        with pytest.raises(SystemExit) as exc_info:
            worker._run_task(task4)
        assert exc_info.value.code == 1
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
            task = Task.create(os.write, (w, TEST_STRING), timeout=100)
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
            os.write(task_writer, struct.pack('=I', len(task2.data)) + task2.data)

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
        worker._task_channel = _, task_writer = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()
        os.write(task_writer, struct.pack('=I', len(task1.data)) + task1.data)
        worker._run_tasks()

        assert os.read(r, 4) == TEST_STRING
        assert os.read(r, 5) == ERROR_STRING

        close(worker._task_channel[0])
        close(worker._result_channel[1])

        def success_handler2(task):
            os.write(w, TEST_STRING)
            os.write(task_writer, struct.pack('=I', len(task4.data)) + task4.data)

        task3 = Task.create(sys.exit)
        QUEUE.enqueue(task3)
        task4 = Task.create(sys.exit, (1,))
        QUEUE.enqueue(task4)

        worker = PreforkedWorker(QUEUE, success_handler=success_handler2, error_handler=error_handler)
        worker._register_signals()
        worker._task_channel = _, task_writer = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        os.write(task_writer, struct.pack('=I', len(task3.data)) + task3.data)
        with pytest.raises(SystemExit) as exc_info:
            worker._run_tasks()
        assert exc_info.value.code is None
        assert os.read(r, 4) == TEST_STRING

        worker._register_signals()
        with pytest.raises(SystemExit) as exc_info:
            worker._run_tasks()
        assert exc_info.value.code == 1
        assert os.read(r, 5) == ERROR_STRING

        close(r)
        close(w)
        close(worker._task_channel[0])
        close(worker._result_channel[1])

        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_run_tasks_with_large_size(self):
        r, w = os.pipe()

        def success_handler(task):
            os.write(w, TEST_STRING)
            os._exit(0)

        task = Task.create(func, (b'1' * BUF_SIZE, b'2' * BUF_SIZE))
        QUEUE.enqueue(task)
        worker = PreforkedWorker(QUEUE, success_handler=success_handler)
        worker._register_signals()
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        pid = os.fork()
        if pid == 0:  # child worker
            worker._run_tasks()
        else:
            worker._child_pid = pid
            assert worker._send_task(task, time.time(), 10)
            os.close(worker._result_channel[1])
            assert wait_pid_ignore_eintr(pid, 0) == (pid, 0)
            assert os.read(r, 4) == TEST_STRING

            os.close(r)
            os.close(w)

            os.close(worker._task_channel[0])
            os.close(worker._task_channel[1])
            os.close(worker._result_channel[0])
            worker._unregister_signals()
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
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()
        os.write(worker._task_channel[1], struct.pack('=I', len(ERROR_STRING)) + ERROR_STRING)
        worker._run_tasks()

        os.close(worker._task_channel[0])
        os.close(worker._result_channel[1])
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

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

    def test_send_task_failed(self):
        task1 = Task.create(func, (1, 2))
        task1.serialize()
        task2 = Task.create(func, ('1' * BUF_SIZE, 2 * BUF_SIZE))
        task2.serialize()

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        pid = os.fork()
        if pid == 0:  # child worker
            os._exit(0)

        os.close(worker._task_channel[0])
        os.close(worker._result_channel[1])
        worker._child_pid = pid
        assert wait_pid_ignore_eintr(pid, 0) == (pid, 0)
        assert not worker._send_task(task1, time.time(), 0.1)  # broken pipe

        os.close(worker._task_channel[1])
        os.close(worker._result_channel[0])
        worker._unregister_signals()

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        pid = os.fork()
        if pid == 0:  # child worker
            os._exit(0)

        os.close(worker._task_channel[0])
        os.close(worker._result_channel[1])
        worker._child_pid = pid
        assert wait_pid_ignore_eintr(pid, 0) == (pid, 0)
        assert not worker._send_task(task2, time.time(), 0.1)  # broken pipe

        os.close(worker._task_channel[1])
        os.close(worker._result_channel[0])
        worker._unregister_signals()

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        pid = os.fork()
        if pid == 0:  # child worker
            time.sleep(0.2)
            os._exit(0)

        os.close(worker._task_channel[0])
        os.close(worker._result_channel[1])
        worker._child_pid = pid
        assert not worker._send_task(task2, 0, 0)  # time out
        assert wait_pid_ignore_eintr(pid, 0) == (pid, 0)

        os.close(worker._task_channel[1])
        os.close(worker._result_channel[0])
        worker._unregister_signals()

    def test_recv_task(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        task1 = Task.create(func, (1, 2))
        QUEUE.enqueue(task1)
        task2 = Task.create(func, (b'1' * BUF_SIZE, b'2' * BUF_SIZE))
        QUEUE.enqueue(task2)

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        worker._task_channel = task_reader, task_writer = non_blocking_pipe()
        worker._result_channel = result_reader, result_writer = non_blocking_pipe()

        p = os.getpid()
        pid = os.fork()
        if pid == 0:  # sender
            os.close(task_reader)
            os.close(result_writer)

            rlist = (result_reader,)
            worker._child_pid = p
            assert worker._send_task(task1, time.time(), 10)
            select_ignore_eintr(rlist, (), ())
            os.read(result_reader, 1)

            assert worker._send_task(task2, time.time(), 10)
            select_ignore_eintr(rlist, (), ())
            os.read(result_reader, 1)

            data_len = BUF_SIZE * 3
            data = struct.pack('=I', data_len) + b'1' * data_len
            data, error_no = try_write(task_writer, data)
            assert error_no == errno.EAGAIN

            select_ignore_eintr((), (task_writer,), ())
            data, error_no = try_write(task_writer, data)
            assert error_no == errno.EAGAIN

            # not finished
            os._exit(2)

        os.close(task_writer)
        os.close(result_reader)

        assert worker._recv_task() == task1.data
        os.write(result_writer, b'0')
        assert worker._recv_task() == task2.data
        os.write(result_writer, b'0')
        assert worker._recv_task() is None

        assert wait_pid_ignore_eintr(pid, 0) == (pid, 0x200)

        os.close(task_reader)
        os.close(result_writer)
        worker._unregister_signals()
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

    def test_monitor_task(self):
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)

        task = Task.create(func, (b'1' * BUF_SIZE, b'2' * BUF_SIZE), timeout=0.1)
        QUEUE.enqueue(task)

        worker = PreforkedWorker(QUEUE)
        worker._register_signals()
        worker._task_channel = non_blocking_pipe()
        worker._result_channel = non_blocking_pipe()

        pid = os.fork()
        if pid == 0:  # child worker
            os._exit(0)

        os.close(worker._task_channel[0])
        os.close(worker._result_channel[1])
        worker._child_pid = pid
        assert worker._monitor_task(task) is None

        os.close(worker._task_channel[1])
        os.close(worker._result_channel[0])
        worker._unregister_signals()
        CONN.delete(QUEUE_NAME, ENQUEUED_KEY, DEQUEUED_KEY, NOTI_KEY)
