# delayed
[![Build status](https://travis-ci.org/yizhisec/delayed.svg?branch=master)](https://secure.travis-ci.org/yizhisec/delayed)
[![Coverage](https://codecov.io/gh/yizhisec/delayed/branch/master/graph/badge.svg)](https://codecov.io/gh/yizhisec/delayed)

Delayed is a simple but robust task queue inspired by [rq](https://python-rq.org/).

## Features

* Robust: all the enqueued tasks will run exactly once, even if the worker got killed at any time.
* Clean: finished tasks (including failed) won't take the space of your Redis.
* Distributed: workers as more as needed can run in the same time without further config.

## Requirements

1. Python 2.7 or later, tested on Python 2.7, 3.3 - 3.9, PyPy and PyPy3.
2. UNIX-like systems (with os.fork() implemented), tested on Ubuntu and macOS.
3. Redis 2.6.0 or later.
4. Keeps syncing time among all the machines of each task queue.

## Getting started

1. Run a redis server:

    ```bash
    $ redis-server
    ```

2. Install delayed:

    ```bash
    $ pip install delayed
    ```

3. Create a task queue:

    ```python
    import redis
    from delayed.queue import Queue

    conn = redis.Redis()
    queue = Queue(name='default', conn=conn)
    ```

4. Four ways to enqueue a task:

    * Define a task function and enqueue it:

        ```python
        from delayed.delay import delayed

        delayed = delayed(queue)

        @delayed()
        def delayed_add(a, b):
            return a + b

        delayed_add.delay(1, 2)  # enqueue delayed_add
        delayed_add.delay(1, b=2)  # same as above
        delayed_add(1, 2)  # call it immediately
        ```
    * Directly enqueue a function:

        ```python
        from delayed.delay import delay, delayed

        delay = delay(queue)
        delayed = delayed(queue)

        def add(a, b):
            return a + b

        delay(add)(1, 2)
        delay(add)(1, b=2)  # same as above

        delayed()(add).delay(1, 2)
        delayed()(add).delay(1, b=2)  # same as above
        ```
    * Create a task and enqueue it:

        ```python
        from delayed.task import Task

        def add(a, b):
            return a + b

        task = Task.create(func=add, args=(1,), kwargs={'b': 2})
        queue.enqueue(task)
        ```
    * Enqueue a predefined task function without importing it:

        ```python
        from delayed.task import Task

        task = Task(id=None, func_path='test:add', args=(1,), kwargs={'b': 2})
        queue.enqueue(task)
        ```

5. Run a task worker (or more) in a separated process:

    ```python
    import redis
    from delayed.queue import Queue
    from delayed.worker import ForkedWorker

    conn = redis.Redis()
    queue = Queue(name='default', conn=conn)
    worker = ForkedWorker(queue=queue)
    worker.run()
    ```

6. Run a task sweeper in a separated process to recovery lost tasks (mainly due to the worker got killed):

    ```python
    import redis
    from delayed.queue import Queue
    from delayed.sweeper import Sweeper

    conn = redis.Redis()
    queue = Queue(name='default', conn=conn)
    sweeper = Sweeper(queues=[queue])
    sweeper.run()
    ```

## Examples

See [examples](examples).

    ```bash
    $ redis-server &
    $ pip install delayed
    $ python -m examples.sweeper &
    $ python -m examples.forked_worker &  # or python -m examples.preforked_worker &
    $ python -m examples.caller
    ```

## QA

1. **Q: What's the limitation on a task function?**  
A: A task function should be defined in module level (except the `__main__` module). Its `args` and `kwargs` should be picklable.

2. **Q: What's the `name` param of a queue?**  
A: It's the key used to store the tasks of the queue. A queue with name "default" will use those keys:
    * default: list, enqueued tasks.
    * default_id: str, the next task id.
    * default_noti: list, the same length as enqueued tasks.
    * default_enqueued: sorted set, enqueued tasks with their timeouts.
    * default_dequeued: sorted set, dequeued tasks with their dequeued timestamps.

3. **Q: Why the worker is slow?**  
A: The `ForkedWorker` forks a new process for each new task. So all the tasks are isolated and you won't leak memory.  
To reduce the overhead of forking processes and importing modules, if your task function code won't be changed in the worker's lifetime, you can switch to `PreforkedWorker`:

    ```python
    import redis
    from delayed.queue import Queue
    from delayed.worker import PreforkedWorker

    conn = redis.Redis()
    queue = Queue(name='default', conn=conn)
    worker = PreforkedWorker(queue=queue)
    worker.run()
    ```

4. **Q: How does a `ForkedWorker` run?**  
A: It runs such a loop:
    1. It dequeues a task from the queue periodically.
    2. It forks a child process to run the task.
    3. It kills the child process if the child runs out of time.
    4. When the child process exits, it releases the task.

5. **Q: How does a `PreforkedWorker` run?**  
A: It runs such a loop:
    1. It dequeues a task from the queue periodically.
    2. If it has no child process, it forks a new one.
    3. It sends the task through a pipe to the child.
    4. It kills the child process if the child runs out of time.
    5. When the child process exits or it received result from the pipe, it releases the task.

6. **Q: How does the child process of a worker run?**  
A: The child of a `ForkedWorker` just runs the task, unmarks the task as dequeued, then exits.
The child of a `PreforkedWorker` runs such a loop:
    1. It tries to receive a task from the pipe.
    2. If the pipe has been closed, it exits.
    3. It runs the task.
    4. It sends the task result to the pipe.
    5. It releases the task.

7. **Q: What's lost tasks?**  
A: There are 2 situations a task might get lost:
    * a worker popped a task notification, then got killed before dequeueing the task.
    * a worker dequeued a task, then both the monitor and its child process got killed before they releasing the task.

8. **Q: How to recovery lost tasks?**  
A: Runs a sweeper. It dose two things:
    * it keeps the task notification length the same as the task queue.
    * it moves the timeout dequeued tasks back to the task queue.

9. **Q: How to set the timeout of tasks?**  
A: You can set `default_timeout` of a queue or `timeout` of a task:

    ```python
    from delayed.delay import delay_with_params

    queue = Queue('default', conn, default_timeout=60)

    delayed_add.timeout(10)(1, 2)

    delay_with_params(queue)(timeout=10)(add)(1, 2)
    ```

10. **Q: How to enqueue a task in front of the queue?**  
A: You can set `prior` of the task to `True`:

    ```python
    task = Task(id=None, func_path='test:add', args=(1, 2), prior=True)
    queue.enqueue(task)
    ```

11. **Q: How to handle the failed tasks?**  
A: Sets the `error_handler` of the task. The handlers would be called in a forked process, except the forked process got killed or the monitor process raised an exception.

    ```python
    from delayed.delay import delay_with_params

    def error_handler(task, kill_signal, exc_info):
        if kill_signal:
            logging.error('task %d got killed by signal %d', task.id, kill_signal)
        else:
            logging.exception('task %d failed', task.id, exc_info=exc_info)

    @delayed_with_param(queue)(error_handler=error_handler)
    def error():
        raise Exception

    def error2():
        raise Exception

    task = Task.create(func_path='test:error2', error_handler=error_handler)
    ```

12. **Q: Why does sometimes the `error_handler` not be called for a failed task?**  
A: If both the child process and the monitor process got killed at the same time, there is no chance to call the `error_handler`.

13. **Q: How to turn on the debug logs?**  
A: Adds a `logging.DEBUG` level handler to `delayed.logger.logger`. The simplest way is to call `delayed.logger.setup_logger()`:
    ```python
    from delayed.logger import setup_logger

    setup_logger()
    ```

14. **Q: Can I enqueue and dequeue tasks in different Python versions?**  
A: `delayed` uses the `pickle` module to serialize and deserialize tasks.
If `pickle.HIGHEST_PROTOCOL` is equal among all your Python runtimes, you can use it without any configurations.
Otherwise you have to choose the lowest `pickle.HIGHEST_PROTOCOL` of all your Python runtime as the pickle protocol.
eg: If you want to enqueue a task in Python 3.7 and dequeue it in Python 2.7. Their `pickle.HIGHEST_PROTOCOL` are `4` and `2`, so you need to set the version to `2`:
    ```python
    from delayed.task import set_pickle_protocol_version

    set_pickle_protocol_version(2)
    ```

15. **Q: Why not use JSON or MessagePack to serialize tasks?**  
A: These serializations may confuse some types (eg: `bytes` / `str`, `list` / `tuple`).

16. **Q: What will happen if I changed the pipe capacity?**  
A: `delayed` assumes the pipe capacity is 65536 bytes (the default value on Linux and macOS).
To reduce syscalls, it won't check whether the pipe is writable if the length of data to be written is less than 65536.
If your system has a lower pipe capacity, the `PreforkedWorker` may not working well for some large tasks.
To fix it, you can set a lower value to `delayed.constants.BUF_SIZE`:
    ```python
    import delayed.constants

    delayed.constants.BUF_SIZE = 1024
    ```

## Release notes

* 0.11:
    1. Sleeps random time when a `Worker` fails to pop a `task` before retrying.

* 0.10:
    1. The `Sweeper` can handle multiple queues now. Its `queue` param has been changed to `queues`. (BREAKING CHANGE)
    2. Changes the separator between `module_path` and `func_name` from `.` to `:`. (BREAKING CHANGE)

* 0.9:
    1. Adds `prior` and `error_handler` params to `deleyed.delayed()`, removes its `timeout()` method. (BREAKING CHANGE)
    2. Adds [examples](examples).

* 0.8:
    1. The `Task` struct has been changed, it's not compatible with older versions. (BREAKING CHANGE)
        * Removes `module_name` and `func_name` from `Task`, adds `func_path` instead.
        * Adds `error_handler_path` to `Task`.
    2. Removes `success_handler` and `error_handler` from `Worker`. (BREAKING CHANGE)

* 0.7:
    1. Implements prior task.

* 0.6:
    1. Adds `dequeued_len()` and `index` to `Queue`.

* 0.5:
    1. Adds `delayed.task.set_pickle_protocol_version()`.

* 0.4:
    1. Refactories and fixes bugs.

* 0.3:
    1. Changes param `second` to `timeout` for `delayed.delayed()`. (BREAKING CHANGE)
    2. Adds debug log.

* 0.2:
    1. Adds `timeout()` to `delayed.delayed()`.

* 0.1:
    1. Init version.
