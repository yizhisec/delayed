# delayed
[![Build status](https://travis-ci.org/yizhisec/delayed.svg?branch=master)](https://secure.travis-ci.org/yizhisec/delayed)
[![Coverage](https://codecov.io/gh/yizhisec/delayed/branch/master/graph/badge.svg)](https://codecov.io/gh/yizhisec/delayed)

Delayed is a simple but robust task queue inspired by [rq](https://python-rq.org/).

## Features

* Robust: all the enqueued tasks will run exactly once, even if the worker got killed at any time.
* Clean: finished tasks (including failed) won't take the space of your Redis.
* Distributed: workers as more as needed can run in the same time without further config.

## Requirements

1. Python 2.7 or later, tested on Python 2.7, 3.3 - 3.7 and PyPy 3.5.
2. UNIX-like systems (with os.fork() implemented, pipe capacity at least 65536 bytes), tested on Ubuntu and macOS.
3. Redis 2.6.0 or later.
4. Keep syncing time among all the machines of a task queue.

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

4. Three ways to enqueue a task:

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
        from delayed.delay import delay

        delay = delay(queue)

        def add(a, b):
            return a + b

        delay(add)(1, 2)
        delay(add)(1, b=2)  # same as above
        ```
    * Enqueue a predefined task function without importing it:

        ```python
        from delayed.task import PickledTask

        task = PickledTask(id=None, module_name='test', func_name='add', args=(1, 2))
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
    sweeper = Sweeper(queue=queue)
    sweeper.run()
    ```

## QA

1. **Q: What's the limitation on a task function?**  
A: A task function should be defined in module level (except the `__main__` module). Its `args` and `kwargs` should be serializable.

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

7. **Q: What's the difference among all the task types?**  
A: `delayed` offers 3 types of tasks type:
    * `PickledTask`: The default task type. It uses `cPickle` or `pickle` module to serialize and deserialize its data, so the data should be picklable. You should specify the same pickle protocol version if you use different versions of Python.
    * `JsonTask`: It uses `ujson` or `json` module to serialize and deserialize its data, so the data should be jsonable. You can use other languages (eg: Golang, Java, etc.) to enqueue a JsonTask. Its drawback is it can't distinguish `tuple` from `list`, or `byte` from `str`. So you can't use it if your tasks cantain binary data.
    * `ReprTask`: It uses `repr()` and `ast.literal_eval()` to serialize and deserialize its data, so the task data should be Python literal structures.

8. **Q: What's lost tasks?**  
A: There are 2 situations a task might get lost:
    * a worker popped a task notification, then got killed before dequeueing the task.
    * a worker dequeued a task, then both the monitor and its child process got killed before they releasing the task.

9. **Q: How to recovery lost tasks?**  
A: Run a sweeper. It dose two things:
    * it keeps the task notification length the same as the task queue.
    * it moves the timeout dequeued tasks back to the task queue.

10. **Q: How to set the timeout of tasks?**  
A: You can set the `default_timeout` of a queue or `timeout` of a task:

    ```python
    from delayed.delay import delay_in_time

    queue = Queue('default', conn, default_timeout=60)

    delayed_add.timeout(10)(1, 2)

    delay_in_time = delay_in_time(queue)
    delay_in_time(add, timeout=10)(1, 2)
    ```

11. **Q: How to handle the finished tasks?**  
A: Set the `success_handler` and `error_handler` of the worker. The handlers would be called in a forked process, except the forked process got killed or the monitor process raised an exception.

    ```python
    def success_handler(task):
        logging.info('task %d finished', task.id)

    def error_handler(task, kill_signal, exc_info):
        if kill_signal:
            logging.error('task %d got killed by signal %d', task.id, kill_signal)
        else:
            logging.exception('task %d failed', exc_info=exc_info)

    worker = PreforkedWorker(Queue, success_handler=success_handler, error_handler=error_handler)
    ```

12. **Q: Why does sometimes both `success_handler` and `error_handler` be called for a single task?**  
A: When the child process got killed after the `success_handler` be called, or the monitor process got killed but the child process still finished the task, both handlers would be called. You can consider it as successful.

13. **Q: How to turn on the debug logs?**  
A: Add a `logging.DEBUG` level handler to `delayed.logger.logger`. The simplest way is to call `delayed.logger.setup_logger()`:
    ```python
    from delayed.logger import setup_logger

    setup_logger()
    ```

14. **Q: Can I enqueue and dequeue tasks in different Python versions?**  
A: It depends on the task type you use:
    * `PickledTask`: If `pickle.HIGHEST_PROTOCOL` is equal among all your Python runtimes, you can use it without any configurations. Otherwise you have to choose the lowest `pickle.HIGHEST_PROTOCOL` of all your Python runtime as the pickle protocol. eg: If you want to enqueue a task in Python 3.7 and dequeue it in Python 2.7, their `pickle.HIGHEST_PROTOCOL` are `4` and `2`, so you need to set the version to `2`:
        ```python
        from delayed.task import set_pickle_protocol_version

        set_pickle_protocol_version(2)
        ```
    * `JsonTask`: It's portable.
    * `ReprTask`: You can't.

15. **Q: What will happen if I changed the pipe capacity?**  
A: `delayed` assumes the pipe capacity is 65536 bytes (the default value on Linux and macOS).
To reduce syscalls, it won't check whether the pipe is writable if the length of data to be written is less than 65536.
If your system has a lower pipe capacity, the `PreforkedWorker` may not working well for some large task.
To fix it, you can set a lower value to `delayed.constants.BUF_SIZE`:
    ```python
    import delayed.constants

    delayed.constants.BUF_SIZE = 1024
    ```
