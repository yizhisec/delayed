# -*- coding: utf-8 -*-

from .task import Task
from .utils import current_timestamp

# KEYS: queue_name, enqueued_key, dequeued_key
# ARGV: current_timestamp
_DEQUEUE_SCRIPT = '''local task = redis.call('lpop', KEYS[1])
if task == nil then
    return nil
end
local timeout = redis.call('zscore', KEYS[2], task)
redis.call('zadd', KEYS[3], tonumber(ARGV[1]) + timeout, task)
return task'''

# KEYS: queue_name, noti_key, dequeued_key
# ARGV: current_timestamp， busy_len
_REQUEUE_SCRIPT = '''local queue_len = redis.call('llen', KEYS[1])
local noti_len = redis.call('llen', KEYS[2])
local count = queue_len - noti_len
if count > 0 then
    local noti_array = {}
    for i=1,count,1 do
        table.insert(noti_array, '1')
    end
    redis.call('lpush', KEYS[2], unpack(noti_array))
else
    count = 0
end
if queue_len >= tonumber(ARGV[2]) then
    return count
end
local dequeued_tasks = redis.call('zrangebyscore', KEYS[3], 0, ARGV[1])
if #dequeued_tasks == 0 then
    return count
end
if queue_len > 0 then
    local queue = redis.call('lrange', KEYS[1], 0, -1)
    local dequeued_task_dict = {}
    for k, v in pairs(dequeued_tasks) do
        dequeued_task_dict[v] = 1
    end
    for k, v in pairs(queue) do
        dequeued_task_dict[v] = nil
    end
    dequeued_tasks = {}
    for k, v in pairs(dequeued_task_dict) do
        table.insert(dequeued_tasks, k)
    end
    if #dequeued_tasks == 0 then
        return count
    end
end
redis.call('lpush', KEYS[1], unpack(dequeued_tasks))
redis.call('zrem', KEYS[3], unpack(dequeued_tasks))
local noti_array = {}
for i=1,#dequeued_tasks do
    table.insert(noti_array, '1')
end
redis.call('lpush', KEYS[2], unpack(noti_array))
return count + #dequeued_tasks'''

_ENQUEUED_KEY_SUFFIX = '_enqueued'
_DEQUEUED_KEY_SUFFIX = '_dequeued'
_NOTI_KEY_SUFFIX = '_noti'
_ID_KEY_SUFFIX = '_id'


class Queue(object):
    """Queue is the class of a task queue.

    Args:
        name (str): The task queue name.
        conn (redis.Redis): A redis connection.
        default_timeout (int or float): The default timeout in seconds of the task queue.
            A task runs out of time will be killed.
        requeue_timeout (int or float): The requeue timeout in seconds of the task queue.
            Dequeued tasks which started more than `default_timeout` + `requeue_timeout` seconds
            ago can be requeued by a sweeper.
            It should be longer than `kill_timeout` of the `Worker`.
        busy_len (int): The busy length of the task queue.
            If the length of the queue reaches busy_len, it will ignore requeue_lost().
    """

    def __init__(self, name, conn, default_timeout=600, requeue_timeout=10, busy_len=10):
        self._name = name
        self._enqueued_key = name + _ENQUEUED_KEY_SUFFIX
        self._dequeued_key = name + _DEQUEUED_KEY_SUFFIX
        self._noti_key = name + _NOTI_KEY_SUFFIX
        self._id_key = name + _ID_KEY_SUFFIX
        self._conn = conn
        self.default_timeout = default_timeout * 1000
        self._requeue_timeout = requeue_timeout * 1000
        self._busy_len = busy_len
        self._dequeue_script = conn.register_script(_DEQUEUE_SCRIPT)
        self._requeue_script = conn.register_script(_REQUEUE_SCRIPT)

    def enqueue(self, task):
        """Enqueues a task to the queue.

        Args:
            task (delayed.task.Task): The task to be enqueued.
        """
        if task.id is None:
            task.id = self._conn.incr(self._id_key)
        data = task.serialize()
        with self._conn.pipeline() as pipe:
            pipe.rpush(self._name, data)
            pipe.rpush(self._noti_key, '1')
            pipe.zadd(self._enqueued_key, {data: (task.timeout or self.default_timeout) + self._requeue_timeout})
            pipe.execute()

    def dequeue(self):
        """Dequeues a task from the queue.

        Returns:
            delayed.task.Task or None: The dequeued task, or None if the queue is empty.
        """
        if self._conn.blpop(self._noti_key, 1):
            data = self._dequeue_script(
                keys=(self._name, self._enqueued_key, self._dequeued_key),
                args=(current_timestamp(),))
            if data:
                return Task.deserialize(data)

    def requeue(self, task):
        """Enqueues a dequeued task back to the queue.

        Args:
            task (delayed.task.Task): The task to be requeued.
        """
        data = task.data
        if not data:
            return
        with self._conn.pipeline() as pipe:
            pipe.rpush(self._name, data)
            pipe.rpush(self._noti_key, '1')
            pipe.zadd(self._enqueued_key, {data: (task.timeout or self.default_timeout) + self._requeue_timeout})
            pipe.zrem(self._dequeued_key, data)
            pipe.execute()

    def release(self, task):
        """Releases a dequeued task.
         It should be called after finishing a task.

        Args:
            task (delayed.task.Task): The task to be release.
        """
        with self._conn.pipeline() as pipe:
            pipe.zrem(self._enqueued_key, task.data)
            pipe.zrem(self._dequeued_key, task.data)
            pipe.execute()

    def len(self):
        """Returns the length of the queue."""
        return self._conn.llen(self._name)

    def requeue_lost(self):
        """Requeues lost tasks.
        It should be called periodically to prevent losing tasks.
        The lost tasks were those popped from the queue, but not existed in the dequeued key.
        It won't requeue lost tasks if the queue is busy.
        """
        return self._requeue_script(
            keys=(self._name, self._noti_key, self._dequeued_key),
            args=(current_timestamp(), self._busy_len))
