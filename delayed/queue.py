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
        if task.id is None:
            task.id = self._conn.incr(self._id_key)
        data = task.serialize()
        with self._conn.pipeline() as pipe:
            pipe.rpush(self._name, data)
            pipe.rpush(self._noti_key, '1')
            pipe.zadd(self._enqueued_key, {data: (task.timeout or self.default_timeout) + self._requeue_timeout})
            pipe.execute()

    def dequeue(self):
        if self._conn.blpop(self._noti_key, 1):
            data = self._dequeue_script(
                keys=(self._name, self._enqueued_key, self._dequeued_key),
                args=(current_timestamp(),))
            if data:
                return Task.deserialize(data)

    def release(self, task):  # should call it after finishing the task
        with self._conn.pipeline() as pipe:
            pipe.zrem(self._enqueued_key, task.data)
            pipe.zrem(self._dequeued_key, task.data)
            pipe.execute()

    def len(self):
        return self._conn.llen(self._name)

    def requeue_lost(self):
        # should call it periodically to prevent losing tasks
        # the lost tasks were those popped from the queue but not existed in the dequeued key
        return self._requeue_script(
            keys=(self._name, self._noti_key, self._dequeued_key),
            args=(current_timestamp(), self._busy_len))
