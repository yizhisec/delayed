# -*- coding: utf-8 -*-

from .task import Task
from .utils import current_timestamp


# KEYS: queue_name enqueued_key
# ARGV: before_timestamp
REQUEUE_SCRIPT = '''local enqueued_tasks = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1])
if #enqueued_tasks == 0 then
    return 0
end
local queue = redis.call('lrange', KEYS[1], 0, -1)
for k, v in pairs(queue) do
    table.remove(enqueued_tasks, k)
end
for k, v in pairs(enqueued_tasks) do
    redis.call('lpush', KEYS[1], v)
end
return #enqueued_tasks
'''

_ENQUEUED_KEY_SUFFIX = '_enqueued'
_DEQUEUED_KEY_SUFFIX = '_dequeued'
_ID_KEY_SUFFIX = '_id'


class Queue(object):
    def __init__(self, name, conn, busy_len=10):
        self._name = name
        self._enqueued_key = name + _ENQUEUED_KEY_SUFFIX
        self._dequeued_key = name + _DEQUEUED_KEY_SUFFIX
        self._id_key = name + _ID_KEY_SUFFIX
        self._conn = conn
        self._busy_len = busy_len
        self._script = conn.register_script(REQUEUE_SCRIPT)

    def enqueue(self, task):
        if task.id is None:
            task.id = self._conn.incr(self._id_key)
        data = task.serialize()
        with self._conn.pipeline() as pipe:
            pipe.rpush(self._name, data)
            pipe.zadd(self._enqueued_key, {data: current_timestamp()})
            pipe.execute()

    def dequeue(self):
        data = self._conn.blpop(self._name, 1)
        if data:
            data = data[1]  # ignore key
            with self._conn.pipeline() as pipe:
                pipe.zrem(self._enqueued_key, data)
                pipe.zadd(self._dequeued_key, {data: current_timestamp()})
                pipe.execute()
            return Task.deserialize(data)

    def release(self, task):  # should call it after finishing the task
        self._conn.zrem(self._dequeued_key, task.data)

    def len(self):
        return self._conn.llen(self._name)

    def requeue_lost(self, timeout):
        # should call it periodically to prevent losing tasks
        # the lost tasks were those popped from the queue but not existed in the dequeued key
        if self.len() >= self._busy_len:  # the queue is busy now, should requeue tasks later
            return 0
        before = current_timestamp() - timeout
        return self._script(keys=(self._name, self._enqueued_key), args=(before,))
