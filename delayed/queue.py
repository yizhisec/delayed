# -*- coding: utf-8 -*-

from .task import Task


class Queue(object):
    def __init__(self, name, conn):
        self._name = name
        self._id_key = name + '_id'
        self._conn = conn

    def enqueue(self, task):
        if task.id is None:
            task.id = self._conn.incr(self._id_key)
        self._conn.rpush(self._name, task.serialize())

    def dequeue(self):
        data = self._conn.blpop(self._name, 1)
        if data:
            return Task.deserialize(data[1])  # ignore key
