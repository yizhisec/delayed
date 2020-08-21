# -*- coding: utf-8 -*-

import redis
from delayed.queue import Queue


conn = redis.Redis()
queue = Queue('test', conn)
