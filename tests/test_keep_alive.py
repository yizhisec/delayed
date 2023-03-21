from threading import Timer

from delayed.queue import Queue
from delayed.worker import Worker

from .common import QUEUE_NAME, CONN


def test_keep_alive_thread():
    queue = Queue(QUEUE_NAME, CONN, 0.01, keep_alive_timeout=0.2)
    worker = Worker(queue, keep_alive_interval=0.01)
    t = Timer(0.1, Worker.stop, args=(worker,))
    t.start()
    worker.run()
    t.join()
    assert not CONN.exists(worker._id)
