"""In-memory state: independent locks let KV and queue operations overlap."""
from collections import deque
import threading


class Storage:
    def __init__(self):
        self._values = {}
        self._values_lock = threading.Lock()
        self._tasks = deque()
        self._available = threading.Condition(threading.Lock())

    def put(self, key, value):
        with self._values_lock:
            self._values[key] = value

    def get(self, key):
        with self._values_lock:
            # A presence flag distinguishes stored JSON null from a missing key.
            return key in self._values, self._values.get(key)

    def enqueue(self, task):
        with self._available:
            self._tasks.append(task)
            self._available.notify()

    def dequeue(self, timeout=0):
        with self._available:
            # wait_for releases the lock while waiting and rechecks the predicate
            # after waking, including spurious wakeups or competing consumers.
            if not self._available.wait_for(lambda: bool(self._tasks), timeout):
                return False, None
            return True, self._tasks.popleft()
