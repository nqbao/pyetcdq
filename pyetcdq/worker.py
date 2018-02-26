import etcd
import threading
import time
import queue
import contextlib
from .const import LOCK_PREFIX, WORKER_PREFIX, TASK_PREFIX, STATE_PREFIX, RESULT_PREFIX, TTL


class Task(object):
    def __init__(self, key):
        self.key = key
        self.tid = key.split('/')[-1]


class Worker(object):
    def __init__(self, etcd_client, prefix):
        self._client = etcd_client
        self._prefix = prefix
        self._watch_thread = None
        self._heartbeat_thread = None
        self._worker_key = None
        self._stop_event = threading.Event()
        self._tasks = queue.Queue(1)
        self._processing_tasks = queue.Queue(1)

    def start(self):
        """
        Start the worker
        """
        assert not self._watch_thread, "Worker has already started."
        self._stop_event.clear()

        # register myself to worker list
        self._worker_key = self._client.write("%s/%s" % (self._prefix, WORKER_PREFIX), "", append=True, ttl=TTL * 2).key

        self._heartbeat_thread = threading.Thread(target=self._beat)
        self._heartbeat_thread.daemon = True

        self._watch_thread = threading.Thread(target=self._watch)
        self._watch_thread.daemon = True

        # kick start
        self._watch_thread.start()
        self._heartbeat_thread.start()

    def stop(self, wait=False):
        assert self._watch_thread, "Worker does not start yet."

        self._stop_event.set()
        self._client.delete(self._worker_key)

        # wait until the thread exit
        while wait and self._watch_thread and self._heartbeat_thread:
            time.sleep(1)

    def acquire_task(self):
        while True:
            if not self._tasks.empty():
                task = self._tasks.get()

                # TODO: we need a dictionary
                self._processing_tasks.put(task)

                return task
            else:
                time.sleep(1)

    def finish_task(self, tid):
        # XXX: this is not atomic
        self._log("Finishing task %s" % tid)
        with self.lock_task(tid):
            task_key = "%s/%s/%s" % (self._prefix, TASK_PREFIX, tid)
            result_key = "%s/%s/%s" % (self._prefix, RESULT_PREFIX, tid)
            state_key = "%s/%s/%s" % (self._prefix, STATE_PREFIX, tid)

            self._client.write(result_key, tid)
            self._client.delete(task_key)
            self._client.delete(state_key)

        self._log("Remove %s from processing list" % tid)
        self._tasks.task_done()
        self._processing_tasks.get()

    def _watch(self):
        """
        Watch for incoming tasks
        """
        while not self._stop_event.is_set():
            # wait until all tasks are processed
            if not self._processing_tasks.empty() or not self._tasks.empty():
                self._log("Wait for next run ...")
                time.sleep(1)
                continue

            # try to acquire pending task first
            if self._acquire_pending_task():
                continue

            # then try to watch for new tasks
            try:
                result = self._client.watch(self._prefix, recursive=True, timeout=5)
                key = result.key[len(self._prefix) + 1:].split('/')
                prefix = key[0]
                tid = key[1]

                if result.action == 'create' and prefix == TASK_PREFIX:
                    self._log("Got new task %s" % tid)
                    self._acquire_task(result)
                elif result.action == 'expire' and prefix == STATE_PREFIX:
                    self._log("Task is expired %s" % tid)
                    self._acquire_task(result)
                else:
                    print result.action, key
            except etcd.EtcdWatchTimedOut:
                pass

        self._watch_thread = None

    def _beat(self):
        while not self._stop_event.is_set():
            # TODO: also do heartbeat for pending tasks
            if self._worker_key:
                self._client.refresh(self._worker_key, ttl=TTL)
            time.sleep(TTL / 2)

    def _acquire_pending_task(self):
        # try to acquire any pending task
        result = None
        try:
            result = self._client.read("%s/%s" % (self._prefix, TASK_PREFIX))
        except etcd.EtcdKeyNotFound:
            # there is no task yet, just move on
            return

        for child in result.children:
            if child.dir:
                continue

            self._log("Try to acquire pending task: %s" % child.key)
            if self._acquire_task(child):
                break

    def _acquire_task(self, result):
        tid = result.key.split("/")[-1]

        # TODO: ignore if task is acquired by current worker

        # try to lock state key
        try:
            state_key = "%s/%s/%s" % (self._prefix, STATE_PREFIX, tid)
            self._client.write(state_key, "STARTED", prevExist=False, ttl=TTL)

            self._tasks.put(tid)
            self._log("Task %s acquired successfully" % tid)
            return tid
        except etcd.EtcdAlreadyExist:
            # this is being acquired by someone else
            self._log("Task %s is already acquired" % tid)
            return

    def _change_task_state(self, tid, state):
        state_key = "%s/%s/%s" % (self._prefix, STATE_PREFIX, tid)
        self._client.write(state_key, state, ttl=TTL)

    @contextlib.contextmanager
    def lock_task(self, tid, ttl=TTL):
        lock_key = "%s/%s/%s" % (self._prefix, LOCK_PREFIX, tid)

        self._client.write(lock_key, self._worker_key, ttl=ttl, prevExist=False)
        try:
            yield
        finally:
            self._client.delete(lock_key)

    def _log(self, msg):
        print(msg)
