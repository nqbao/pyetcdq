from __future__ import absolute_import
from __future__ import print_function

import etcd
import threading
import time
import contextlib
from .const import LOCK_PREFIX, WORKER_PREFIX, TASK_PREFIX, STATE_PREFIX, RESULT_PREFIX, TTL


class Task(object):
    """
    Task object
    """
    def __init__(self, key, data, result=None):
        self._key = key
        self._tid = key.split('/')[-1]
        self.data = data or {}
        self.result = result

    @property
    def key(self):
        return self._key

    @property
    def tid(self):
        return self._tid

    def __repr__(self):
        return "Task(%s)" % self._tid


class Worker(object):
    def __init__(self, etcd_client, prefix):
        self._client = etcd_client
        self._prefix = prefix
        self._heartbeat_thread = None
        self._worker_key = None
        self._stop_event = threading.Event()

    def start(self):
        """
        Start the worker and watch for tasks
        """
        self._stop_event.clear()

        # register myself to worker list
        self._worker_key = self._client.write("%s/%s" % (self._prefix, WORKER_PREFIX), "", append=True, ttl=TTL * 2).key

        self._heartbeat_thread = threading.Thread(target=self._beat)
        self._heartbeat_thread.daemon = True

        # kick start
        self._heartbeat_thread.start()

    def stop(self, wait=False):
        """
        Stop worker
        """
        self._stop_event.set()
        self._client.delete(self._worker_key)

        # wait until the thread exit
        while wait and self._heartbeat_thread:
            time.sleep(1)

    def acquire_task(self):
        """
        Acquire a task from the pool. This will block until a task is acquired.
        """
        # try to acquire pending task first
        task = self._acquire_pending_task()
        if task:
            return task

        # then try to watch for new tasks
        while not task:
            try:
                result = self._client.watch(self._prefix, recursive=True, timeout=5)
                key = result.key[len(self._prefix) + 1:].split('/')
                prefix = key[0]
                tid = key[1]

                if result.action == 'create' and prefix == TASK_PREFIX:
                    self._log("Got new task %s" % tid)
                    task = self._acquire_task(result)
                elif result.action == 'expire' and prefix == STATE_PREFIX:
                    self._log("Task is expired %s" % tid)
                    task = self._acquire_task(result)
                else:
                    print(result.action, key)
            except etcd.EtcdWatchTimedOut:
                pass

        return task

    def release_task(self, task):
        """
        Release the task from current worker
        """
        assert task
        self._log("Remove %s from processing list" % task.tid)
        self._client.delete(self._key(STATE_PREFIX, task.tid))

    def finish_task(self, task, result=None):
        """
        Mark a task as finish and also release it
        """
        assert task
        self._log("Finishing task %s" % task.tid)
        with self.lock_task(task):
            # XXX: this is not atomic, any of this could fail
            task_key = "%s/%s/%s" % (self._prefix, TASK_PREFIX, task.tid)
            result_key = "%s/%s/%s" % (self._prefix, RESULT_PREFIX, task.tid)

            self._client.write(result_key, result)
            self._client.delete(task_key)

        self.release_task(task)

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
            result = self._client.read(self._key(TASK_PREFIX))
        except etcd.EtcdKeyNotFound:
            # there is no task yet, just move on
            return

        for child in result.children:
            if child.dir:
                continue

            self._log("Try to acquire pending task: %s" % child.key)
            task = self._acquire_task(child)
            if task:
                return task

    def _acquire_task(self, result):
        """
        Try to acquire a task. Return Task object if successfully

        :rtype Task
        """
        task = self._task(result)

        # TODO: ignore if task is acquired by current worker

        # try to lock state key
        try:
            state_key = self._key(STATE_PREFIX, task.tid)
            self._client.write(state_key, "STARTED", prevExist=False, ttl=TTL)

            self._log("Task %s acquired successfully" % task.tid)
            return task
        except etcd.EtcdAlreadyExist:
            # this is being acquired by someone else
            self._log("Task %s is already acquired" % task.tid)
            return

    @contextlib.contextmanager
    def lock_task(self, task, ttl=TTL):
        tid = task.tid
        lock_key = self._key(LOCK_PREFIX, tid)

        self._client.write(lock_key, self._worker_key, ttl=ttl, prevExist=False)
        try:
            yield
        finally:
            self._client.delete(lock_key)

    def _key(self, *args):
        return "%s/%s" % (self._prefix, "/".join(args))

    @staticmethod
    def _log(msg):
        print(msg)

    @staticmethod
    def _task(etcd_result):
        """
        Helper method to create task from etcd result

        :rtype Task
        """
        return Task(etcd_result.key, etcd_result.value)
