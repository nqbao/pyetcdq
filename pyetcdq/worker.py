from __future__ import absolute_import
from __future__ import print_function

import etcd
import threading
import time
import json
from datetime import datetime
from .const import WORKER_PREFIX, TASK_PREFIX, QUEUE_PREFIX, TTL
from .client import Client


class Worker(object):
    def __init__(self, etcd_client, prefix):
        self._etcd = etcd_client
        self._prefix = prefix
        self._client = Client(self._etcd, prefix)
        self._heartbeat_thread = None
        self._worker_key = None
        self._stop_event = threading.Event()
        self._acquired_tasks = {}

    def start(self):
        """
        Start the worker and watch for tasks
        """
        self._stop_event.clear()

        # register myself to worker list
        self._worker_key = self._etcd.write(self._key(WORKER_PREFIX), "", append=True, ttl=TTL * 2).key

        self._heartbeat_thread = threading.Thread(target=self._beat)
        self._heartbeat_thread.daemon = True

        # kick start
        self._heartbeat_thread.start()

    def stop(self, wait=False):
        """
        Stop worker
        """
        self._stop_event.set()
        self._etcd.delete(self._worker_key)

        # wait until the thread exit
        while wait and self._heartbeat_thread:
            time.sleep(1)

    def wait_for_task(self, timeout=None):
        """
        Wait for a task available. This will block until a task is available or timeout.
        """
        # try to acquire pending task first
        task = self._acquire_pending_task()
        if task:
            return task

        # then try to watch for new tasks
        start = datetime.now()
        watch_timeout = 5

        if timeout:
            watch_timeout = timeout / 2
            assert watch_timeout >= 1, "timeout is too small"

        while not task:
            if timeout and (datetime.now() - start).seconds > timeout:
                raise RuntimeError("Timeout while waiting for task")

            try:
                # ideally, we should watch for the queues folder
                result = self._etcd.watch(self._prefix, recursive=True, timeout=watch_timeout)
                key = result.key[len(self._prefix) + 1:].split('/')
                prefix = key[0]

                if prefix == QUEUE_PREFIX and result.action in ('create', 'set'):
                    self._log("Got new task %s" % result.value)
                    task = self._acquire_task(result)
                elif prefix == TASK_PREFIX and result.action == 'expire':
                    # this means a the task is lost, try to rerun it
                    if key[-1] == 'state':
                        self._log("Task %s is lost, try to acquire it" % key[1])
                        task = self._acquire_pending_task()
                elif prefix == WORKER_PREFIX:
                    tid = key[1]
                    if result.action == 'create':
                        self._log('New worker joins: %s' % tid)
                    elif result.action == 'expire':
                        self._log('Worker %s is lost' % tid)
                    elif result.action == 'delete':
                        self._log('Worker %s leaves' % tid)
                    else:
                        self._log("Ignore worker event: %s %s" % (result.action, key))
                else:
                    self._log("Ignore event %s %s" % (result.action, key))
            except etcd.EtcdWatchTimedOut:
                pass

        return task

    def release_task(self, task):
        """
        Release the task from current worker
        """
        # TODO: validate if this task was really acquired by us
        assert task
        self._log("Remove %s from processing list" % task.tid)
        self._etcd.delete(self._key(QUEUE_PREFIX, task.tid))
        del self._acquired_tasks[task.tid]

    def finish_task(self, task, result=None, error=None):
        """
        Mark a task as finish and also release it
        """
        # TODO: validate if this task was really acquired by us
        assert task
        self._log("Finishing task %s" % task.tid)
        with self._client.lock_task(task):
            # XXX: this is not atomic, any of this could fail
            result_key = self._key(TASK_PREFIX, task.tid, "result")
            state_key = self._key(TASK_PREFIX, task.tid, "state")
            error_key = self._key(TASK_PREFIX, task.tid, "error")

            if error:
                self._etcd.write(error_key, json.dumps(error))
                self._etcd.write(state_key, json.dumps('FAILURE'))
            else:
                self._etcd.write(result_key, json.dumps(result))
                self._etcd.write(state_key, json.dumps('SUCCESS'))

        self.release_task(task)

    def _beat(self):
        while not self._stop_event.is_set():
            # TODO: also do heartbeat for pending tasks
            if self._worker_key:
                self._etcd.refresh(self._worker_key, ttl=TTL)

            acquired_tasks = self._acquired_tasks.keys()
            if acquired_tasks:
                for tid in acquired_tasks:
                    try:
                        self._etcd.refresh(self._key(TASK_PREFIX, tid, "state"), ttl=TTL)
                    except Exception as ex:
                        self._log("Unable to refresh task %s: %s" % (tid, ex))

            time.sleep(TTL / 2)

    def _acquire_pending_task(self):
        # try to acquire any pending task in queue
        result = None
        try:
            result = self._etcd.read(self._key(QUEUE_PREFIX))
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
        queue_data = json.loads(result.value)
        task = self._client.get_task(queue_data.get('id'))

        # try to lock state key
        try:
            state_key = self._key(TASK_PREFIX, task.tid, "state")
            self._etcd.write(state_key, json.dumps("ACQUIRED"), prevExist=False, ttl=TTL)
            self._acquired_tasks[task.tid] = task

            self._log("Task %s acquired successfully" % task.tid)
            return task
        except etcd.EtcdAlreadyExist:
            # this is being acquired by someone else
            self._log("Task %s is already acquired" % task.tid)
            return

    def _key(self, *args):
        return "%s/%s" % (self._prefix, "/".join(args))

    @staticmethod
    def _log(msg):
        print(msg)
