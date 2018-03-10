from .const import TASK_PREFIX, QUEUE_PREFIX, TTL
from .task import Task
import json
import uuid
import datetime
import contextlib


class Client(object):
    def __init__(self, etcd_client, prefix):
        self._etcd = etcd_client
        self._prefix = prefix

    def submit(self, data):
        tid = str(uuid.uuid4())
        self._etcd.write(self._key(TASK_PREFIX, tid, "data"), json.dumps(data))

        # add task to queue
        self._etcd.write(self._key(QUEUE_PREFIX, tid), json.dumps({"id": tid}))

        return Task(tid, data)

    def get_task(self, tid):
        """
        Get task by id

        :rtype Task
        """
        result = self._etcd.read(self._key(TASK_PREFIX, tid))
        task = Task(tid)
        for child in result.children:
            key = child.key.split('/')[-1]
            setattr(task, key, json.loads(child.value))

        return task

    def cancel(self, task):
        cancel_key = self._key(TASK_PREFIX, task.tid, 'cancelled')
        self._etcd.write(cancel_key, json.dumps({
            "requested_at": datetime.datetime.utcnow().isoformat()
        }), prevExist=False)

    @contextlib.contextmanager
    def lock_task(self, task, ttl=TTL):
        tid = task.tid
        lock_key = self._key(TASK_PREFIX, tid, 'lock')

        self._etcd.write(lock_key, "", ttl=ttl, prevExist=False)
        try:
            yield
        finally:
            self._etcd.delete(lock_key)

    def _key(self, *args):
        return "%s/%s" % (
            self._prefix,
            "/".join(args)
        )
