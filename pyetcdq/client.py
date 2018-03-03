from .const import TASK_PREFIX
from .task import Task


class Client(object):
    def __init__(self, etcd_client, prefix):
        self._client = etcd_client
        self._prefix = prefix

    def submit(self, data):
        result = self._client.write("%s/%s/" % (self._prefix, TASK_PREFIX), data, append=True)

        return Task(result.key, data)
