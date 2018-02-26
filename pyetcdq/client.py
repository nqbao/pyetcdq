from .const import TASK_PREFIX


class Client(object):
    def __init__(self, etcd_client, prefix):
        self._client = etcd_client
        self._prefix = prefix

    def submit(self, task):
        result = self._client.write("%s/%s/" % (self._prefix, TASK_PREFIX), task, append=True)

        return result.key
