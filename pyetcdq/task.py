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
