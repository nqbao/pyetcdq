class Task(object):
    """
    Task object
    """
    def __init__(self, tid, data=None, result=None):
        self._tid = tid
        self.data = data or {}
        self.result = result

    @property
    def tid(self):
        return self._tid

    def __repr__(self):
        return "Task(%s)" % self._tid
