class FixName(object):
    def __init__(self, file_, name):
        self._file = file_
        self.name = name

    def __getattr__(self, attr):
        return getattr(self._file, attr)
