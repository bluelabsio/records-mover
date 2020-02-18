from collections import namedtuple


class FakeOneRowResultProxy():
    def __init__(self, first_result=None):
        self.first_result = first_result
        self.rowcount = 567

    def first(self):
        return self.first_result

    def close(self):
        pass


class FakeMultiRowResultProxy():
    def __init__(self, results=[]):
        self.results = results

    def fetchall(self):
        return self.results

    def __iter__(self):
        return self.results.__iter__()


class FakeEmptyResultProxy():
    def __init__(self):
        self.rowcount = 0

    def __iter__(self):
        return [].__iter__()


def Struct(**kwargs):
    return namedtuple('Struct', ' '.join(kwargs.keys()))(**kwargs)
