import functools
from typing import Callable, TypeVar


A = TypeVar('A')
B = TypeVar('B')


# https://stackoverflow.com/questions/3012421/python-memoising-deferred-lookup-property-decorator
def lazyprop(fn: Callable[[A], B]) -> B:
    attr_name = '_lazy_' + fn.__name__

    # https://github.com/python/mypy/issues/1362
    @property  # type: ignore
    @functools.wraps(fn)
    def _lazyprop(self: A) -> B:
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazyprop  # type: ignore
