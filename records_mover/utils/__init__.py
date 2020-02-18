import os
from typing import List, TypeVar, MutableSet, Union
import warnings
import functools


def filenames_from_local_directory(directory: str) -> List[str]:
    files: List[str] = []
    for dirpath, dirnames, filenames in os.walk(directory):
        files += [
            "{0}/{1}".format(dirpath[len(directory):], filename)[1:]  # Strips leading /
            for filename in filenames
        ]
    return files


def idempotent_path(path: str) -> str:
    if not os.path.exists(path):
        os.makedirs(path)

    return path


T = TypeVar('T')


def quiet_remove(coll: Union[List[T], MutableSet[T]], element: T) -> None:
    try:
        coll.remove(element)
    except KeyError:
        pass


# https://stackoverflow.com/questions/2536307/decorators-in-the-python-standard-lib-deprecated-specifically
def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


class BetaWarning(Warning):
    pass


def beta(func):
    """This is a decorator which can be used to mark functions as beta
    (interface may change in future). It will result in a warning
    being emitted when the function is used.
    """
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', BetaWarning)  # turn off filter
        warnings.warn("Call to beta function {} - "
                      "interface may change in the future!.".format(func.__name__),
                      category=BetaWarning,
                      stacklevel=2)
        warnings.simplefilter('default', BetaWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func
