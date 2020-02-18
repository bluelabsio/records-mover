# https://github.com/python/typeshed/issues/2171

from typing import Iterable, List


# https://github.com/pypa/setuptools/blob/master/setuptools/__init__.py
def setup(**attrs) -> None:
    ...


def find_packages(where: str = '.',
                  exclude: Iterable[str] = (),
                  include: Iterable[str] = ('*',)) -> List[str]:
    ...
