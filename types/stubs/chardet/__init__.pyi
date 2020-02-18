# https://chardet.readthedocs.io/en/latest/_modules/chardet/universaldetector.html
from typing import Optional
from mypy_extensions import TypedDict


class Result(TypedDict):
    encoding: Optional[str]
    confidence: float
    language: Optional[str]


class UniversalDetector:
    done: Optional[bool]
    result: Optional[Result]

    def feed(self, chunk: bytes) -> None:
        ...

    def close(self) -> Result:
        ...
