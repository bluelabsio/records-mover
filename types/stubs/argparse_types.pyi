from typing import Iterable, Any, Union
from mypy_extensions import TypedDict


class ArgParseArgument(TypedDict, total=False):
    # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument
    action: str
    nargs: Union[str, int]
    const: object
    default: object
    type: Any
    choices: Iterable[Any]
    required: bool
    help: str
    metavar: str
    dest: str
