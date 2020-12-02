from enum import Enum
from typing import Dict, Any, Optional, Union, Mapping, List, NoReturn

JsonSchema = Dict[str, Any]

JobConfig = Dict[str, Any]

JsonValue = Optional[Union[bool, str, float, int, Mapping[str, Any], List[Any]]]


# mypy way of validating we're covering all cases of an enum.  This
# version includes an assert at runtime in case poorly-typed input is
# given.
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
def _assert_never(x: NoReturn, errmsg: Optional[str] = None) -> NoReturn:
    if errmsg is None:
        errmsg = "Unhandled type: {}".format(type(x).__name__)
    assert False, errmsg


# mypy way of validating we're covering all cases of an enum.  This
# version allows poorly typed things to pass through at runtime.
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
def _ensure_all_cases_covered(x: NoReturn) -> NoReturn:
    pass


# mypy-friendly way of doing a singleton object:
#
# https://github.com/python/typing/issues/236
class PleaseInfer(Enum):
    token = 1
