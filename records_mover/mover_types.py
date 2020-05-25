from typing import Dict, Any, Optional, Union, Mapping, List, NoReturn

JsonSchema = Dict[str, Any]

JobConfig = Dict[str, Any]

JsonValue = Optional[Union[bool, str, float, int, Mapping[str, Any], List[Any]]]


# mypy way of validating we're covering all cases of an enum
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
def _assert_never(x: NoReturn) -> NoReturn:
    assert False, "Unhandled type: {}".format(type(x).__name__)
