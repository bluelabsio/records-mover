from enum import Enum
from typing import Dict, Any, Optional, Union, Mapping, List

JsonSchema = Dict[str, Any]

JobConfig = Dict[str, Any]

JsonValue = Optional[Union[bool, str, float, int, Mapping[str, Any], List[Any]]]


# This is a mypy-friendly way of doing a singleton object:
#
# https://github.com/python/typing/issues/236
class NotYetFetched(Enum):
    token = 1
