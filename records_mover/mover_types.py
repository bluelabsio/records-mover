from typing import Dict, Any, Optional, Union, Mapping, List

JsonSchema = Dict[str, Any]

JobConfig = Dict[str, Any]

JsonValue = Optional[Union[bool, str, float, int, Mapping[str, Any], List[Any]]]
