import inspect
from ..types import JsonSchema
from typing import Any, Optional, List, Union


# default is set to inspect.Parameter.empty if no default
DefaultValue = object


class JsonSchemaDocument:
    """Represents JSON Schema"""
    def __init__(self,
                 json_type: Union[str, List[str]],
                 default: DefaultValue=inspect.Parameter.empty,
                 enum: Optional[List[Any]]=None,
                 description: Optional[str]=None) -> None:
        self.json_type = json_type
        self.default = default
        self.enum = enum
        self.description = description

    def to_data(self) -> JsonSchema:
        out: JsonSchema = {
            'type': self.json_type
        }
        if self.enum is not None:
            out['enum'] = self.enum
        if self.description is not None:
            out['description'] = self.description
        if not self.default == inspect.Parameter.empty:
            out['default'] = self.default
        return out
