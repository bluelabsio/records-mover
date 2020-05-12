import inspect
from typing import Optional
from ..mover_types import JsonSchema
from .json_schema_document import JsonSchemaDocument, DefaultValue


class JsonSchemaArrayDocument(JsonSchemaDocument):
    """Represents a JSON Schema array type"""
    def __init__(self,
                 json_type: str,
                 items: JsonSchemaDocument,
                 default: DefaultValue = inspect.Parameter.empty,
                 description: Optional[str] = None) -> None:
        super().__init__(json_type=json_type, default=default, description=description)
        self.items = items

    def to_data(self) -> JsonSchema:
        out = super().to_data()
        out['items'] = self.items.to_data()
        return out
