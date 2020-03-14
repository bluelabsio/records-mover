from .json_schema_document import JsonSchemaDocument


class JsonParameter:
    def __init__(self,
                 name: str,
                 json_schema_document: JsonSchemaDocument,
                 optional: bool = False) -> None:
        self.name = name
        self.optional = optional
        self.json_schema_document = json_schema_document
