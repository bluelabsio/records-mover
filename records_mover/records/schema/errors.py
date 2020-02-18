class DeserializationError(Exception):
    pass


class UnsupportedSchemaError(DeserializationError):
    pass
