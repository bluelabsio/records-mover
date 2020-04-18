from .constraints import RecordsSchemaFieldConstraints
from typing import Optional, Tuple, cast, TYPE_CHECKING
if TYPE_CHECKING:
    import sqlalchemy
    from .constraints import FieldStringConstraintsDict  # noqa
    from records_mover.db import DBDriver  # noqa


class RecordsSchemaFieldStringConstraints(RecordsSchemaFieldConstraints):
    def __init__(self,
                 required: bool,
                 unique: Optional[bool],
                 max_length_bytes: Optional[int],
                 max_length_chars: Optional[int]):
        super().__init__(required=required, unique=unique)
        self.max_length_bytes = max_length_bytes
        self.max_length_chars = max_length_chars

    @staticmethod
    def string_type_constraints(type_: 'sqlalchemy.types.String',
                                driver: 'DBDriver') ->\
            Tuple[Optional[int], Optional[int]]:
        max_length_bytes = None
        max_length_chars = None
        if driver.varchar_length_is_in_chars():
            max_length_chars = type_.length
        else:
            max_length_bytes = type_.length
        return (max_length_bytes, max_length_chars)

    @staticmethod
    def from_sqlalchemy_type(required: bool,
                             unique: Optional[bool],
                             type_: 'sqlalchemy.types.TypeEngine',
                             driver: 'DBDriver') -> 'RecordsSchemaFieldStringConstraints':
        import sqlalchemy

        if not isinstance(type_, sqlalchemy.types.String):
            raise TypeError(f"Unexpected column type: {type_}")
        max_length_bytes, max_length_chars =\
            RecordsSchemaFieldStringConstraints.string_type_constraints(type_,
                                                                        driver)
        return RecordsSchemaFieldStringConstraints(required=required,
                                                   unique=unique,
                                                   max_length_bytes=max_length_bytes,
                                                   max_length_chars=max_length_chars)

    def to_data(self) -> 'FieldStringConstraintsDict':
        raw_out = super().to_data()
        out = cast('FieldStringConstraintsDict', raw_out)
        if self.max_length_bytes is not None:
            out['max_length_bytes'] = self.max_length_bytes
        if self.max_length_chars is not None:
            out['max_length_chars'] = self.max_length_chars
        return out
