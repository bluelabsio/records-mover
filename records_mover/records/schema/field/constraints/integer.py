from .constraints import RecordsSchemaFieldConstraints
from typing import Optional, cast, TYPE_CHECKING
if TYPE_CHECKING:
    import sqlalchemy
    from records_mover.db import DBDriver  # noqa
    from .constraints import FieldIntegerConstraintsDict  # noqa


class RecordsSchemaFieldIntegerConstraints(RecordsSchemaFieldConstraints):
    def __init__(self,
                 required: bool,
                 unique: Optional[bool],
                 min_: Optional[int],
                 max_: Optional[int]):
        super().__init__(required=required, unique=unique)
        self.min_ = min_
        self.max_ = max_

    @staticmethod
    def from_sqlalchemy_type(required: bool,
                             unique: Optional[bool],
                             type_: 'sqlalchemy.types.TypeEngine',
                             driver: 'DBDriver') -> 'RecordsSchemaFieldIntegerConstraints':
        import sqlalchemy

        if not isinstance(type_, sqlalchemy.types.Integer):
            raise TypeError(f"Unexpected column type: {type_}")
        limits = driver.integer_limits(type_)
        min_: Optional[int] = None
        max_: Optional[int] = None
        if limits:
            min_, max_ = limits

        return RecordsSchemaFieldIntegerConstraints(required=required,
                                                    unique=unique,
                                                    min_=min_,
                                                    max_=max_)

    def to_data(self) -> 'FieldIntegerConstraintsDict':
        raw_out = super().to_data()
        out = cast('FieldIntegerConstraintsDict', raw_out)
        if self.min_ is not None:
            out['min'] = str(self.min_)
        if self.max_ is not None:
            out['max'] = str(self.max_)
        return out
