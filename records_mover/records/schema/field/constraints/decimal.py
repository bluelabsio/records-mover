from .constraints import RecordsSchemaFieldConstraints
from typing import Optional, cast, TYPE_CHECKING
if TYPE_CHECKING:
    import sqlalchemy
    from .constraints import FieldDecimalConstraintsDict  # noqa
    from records_mover.db import DBDriver  # noqa


class RecordsSchemaFieldDecimalConstraints(RecordsSchemaFieldConstraints):
    def __init__(self,
                 required: bool,
                 unique: Optional[bool],
                 fixed_precision: Optional[int]=None,
                 fixed_scale: Optional[int]=None,
                 fp_total_bits: Optional[int]=None,
                 fp_significand_bits: Optional[int]=None):
        super().__init__(required=required, unique=unique)
        self.fixed_precision = fixed_precision
        self.fixed_scale = fixed_scale
        self.fp_total_bits = fp_total_bits
        self.fp_significand_bits = fp_significand_bits

    @staticmethod
    def from_sqlalchemy_type(required: bool,
                             unique: Optional[bool],
                             type_: 'sqlalchemy.types.TypeEngine',
                             driver: 'DBDriver') -> 'RecordsSchemaFieldDecimalConstraints':
        import sqlalchemy

        if not isinstance(type_, sqlalchemy.types.Numeric):
            raise TypeError(f"Unexpected column type: {type_}")

        if isinstance(type_, sqlalchemy.types.Float):
            constraints = driver.fp_constraints(type_)
            fp_total_bits: Optional[int] = None
            fp_significand_bits: Optional[int] = None
            if constraints:
                fp_total_bits, fp_significand_bits = constraints

            return RecordsSchemaFieldDecimalConstraints(required=required,
                                                        unique=unique,
                                                        fp_total_bits=fp_total_bits,
                                                        fp_significand_bits=fp_significand_bits)
        else:
            constraints = driver.fixed_point_constraints(type_)
            fixed_precision: Optional[int] = None
            fixed_scale: Optional[int] = None
            if constraints:
                fixed_precision, fixed_scale = constraints

            return RecordsSchemaFieldDecimalConstraints(required=required,
                                                        unique=unique,
                                                        fixed_precision=fixed_precision,
                                                        fixed_scale=fixed_scale)

    def to_data(self) -> 'FieldDecimalConstraintsDict':
        raw_out = super().to_data()
        out = cast('FieldDecimalConstraintsDict', raw_out)
        if self.fixed_precision is not None:
            out['fixed_precision'] = self.fixed_precision
        if self.fixed_scale is not None:
            out['fixed_scale'] = self.fixed_scale
        if self.fp_total_bits is not None:
            out['fp_total_bits'] = self.fp_total_bits
        if self.fp_significand_bits is not None:
            out['fp_significand_bits'] = self.fp_significand_bits
        return out
