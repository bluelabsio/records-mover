import logging
from records_mover.mover_types import _assert_never
from typing import Optional, cast, TYPE_CHECKING
from records_mover.utils.limits import (FLOAT16_SIGNIFICAND_BITS,
                                        FLOAT32_SIGNIFICAND_BITS,
                                        FLOAT64_SIGNIFICAND_BITS,
                                        FLOAT80_SIGNIFICAND_BITS)
if TYPE_CHECKING:
    import sqlalchemy
    import numpy as np
    from records_mover.db import DBDriver  # noqa
    from mypy_extensions import TypedDict

    class MandatoryFieldConstraintsDict(TypedDict):
        required: bool

    class FieldConstraintsDict(MandatoryFieldConstraintsDict, total=False):
        unique: bool

    class FieldStringConstraintsDict(FieldConstraintsDict, total=False):
        max_length_bytes: int
        max_length_chars: int

    class FieldDecimalConstraintsDict(FieldConstraintsDict, total=False):
        fixed_precision: int
        fixed_scale: int
        fp_total_bits: int
        fp_significand_bits: int

    class FieldIntegerConstraintsDict(FieldConstraintsDict, total=False):
        min: str
        max: str

    from ..field_types import FieldType  # noqa


logger = logging.getLogger(__name__)


class RecordsSchemaFieldConstraints:
    def __init__(self, required: bool, unique: Optional[bool] = None):
        """
        :param required: If True, data must always be provided for this
        column in the origin representation; if False, a 'null' or
        equivalent optional value can exist instead.

        :param unique: If knowledge of a constraint is known, will be
        set to True if there is a uniqueness constraint on the field,
        False if there is not an explicit constraint on uniqueness.
        This is a constraint, so we're talking about whether a
        non-unique value *could* potentially be created in the origin
        representation, not whether one exists in practice.  If set to
        None, we have no knowledge of uniqueness constraints.
        """
        self.required = required
        self.unique = unique

    def to_data(self) -> 'FieldConstraintsDict':
        out: 'FieldConstraintsDict' = {
            'required': self.required,
        }
        if self.unique is not None:
            out['unique'] = self.unique

        return out

    @staticmethod
    def from_data(data: Optional['FieldConstraintsDict'],
                  field_type: 'FieldType') -> Optional['RecordsSchemaFieldConstraints']:
        from .string import RecordsSchemaFieldStringConstraints
        from .integer import RecordsSchemaFieldIntegerConstraints
        from .decimal import RecordsSchemaFieldDecimalConstraints
        if data is None:
            return None
        required = data.get('required', False)
        unique = data.get('unique')
        if field_type == 'string':
            string_data = cast('FieldStringConstraintsDict', data)
            return RecordsSchemaFieldStringConstraints(
                required=required,
                unique=unique,
                max_length_bytes=string_data.get('max_length_bytes'),
                max_length_chars=string_data.get('max_length_chars'))
        elif field_type == 'integer':
            int_data = cast('FieldIntegerConstraintsDict', data)
            min_str = int_data.get('min')
            max_str = int_data.get('max')
            min_: Optional[int] = None
            max_: Optional[int] = None
            if min_str:
                min_ = int(min_str)
            if max_str:
                max_ = int(max_str)
            return RecordsSchemaFieldIntegerConstraints(
                required=required,
                unique=unique,
                min_=min_,
                max_=max_)
        elif field_type == 'decimal':
            decimal_data = cast('FieldDecimalConstraintsDict', data)
            return RecordsSchemaFieldDecimalConstraints(
                required=required,
                unique=unique,
                fixed_precision=decimal_data.get("fixed_precision"),
                fixed_scale=decimal_data.get("fixed_scale"),
                fp_total_bits=decimal_data.get("fp_total_bits"),
                fp_significand_bits=decimal_data.get("fp_significand_bits"))
        else:
            return RecordsSchemaFieldConstraints(required=required, unique=unique)

    def cast(self, field_type: 'FieldType') -> 'RecordsSchemaFieldConstraints':
        from .integer import RecordsSchemaFieldIntegerConstraints
        from .decimal import RecordsSchemaFieldDecimalConstraints
        from .string import RecordsSchemaFieldStringConstraints
        required = self.required
        unique = self.unique
        constraints: RecordsSchemaFieldConstraints
        if field_type == 'integer':
            constraints =\
                RecordsSchemaFieldIntegerConstraints(required=required,
                                                     unique=unique,
                                                     min_=None,
                                                     max_=None)
        elif field_type == 'string':
            constraints =\
                RecordsSchemaFieldStringConstraints(required=required,
                                                    unique=unique,
                                                    max_length_bytes=None,
                                                    max_length_chars=None)
        elif field_type == 'decimal':
            constraints =\
                RecordsSchemaFieldDecimalConstraints(required=required,
                                                     unique=unique)
        elif (field_type == 'boolean' or
              field_type == 'date' or
              field_type == 'time' or
              field_type == 'timetz' or
              field_type == 'datetime' or
              field_type == 'datetimetz'):
            constraints =\
                RecordsSchemaFieldConstraints(required=required,
                                              unique=unique)
        else:
            _assert_never(field_type,
                          'Teach me how to downcast constraints '
                          f'for {field_type}')

        return constraints

    @staticmethod
    def from_sqlalchemy_type(required: bool,
                             unique: Optional[bool],
                             type_: 'sqlalchemy.types.TypeEngine',
                             driver: 'DBDriver') -> 'RecordsSchemaFieldConstraints':
        return RecordsSchemaFieldConstraints(required=required, unique=unique)

    @staticmethod
    def from_numpy_dtype(dtype: 'np.dtype',
                         unique: bool) -> 'RecordsSchemaFieldConstraints':
        from .string import RecordsSchemaFieldStringConstraints
        from .integer import RecordsSchemaFieldIntegerConstraints
        from .decimal import RecordsSchemaFieldDecimalConstraints

        SIGNED_INTEGER_KIND = 'i'
        UNSIGNED_INTEGER_KIND = 'u'
        FLOAT_KIND = 'f'
        if dtype.kind == SIGNED_INTEGER_KIND:
            bytes_length = dtype.itemsize
            # http://python-history.blogspot.com/2009/03/problem-with-integer-division.html
            max_ = 2 ** (bytes_length*8)//2-1
            min_ = 0 - 2 ** (bytes_length*8)//2
            return RecordsSchemaFieldIntegerConstraints(required=False, unique=unique,
                                                        min_=min_, max_=max_)
        elif dtype.kind == UNSIGNED_INTEGER_KIND:
            bytes_length = dtype.itemsize
            max_ = 2 ** (bytes_length*8) - 1
            min_ = 0
            return RecordsSchemaFieldIntegerConstraints(required=False, unique=unique,
                                                        min_=min_, max_=max_)
        elif dtype.kind == FLOAT_KIND:
            total_and_significand_precision = {
                2: (16, FLOAT16_SIGNIFICAND_BITS),
                4: (32, FLOAT32_SIGNIFICAND_BITS),
                8: (64, FLOAT64_SIGNIFICAND_BITS),
                16: (80, FLOAT80_SIGNIFICAND_BITS),
            }
            fp_total_bits: Optional[int] = None
            fp_significand_bits: Optional[int] = None
            if dtype.itemsize in total_and_significand_precision:
                fp_total_bits, fp_significand_bits = total_and_significand_precision[dtype.itemsize]
            else:
                logger.warning(f"Could not determine floating point constraints for {dtype}")
            return RecordsSchemaFieldDecimalConstraints(required=False,
                                                        unique=unique,
                                                        fp_total_bits=fp_total_bits,
                                                        fp_significand_bits=fp_significand_bits)
        elif dtype == 'O':
            return RecordsSchemaFieldStringConstraints(required=False,
                                                       unique=unique,
                                                       max_length_bytes=None,
                                                       max_length_chars=None)
        else:
            return RecordsSchemaFieldConstraints(required=False, unique=unique)

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.to_data()})"

    def __repr__(self) -> str:
        return self.__str__()
