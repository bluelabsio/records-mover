import numpy
from typing import Optional, Tuple, TYPE_CHECKING
from .constraints import RecordsSchemaFieldConstraints
if TYPE_CHECKING:
    from .field_types import FieldType  # noqa


def details_from_numpy_dtype(dtype: numpy.dtype,
                             unique: bool) -> Tuple['FieldType',
                                                    RecordsSchemaFieldConstraints]:
    from ..field import RecordsSchemaField
    basename = dtype.base.name
    field_type: Optional['FieldType']
    if basename.startswith('datetime64'):
        has_tz = getattr(dtype, "tz", None) is not None
        if basename == 'datetime64[D]':
            field_type = 'date'
        else:
            if has_tz:
                # See: 'Represent pandas datetime64 with timezone in records schema'
                #
                # https://github.com/bluelabsio/records-mover/issues/89
                field_type = 'datetimetz'
            else:
                field_type = 'datetime'
    else:
        field_type = RecordsSchemaField.python_type_to_field_type(dtype.type)
        if field_type is None:
            raise NotImplementedError(f"Teach me how to handle Pandas/numpy dtype {dtype} "
                                      f"which is dtype.type {dtype.type}")

    constraints = RecordsSchemaFieldConstraints.from_numpy_dtype(dtype, unique=unique)

    return (field_type, constraints)
