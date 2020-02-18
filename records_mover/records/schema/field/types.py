from typing import TYPE_CHECKING
# Be sure to add new things below in FieldType, too
RECORDS_FIELD_TYPES = {
    'integer',
    'decimal',
    'string',
    'boolean',
    'date',
    'time',
    'timetz',
    'datetime',
    'datetimetz'
}

if TYPE_CHECKING:
    from typing_extensions import Literal  # noqa
    FieldType = Literal['integer',
                        'decimal',
                        'string',
                        'boolean',
                        'date',
                        'time',
                        'timetz',
                        'datetime',
                        'datetimetz']
