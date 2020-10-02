from typing_extensions import Literal
from typing_inspect import get_args
from typing import List

FieldType = Literal['integer',
                    'decimal',
                    'string',
                    'boolean',
                    'date',
                    'time',
                    'timetz',
                    'datetime',
                    'datetimetz']

# Be sure to add new things below in FieldType, too
RECORDS_FIELD_TYPES: List[str] = list(get_args(FieldType))  # type: ignore
