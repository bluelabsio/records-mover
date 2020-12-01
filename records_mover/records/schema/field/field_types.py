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

RECORDS_FIELD_TYPES: List[str] = list(get_args(FieldType))
