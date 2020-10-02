import logging
from typing import Optional, Union, cast, TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    from .field_types import FieldType  # noqa

    class FieldStatisticsDict(TypedDict):
        rows_sampled: int
        total_rows: int

    class StringFieldStatisticsDict(FieldStatisticsDict, total=False):
        max_length_bytes: int
        max_length_chars: int


logger = logging.getLogger(__name__)


class RecordsSchemaFieldStatistics:
    def __init__(self,
                 rows_sampled: int,
                 total_rows: int):
        self.rows_sampled = rows_sampled
        self.total_rows = total_rows

    def to_data(self) -> 'FieldStatisticsDict':
        return {
            'rows_sampled': self.rows_sampled,
            'total_rows': self.total_rows
        }

    @staticmethod
    def from_data(data: Optional[Union['FieldStatisticsDict', 'StringFieldStatisticsDict']],
                  field_type: 'FieldType') -> Optional['RecordsSchemaFieldStatistics']:
        if data is None:
            return None
        rows_sampled = data['rows_sampled']
        total_rows = data['total_rows']
        if field_type == 'string':
            data = cast('StringFieldStatisticsDict', data)
            return RecordsSchemaFieldStringStatistics(
                rows_sampled=rows_sampled,
                total_rows=total_rows,
                max_length_bytes=data.get('max_length_bytes'),
                max_length_chars=data.get('max_length_chars'))
        else:
            return RecordsSchemaFieldStatistics(rows_sampled=rows_sampled,
                                                total_rows=total_rows)

    def cast(self, field_type: 'FieldType') -> Optional['RecordsSchemaFieldStatistics']:
        # only string provides statistics at this point
        return None

    def __str__(self) -> str:
        return f"{type(self)}({self.to_data()})"


class RecordsSchemaFieldStringStatistics(RecordsSchemaFieldStatistics):
    def __init__(self,
                 rows_sampled: int,
                 total_rows: int,
                 max_length_bytes: Optional[int],
                 max_length_chars: Optional[int]):
        super().__init__(rows_sampled=rows_sampled,
                         total_rows=total_rows)
        self.max_length_bytes = max_length_bytes
        self.max_length_chars = max_length_chars

    def to_data(self) -> 'StringFieldStatisticsDict':
        generic_out = super().to_data()
        out = cast('StringFieldStatisticsDict', generic_out)
        if self.max_length_bytes is not None:
            out['max_length_bytes'] = int(self.max_length_bytes)
        if self.max_length_chars is not None:
            out['max_length_chars'] = int(self.max_length_chars)
        return out

    def merge(self, other: 'RecordsSchemaFieldStringStatistics') -> None:
        raise NotImplementedError

    def cast(self, field_type: 'FieldType') -> Optional['RecordsSchemaFieldStatistics']:
        if field_type == 'string':
            return self
        else:
            return super().cast(field_type)
