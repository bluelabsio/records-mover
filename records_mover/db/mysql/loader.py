import sqlalchemy
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat
from typing import Union, Optional, List


class MySQLLoader:
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],):
        ...

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        raise NotImplementedError

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        raise NotImplementedError

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        raise NotImplementedError
