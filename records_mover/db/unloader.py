from sqlalchemy import MetaData
from ..records.unload_plan import RecordsUnloadPlan
from ..records.records_format import BaseRecordsFormat
from ..records.records_directory import RecordsDirectory
from typing import Union, List
import sqlalchemy


class Unloader:
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine]) -> None:
        self.db = db
        self.meta = MetaData()

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> int:
        raise NotImplementedError(f"Teach me how to bulk export from {self.db.engine.name}")

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return []

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        return False
