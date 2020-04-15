from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import BaseRecordsFormat
from ...records.records_directory import RecordsDirectory
from typing import List
from ..unloader import Unloader


class PostgresUnloader(Unloader):
    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> int:
        raise NotImplementedError

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        raise NotImplementedError

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        raise NotImplementedError
