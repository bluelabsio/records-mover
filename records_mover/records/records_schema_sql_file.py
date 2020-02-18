from ..url.base import BaseDirectoryUrl
import logging

logger = logging.getLogger(__name__)


class RecordsSchemaSqlFile:
    def __init__(self, records_loc: BaseDirectoryUrl) -> None:
        self.schema_loc = records_loc.file_in_this_directory('_schema')

    def load_schema_sql(self) -> str:
        return self.schema_loc.string_contents()
