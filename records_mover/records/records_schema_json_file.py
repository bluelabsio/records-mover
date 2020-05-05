from ..url.base import BaseDirectoryUrl
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class RecordsSchemaJsonFile:
    def __init__(self, records_loc: BaseDirectoryUrl) -> None:
        self.schema_loc = records_loc.file_in_this_directory('_schema.json')

    def save_schema_json(self, json: str) -> None:
        logger.info(f"Putting into {self.schema_loc.url}")
        self.schema_loc.store_string(json)

    def load_schema_json(self) -> Optional[str]:
        try:
            return self.schema_loc.string_contents()
        except FileNotFoundError:
            logger.debug('No schema JSON found', exc_info=True, stack_info=True)
            return None
