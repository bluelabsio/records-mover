from records_mover.records.schema import RecordsSchema
from records_mover.records.records_format import BaseRecordsFormat
from records_mover.db import DBDriver
from records_mover.records.prep import TablePrep
from records_mover.records.table import TargetTableDetails
from records_mover.records.processing_instructions import ProcessingInstructions
import logging

logger = logging.getLogger(__name__)


class BaseTableMoveAlgorithm:
    def __init__(self,
                 prep: TablePrep,
                 target_table_details: TargetTableDetails,
                 processing_instructions: ProcessingInstructions) -> None:
        self.prep = prep
        self.tbl = target_table_details
        self.processing_instructions = processing_instructions

    def schema_sql_for_load(self,
                            records_schema: RecordsSchema,
                            records_format: BaseRecordsFormat,
                            driver: 'DBDriver') -> str:
        tweaked_records_schema = driver.tweak_records_schema_for_load(records_schema,
                                                                      records_format)
        return tweaked_records_schema.to_schema_sql(driver,
                                                    self.tbl.schema_name,
                                                    self.tbl.table_name)
