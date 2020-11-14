from typing import Optional
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.records_format import BaseRecordsFormat
from records_mover.db import DBDriver
from records_mover.utils.lazyprop import lazyprop
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.schema import RecordsSchema
from records_mover.records.prep_and_load import prep_and_load
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
import logging

logger = logging.getLogger(__name__)


class DoMoveFromRecordsDirectory(BaseTableMoveAlgorithm):
    def __init__(self,
                 prep: TablePrep,
                 target_table_details: TargetTableDetails,
                 directory: RecordsDirectory,
                 processing_instructions: ProcessingInstructions,
                 override_records_format: Optional[BaseRecordsFormat]) -> None:
        self.directory = directory
        self.override_records_format = override_records_format
        super().__init__(prep, target_table_details, processing_instructions)

    def load_schema_sql(self,
                        driver: 'DBDriver') -> str:
        schema_obj: Optional[RecordsSchema] = self.directory.load_schema_json_obj()
        if schema_obj is None:
            sql = self.directory.load_schema_sql_from_sql_file()
            if sql is None:
                raise SyntaxError(f'RecordsDirectory ({self}) contains '
                                  'neither schema JSON nor schema SQL')
            return sql
        records_format = self.load_plan.records_format
        return self.schema_sql_for_load(schema_obj, records_format, driver)

    @lazyprop
    def load_plan(self) -> RecordsLoadPlan:
        if self.override_records_format is None:
            records_format = self.directory.load_format(self.processing_instructions.
                                                        fail_if_dont_understand)
        else:
            records_format = self.override_records_format
        return RecordsLoadPlan(records_format=records_format,
                               processing_instructions=self.processing_instructions)

    def load(self, driver: DBDriver) -> Optional[int]:
        plan = self.load_plan
        loader = driver.loader()
        # If we've gotten here, .can_move_from_format() has
        # returned True in the move() method, and that can only happen
        # if we have a valid loader.
        assert loader is not None
        return loader.load(schema=self.tbl.schema_name, table=self.tbl.table_name,
                           load_plan=plan, directory=self.directory)

    def move(self) -> MoveResult:
        logger.info(f"Connecting to database...")

        with self.tbl.db_engine.begin() as db:
            driver = self.tbl.db_driver(db)
            loader = driver.loader()
            # If we've gotten here, .can_move_from_format() has
            # returned True in the move() method, and that can only happen
            # if we have a valid loader.
            assert loader is not None
            load_exception_type = loader.load_failure_exception()
            schema_sql = self.load_schema_sql(driver)
        return prep_and_load(self.tbl, self.prep, schema_sql, self.load,
                             load_exception_type)
