from records_mover.utils.lazyprop import lazyprop
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.db import DBDriver
from records_mover.pandas import purge_unnamed_unused_columns
from records_mover.records.prep_and_load import prep_and_load
from records_mover.records.schema import RecordsSchema
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
from records_mover.records.sources.dataframes import DataframesRecordsSource
import logging
import sqlalchemy
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .target import TableRecordsTarget  # Dodge circular dependency

logger = logging.getLogger(__name__)


class DoMoveFromDataframesSource(BaseTableMoveAlgorithm):
    def __init__(self,
                 prep: TablePrep,
                 target_table_details: TargetTableDetails,
                 table_target: 'TableRecordsTarget',
                 dfs_source: DataframesRecordsSource,
                 processing_instructions: ProcessingInstructions) -> None:
        self.dfs_source = dfs_source
        self.table_target = table_target

        super().__init__(prep, target_table_details, processing_instructions)

    def move(self) -> MoveResult:
        target_supports_formats = len(self.table_target.known_supported_records_formats()) != 0
        if (target_supports_formats and self.table_target.can_move_from_fileobjs_source()):
            return self.move_from_dataframes_source_via_fileobjs()
        elif (target_supports_formats and
              self.table_target.can_move_from_temp_loc_after_filling_it()):
            # Some databases, like Redshift, can't load from a
            # stream, but can load from files on an object store
            # they're pointed to.
            return self.move_from_dataframes_source_via_temporary_records_directory()
        else:
            logger.info("Known formats for target database: "
                        f"{self.table_target.known_supported_records_formats()}")
            logger.info("Table target can move from fileobjs source? "
                        f"{self.table_target.can_move_from_fileobjs_source()}")
            logger.warning("Loading via INSERT statement as this DB "
                           "driver does not yet support or is not configured for "
                           "more direct load methods.  "
                           "This may be very slow.")
            return self.move_from_dataframes_source_via_insert()

    def move_from_dataframes_source_via_temporary_records_directory(self) -> MoveResult:
        records_format = next(iter(self.table_target.known_supported_records_formats()), None)
        with self.dfs_source.to_fileobjs_source(self.processing_instructions,
                                                records_format) as fileobjs_source:
            return self.table_target.\
                move_from_temp_loc_after_filling_it(fileobjs_source,
                                                    self.processing_instructions)

    def move_from_dataframes_source_via_fileobjs(self) -> MoveResult:
        records_format = next(iter(self.table_target.known_supported_records_formats()), None)
        with self.dfs_source.to_fileobjs_source(self.processing_instructions,
                                                records_format) as fileobjs_source:
            return self.table_target.move_from_fileobjs_source(fileobjs_source,
                                                               self.processing_instructions)

    @lazyprop
    def records_schema(self) -> RecordsSchema:
        return self.dfs_source.initial_records_schema(self.processing_instructions)

    def load(self, driver: DBDriver) -> int:
        rows_loaded = 0
        with self.tbl.db_engine.begin() as db:
            for df in self.dfs_source.dfs:
                df = purge_unnamed_unused_columns(df)
                df = self.records_schema.\
                    assign_dataframe_names(include_index=self.dfs_source.include_index, df=df)
                df.to_sql(name=self.tbl.table_name,
                          con=db,
                          schema=self.tbl.schema_name,
                          index=self.dfs_source.include_index,
                          if_exists='append')
                rows_loaded += len(df.index)
        return rows_loaded

    def move_from_dataframes_source_via_insert(self) -> MoveResult:
        driver = self.tbl.db_driver(self.tbl.db_engine)
        schema_sql = self.records_schema.to_schema_sql(driver,
                                                       self.tbl.schema_name,
                                                       self.tbl.table_name)
        out = prep_and_load(self.tbl, self.prep, schema_sql, self.load,
                            sqlalchemy.exc.InternalError)
        logger.info(f"Loaded {out.move_count} rows into "
                    f"{self.tbl.schema_name}.{self.tbl.table_name} via INSERT statement")
        return out
