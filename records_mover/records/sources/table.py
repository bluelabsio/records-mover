from typing import Optional
from .base import (SupportsMoveToRecordsDirectory,
                   SupportsToDataframesSource)
from ...db.quoting import quote_schema_and_table
from ...db import DBDriver
from ..records_directory import RecordsDirectory
from ..processing_instructions import ProcessingInstructions
from ..records_format import BaseRecordsFormat
from ..unload_plan import RecordsUnloadPlan
from ..results import MoveResult
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from ..schema import RecordsSchema
from ...url.resolver import UrlResolver
from records_mover.url.base import BaseDirectoryUrl
import logging
from typing import Generator, Iterator, List, TYPE_CHECKING
if TYPE_CHECKING:
    from .dataframes import DataframesRecordsSource  # noqa
    from pandas import DataFrame  # noqa

logger = logging.getLogger(__name__)


class TableRecordsSource(SupportsMoveToRecordsDirectory,
                         SupportsToDataframesSource):
    records_format: Optional[BaseRecordsFormat]

    def __init__(self,
                 schema_name: str,
                 table_name: str,
                 driver: DBDriver,
                 url_resolver: UrlResolver) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.driver = driver
        self.unloader = driver.unloader()
        if self.unloader is not None:
            self.records_format = self.unloader.best_records_format()
        else:
            self.records_format = None
        self.url_resolver = url_resolver

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        unloader = self.driver.unloader()
        if unloader is None:
            return []
        return unloader.known_supported_records_formats_for_unload()

    def can_move_to_format(self,
                           target_records_format: BaseRecordsFormat) -> bool:
        unloader = self.driver.unloader()
        if unloader is None:
            return False
        return unloader.can_unload_format(target_records_format)

    def can_move_to_scheme(self, scheme: str) -> bool:
        unloader = self.driver.unloader()
        if unloader is None:
            # bulk export is not provided by this database
            logger.warning("No unloader configured for this database "
                           f"type ({self.driver.db_engine.name})")
            return False
        can_unload = unloader.can_unload_to_scheme(scheme)
        if not can_unload:
            logger.warning(f"This database ({self.driver.db_engine.name}) is "
                           f"not configured to export to {scheme}")
        return can_unload

    @contextmanager
    def to_dataframes_source(self,
                             processing_instructions: ProcessingInstructions) -> \
            Iterator['DataframesRecordsSource']:
        from .dataframes import DataframesRecordsSource  # noqa
        import pandas

        db = self.driver.db
        records_schema = self.pull_records_schema()

        columns = db.dialect.get_columns(db,
                                         self.table_name,
                                         schema=self.schema_name)

        num_columns = len(columns)
        if num_columns == 0:
            raise LookupError(f"Could not find {self.schema_name}.{self.table_name}")
        entries_per_chunk = 2000000
        chunksize = int(entries_per_chunk / num_columns)
        logger.info(f"Exporting in chunks of up to {chunksize} rows by {num_columns} columns")

        quoted_table = quote_schema_and_table(db, self.schema_name, self.table_name)
        chunks: Generator['DataFrame', None, None] = \
            pandas.read_sql(f"SELECT * FROM {quoted_table}",
                            con=db,
                            chunksize=chunksize)
        try:
            yield DataframesRecordsSource(dfs=self.with_cast_dataframe_types(records_schema,
                                                                             chunks),
                                          records_schema=records_schema,
                                          processing_instructions=processing_instructions)
        finally:
            chunks.close()

    def with_cast_dataframe_types(self,
                                  records_schema: RecordsSchema,
                                  dfs: Iterator['DataFrame']) -> Iterator['DataFrame']:
        for df in dfs:
            yield records_schema.cast_dataframe_types(df)

    def pull_records_schema(self) -> RecordsSchema:
        return RecordsSchema.from_db_table(self.schema_name, self.table_name,
                                           driver=self.driver)

    def move_to_records_directory(self,
                                  records_directory: RecordsDirectory,
                                  records_format: BaseRecordsFormat,
                                  processing_instructions: ProcessingInstructions,
                                  schema_name: Optional[str]=None,
                                  table_name: Optional[str]=None,
                                  engine: Optional[Engine]=None) -> MoveResult:
        unload_plan = RecordsUnloadPlan(records_format=records_format,
                                        processing_instructions=processing_instructions)
        unloader = self.driver.unloader()
        if unloader is None:
            raise ValueError('This DBDriver does not support bulk unloading')
        export_count = unloader.unload(schema=self.schema_name, table=self.table_name,
                                       unload_plan=unload_plan,
                                       directory=records_directory)
        records_schema = self.pull_records_schema()
        records_directory.save_format(unload_plan.records_format)
        records_schema = self.driver.tweak_records_schema_after_unload(records_schema,
                                                                       unload_plan.records_format)
        records_directory.save_schema(records_schema)
        records_directory.finalize_manifest()

        return MoveResult(move_count=export_count, output_urls=None)

    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        """Yield a temporary directory that can be used to call move_to_records_directory() on."""
        unloader = self.driver.unloader()
        if unloader is None:
            raise ValueError('This DBDriver does not support bulk unloading')
        with unloader.temporary_unloadable_directory_loc() as temp_loc:
            yield temp_loc

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.driver.db_engine.name})"
