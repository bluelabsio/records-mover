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
import logging
from typing import Iterator, List, TYPE_CHECKING
if TYPE_CHECKING:
    from .dataframes import DataframesRecordsSource  # noqa
    from pandas import DataFrame  # noqa

logger = logging.getLogger(__name__)


class TableRecordsSource(SupportsMoveToRecordsDirectory,
                         SupportsToDataframesSource):
    def __init__(self,
                 schema_name: str,
                 table_name: str,
                 driver: DBDriver,
                 url_resolver: UrlResolver) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.driver = driver
        self.records_format = driver.best_records_format()
        self.url_resolver = url_resolver

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        return self.driver.known_supported_records_formats_for_unload()

    def can_move_to_this_format(self,
                                target_records_format: BaseRecordsFormat) -> bool:
        return self.driver.can_unload_this_format(target_records_format)

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
        chunks: Iterator['DataFrame'] = \
            pandas.read_sql(f"SELECT * FROM {quoted_table}",
                            con=db,
                            chunksize=chunksize)
        yield DataframesRecordsSource(dfs=self.with_cast_dataframe_types(records_schema, chunks),
                                      records_schema=records_schema,
                                      processing_instructions=processing_instructions)

    def with_cast_dataframe_types(self,
                                  records_schema,
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
        export_count = self.driver.unload(schema=self.schema_name, table=self.table_name,
                                          unload_plan=unload_plan,
                                          directory=records_directory)
        records_schema = self.pull_records_schema()
        records_directory.save_format(unload_plan.records_format)
        records_directory.save_schema(records_schema)
        records_directory.finalize_manifest()

        return MoveResult(move_count=export_count, output_urls=None)

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.driver.db_engine.name})"
