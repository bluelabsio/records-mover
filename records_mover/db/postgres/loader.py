import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from ..quoting import quote_value
from ...url.resolver import UrlResolver
from ...records.load_plan import RecordsLoadPlan
from ...records.delimited import complain_on_unhandled_hints
from ...records.records_format import DelimitedRecordsFormat, BaseRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
from .sqlalchemy_postgres_copy import copy_from
from .copy_options import postgres_copy_from_options
from typing import IO, Union, List, Iterable
from ..loader import LoaderFromFileobj
import logging

logger = logging.getLogger(__name__)


class PostgresLoader(LoaderFromFileobj):
    def __init__(self,
                 url_resolver: UrlResolver,
                 meta: MetaData,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine]) -> None:
        self.url_resolver = url_resolver
        self.db = db
        self.meta = meta

    def load_from_fileobj(self,
                          schema: str,
                          table: str,
                          load_plan: RecordsLoadPlan,
                          fileobj: IO[bytes]) -> None:
        return self.load_from_fileobjs(schema=schema,
                                       table=table,
                                       load_plan=load_plan,
                                       fileobjs=[fileobj])

    def load_from_fileobjs(self,
                           schema: str,
                           table: str,
                           load_plan: RecordsLoadPlan,
                           fileobjs: Iterable[IO[bytes]]) -> None:
        records_format = load_plan.records_format
        if not isinstance(records_format, DelimitedRecordsFormat):
            raise NotImplementedError("Not currently able to load "
                                      f"{records_format.format_type}")
        processing_instructions = load_plan.processing_instructions
        unhandled_hints = set(records_format.hints.keys())
        date_order_style, postgres_options = postgres_copy_from_options(unhandled_hints, load_plan)
        if date_order_style is None:
            # U-S-A!  U-S-A!
            date_order_style = 'MDY'
        logger.info(f"PostgreSQL load options: {postgres_options}")
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    records_format.hints)

        table_obj = Table(table,
                          self.meta,
                          schema=schema,
                          autoload=True,
                          autoload_with=self.db)

        with self.db.engine.begin() as conn:
            # https://www.postgresql.org/docs/8.3/sql-set.html
            #
            # The effects of SET LOCAL last only till the end of the
            # current transaction, whether committed or not. A special
            # case is SET followed by SET LOCAL within a single
            # transaction: the SET LOCAL value will be seen until the end
            # of the transaction, but afterwards (if the transaction is
            # committed) the SET value will take effect.
            date_style = f"ISO, {date_order_style}"
            sql = f"SET LOCAL DateStyle = {quote_value(conn, date_style)}"
            logger.info(sql)
            conn.execute(sql)

            for fileobj in fileobjs:
                # Postgres COPY FROM defaults to appending data--we
                # let the records Prep class decide what to do about
                # the existing table, so it's safe to call this
                # multiple times and append until done:
                copy_from(fileobj,
                          table_obj,
                          conn,
                          **postgres_options)
        logger.info('Copy complete')

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False

            unhandled_hints = set(load_plan.records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            postgres_copy_from_options(unhandled_hints, load_plan)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            # To validate that these load without pandas, watch
            # logging while running and verify 'dataframe' doesn't
            # appear:
            #
            # ./itest shell
            #
            # mvrec file2table --source.variant bluelabs
            # --source.no_compression
            # tests/integration/resources/delimited-bluelabs-no-header.csv
            # dockerized-postgres public bluelabsformat
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints={'compression': None}),
            # mvrec file2table --source.variant csv
            # --source.no_compression
            # tests/integration/resources/delimited-csv-with-header.csv
            # dockerized-postgres public csvformat
            DelimitedRecordsFormat(variant='csv',
                                   hints={'compression': None}),
            # mvrec file2table --source.variant bigquery
            # --source.no_compression
            # tests/integration/resources/delimited-bigquery-with-header.csv
            # dockerized-postgres public bigqueryformat
            DelimitedRecordsFormat(variant='bigquery',
                                   hints={'compression': None}),
        ]
