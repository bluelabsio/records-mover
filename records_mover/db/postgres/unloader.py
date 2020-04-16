from sqlalchemy.schema import Table
from .sqlalchemy_postgres_copy import copy_to
from ..quoting import quote_value
from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import BaseRecordsFormat
from ...records.records_directory import RecordsDirectory
from ...records.records_format import DelimitedRecordsFormat
from ...records.hints import complain_on_unhandled_hints
from typing import List
from ..unloader import Unloader
from .copy_options import postgres_copy_to_options
import logging

logger = logging.getLogger(__name__)


class PostgresUnloader(Unloader):
    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> None:
        if not isinstance(unload_plan.records_format, DelimitedRecordsFormat):
            raise NotImplementedError("This only supports delimited mode for now")

        unhandled_hints = set(unload_plan.records_format.hints.keys())
        processing_instructions = unload_plan.processing_instructions
        date_output_style, date_order_style, postgres_options =\
            postgres_copy_to_options(unhandled_hints,
                                     unload_plan.records_format,
                                     processing_instructions.fail_if_cant_handle_hint)
        if date_order_style is None:
            # U-S-A!  U-S-A!
            date_order_style = 'MDY'
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    unload_plan.records_format.hints)

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
            date_style = f"{date_output_style}, {date_order_style}"
            sql = f"SET LOCAL DateStyle = {quote_value(conn, date_style)}"
            logger.info(sql)
            conn.execute(sql)

            filename = unload_plan.records_format.generate_filename('data')
            loc = directory.loc.file_in_this_directory(filename)
            with loc.open(mode='wb') as fileobj:
                copy_to(table_obj.select(),
                        fileobj,
                        conn,
                        **postgres_options)

        logger.info('Copy complete')
        directory.save_preliminary_manifest()

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return [
            # TODO update these validation instructions and validate each
            #
            # To validate that these unload without pandas:
            #
            #
            # pip3 uninstall -y pandas
            # ./itest shell
            #
            # mvrec table2file --target.variant bluelabs
            # --target.no_compression
            # dockerized-postgres public test
            # tests/integration/resources/delimited-bluelabs-no-header.csv
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints={'compression': None}),
        ]

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        try:
            unload_plan = RecordsUnloadPlan(records_format=target_records_format)
            unhandled_hints = set()
            records_format = unload_plan.records_format
            if not isinstance(records_format, DelimitedRecordsFormat):
                return False
            unhandled_hints = set(records_format.hints.keys())
            processing_instructions = unload_plan.processing_instructions
            postgres_copy_to_options(unhandled_hints,
                                     records_format,
                                     processing_instructions.fail_if_cant_handle_hint)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints,
                                        records_format.hints)
            return True
        except NotImplementedError:
            return False
