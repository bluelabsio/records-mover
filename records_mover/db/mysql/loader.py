import sqlalchemy
from records_mover.db.loader import LoaderFromRecordsDirectory
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat
from .load_options import mysql_load_options, MySqlLoadOptions
from ...records.hints import complain_on_unhandled_hints
from typing import Union, Optional, List
import logging

logger = logging.getLogger(__name__)


class MySQLLoader(LoaderFromRecordsDirectory):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection]) -> None:
        self.db = db

    def generate_load_data_sql(self, load_options: MySqlLoadOptions) -> str:
        raise NotImplementedError

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> int:
        if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
            raise NotImplementedError('Teach me how to load '
                                      f'{load_plan.records_format.format_type} format')

        unhandled_hints = set(load_plan.records_format.hints.keys())
        processing_instructions = load_plan.processing_instructions
        load_options = mysql_load_options(unhandled_hints,
                                          load_plan.records_format,
                                          processing_instructions.fail_if_cant_handle_hint)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints, load_plan.records_format.hints)
        sql = self.generate_load_data_sql(load_options)
        logger.info(f"Loading to MySQL with options: {load_options}")
        logger.info(sql)
        # http://sqlalchemy-redshift.readthedocs.io/en/latest/commands.html
        #
        # Upon error, an exception is raised with the full SQL -
        # including the AWS creds inside.  Let's register those
        # with the logger so they get redacted.
        #
        out = self.db.execute(sql)
        logger.info("MySQL LOAD DATA complete.")
        return out.scalar()

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        raise NotImplementedError

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints={
                                       'compression': None
                                   }),
            DelimitedRecordsFormat(variant='csv',
                                   hints={
                                       'compression': None
                                   }),
            DelimitedRecordsFormat(variant='bigquery',
                                   hints={
                                       'compression': None
                                   }),
            DelimitedRecordsFormat(variant='vertica',
                                   hints={
                                       'compression': None
                                   }),
        ]
