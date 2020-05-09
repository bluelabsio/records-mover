import urllib
import vertica_python
import sqlalchemy
from .import_sql import vertica_import_sql
from .records_import_options import vertica_import_options
from .io_base_wrapper import IOBaseWrapper
from ...url.resolver import UrlResolver
from ...records.load_plan import RecordsLoadPlan
from ...records.delimited import complain_on_unhandled_hints
from ...records.records_format import DelimitedRecordsFormat, BaseRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
from ..loader import LoaderFromFileobj
from typing import IO, Union, List, Type
import logging

logger = logging.getLogger(__name__)


class VerticaLoader(LoaderFromFileobj):
    def __init__(self,
                 url_resolver: UrlResolver,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine]) -> None:
        self.url_resolver = url_resolver
        self.db = db

    def load_from_fileobj(self,
                          schema: str,
                          table: str,
                          load_plan: RecordsLoadPlan,
                          fileobj: IO[bytes]) -> None:
        records_format = load_plan.records_format
        if not isinstance(records_format, DelimitedRecordsFormat):
            raise NotImplementedError("Not currently able to load "
                                      f"{records_format.format_type}")
        processing_instructions = load_plan.processing_instructions
        unhandled_hints = set(records_format.hints.keys())
        vertica_options = vertica_import_options(unhandled_hints, load_plan)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    records_format.hints)

        # vertica_options isn't yet a TypedDict that matches the
        # vertica_import_sql options, so suppress type checking
        import_sql = vertica_import_sql(db_engine=self.db.engine, table=table,
                                        schema=schema, **vertica_options)   # type: ignore
        rawconn = None
        try:
            rawconn = self.db.engine.raw_connection()
            cursor = rawconn.cursor()
            logger.info(import_sql)
            if isinstance(fileobj, urllib.response.addinfourl):
                # Vertica driver is a little too aggressive validating
                # streams and checks type, not behavior, and it
                # rejects some third-party streams.
                #
                # In this case, give it something with the type it's
                # looking for:
                compatible_fileobj = IOBaseWrapper(fileobj)
                cursor.copy(import_sql, compatible_fileobj)
            else:
                cursor.copy(import_sql, fileobj)
            logger.info('Copy complete')
            return None
        finally:
            if rawconn is not None:
                rawconn.close()

    def load_failure_exception(self) -> Type[Exception]:
        return vertica_python.errors.CopyRejected

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False

            unhandled_hints = set(load_plan.records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            vertica_import_options(unhandled_hints, load_plan)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            DelimitedRecordsFormat(variant='bluelabs'),
            DelimitedRecordsFormat(variant='vertica')
        ]
