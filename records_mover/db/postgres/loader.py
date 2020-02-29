import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from contextlib import ExitStack
from ...url.resolver import UrlResolver
from ...records.load_plan import RecordsLoadPlan
from ...records.hints import complain_on_unhandled_hints
from ...records.records_directory import RecordsDirectory
from ...records.records_format import DelimitedRecordsFormat, BaseRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
from ...utils.concat_files import ConcatFiles
from postgres_copy import copy_from
from .postgres_copy_options import postgres_copy_options
from typing import IO, Union, List
import logging

logger = logging.getLogger(__name__)


class PostgresLoader:
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
                          fileobj: IO[bytes]) -> int:
        records_format = load_plan.records_format
        if not isinstance(records_format, DelimitedRecordsFormat):
            raise NotImplementedError("Not currently able to load "
                                      f"{records_format.format_type}")
        processing_instructions = load_plan.processing_instructions
        unhandled_hints = set(records_format.hints.keys())
        postgres_options = postgres_copy_options(unhandled_hints, load_plan)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    records_format.hints)

        table_obj = Table(table,
                          self.meta,
                          schema=schema,
                          autoload=True,
                          autoload_with=self.db)
        out = copy_from(fileobj,
                        table_obj,
                        self.db,
                        **postgres_options)
        logger.info('Copy complete')
        return out

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> int:
        all_urls = directory.manifest_entry_urls()

        locs = [self.url_resolver.file_url(url) for url in all_urls]
        fileobjs: List[IO[bytes]] = []
        with ExitStack() as stack:
            fileobjs = [stack.enter_context(loc.open()) for loc in locs]
            # TODO: Is this even right if there are headers?
            concatted_fileobj: IO[bytes] = ConcatFiles(fileobjs)  # type: ignore
            return self.load_from_fileobj(schema,
                                          table,
                                          load_plan,
                                          concatted_fileobj)
        assert False  # TODO WTF

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False

            unhandled_hints = set(load_plan.records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            postgres_copy_options(unhandled_hints, load_plan)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
            return True
        except NotImplementedError as e:
            logger.exception(e)  # TODO
            return False

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            # TODO: Validate these
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints={'compression': None}),
            DelimitedRecordsFormat(variant='csv',
                                   hints={'compression': None}),
            DelimitedRecordsFormat(variant='bigquery',
                                   hints={'compression': None}),
            DelimitedRecordsFormat(variant='vertica',
                                   hints={'compression': None}),
        ]
