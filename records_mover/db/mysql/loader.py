import sqlalchemy
from pathlib import Path
from records_mover.records import ProcessingInstructions
from records_mover.db.loader import LoaderFromRecordsDirectory
from records_mover.url.filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from .load_options import mysql_load_options
from ...records.hints import complain_on_unhandled_hints
from ...url.resolver import UrlResolver
from typing import Union, List
import logging
import tempfile

logger = logging.getLogger(__name__)


class MySQLLoader(LoaderFromRecordsDirectory):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 url_resolver: UrlResolver) -> None:
        self.db = db
        self.url_resolver = url_resolver

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
        if not isinstance(directory.loc, FilesystemDirectoryUrl):
            with tempfile.TemporaryDirectory(prefix='mysql_loader_load') as tempdir:
                filesystem_url = Path(tempdir).as_uri()
                temp_filesystem_loc = FilesystemDirectoryUrl(filesystem_url)
                filesystem_directory = directory.copy_to(temp_filesystem_loc)
                return self.load(schema=schema,
                                 table=table,
                                 load_plan=load_plan,
                                 directory=filesystem_directory)

        all_urls = directory.manifest_entry_urls()

        locs = [self.url_resolver.file_url(url) for url in all_urls]
        rows_so_far = 0
        with self.db.connect() as conn:
            dbapi_conn = conn.connection
            for loc in locs:
                # This came from a FilesystemDirectoryUrl, so it had better be...
                # TODO: Can I make records directory a type class?
                assert isinstance(loc, FilesystemFileUrl)
                # TODO: Verify this is appending
                filename = loc.local_file_path
                with dbapi_conn.cursor() as cursor:
                    sql = load_options.generate_load_data_sql(filename=filename,
                                                              table_name=table,
                                                              schema_name=schema)
                    sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

                    logger.info(f"Loading to MySQL with options: {load_options}")
                    logger.info(sqltext)
                    out = cursor.execute(sqltext)
                logger.info("MySQL LOAD DATA complete.")
                rows_so_far += out
            return rows_so_far

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False

            unhandled_hints = set(load_plan.records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            mysql_load_options(unhandled_hints, load_plan.records_format,
                               fail_if_cant_handle_hint=True)
            complain_on_unhandled_hints(fail_if_dont_understand=True,
                                        unhandled_hints=unhandled_hints,
                                        hints=load_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False

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
