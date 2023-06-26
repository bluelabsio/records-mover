import sqlalchemy
from pathlib import Path
from records_mover.records import ProcessingInstructions
from records_mover.db.loader import LoaderFromRecordsDirectory
from records_mover.url.filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from .load_options import mysql_load_options
from ...records.delimited import complain_on_unhandled_hints
from ...url.resolver import UrlResolver
from typing import Union, List, Optional
import logging
import tempfile
from ...check_db_conn_engine import check_db_conn_engine

logger = logging.getLogger(__name__)


class MySQLLoader(LoaderFromRecordsDirectory):
    def __init__(self,
                 db: Optional[Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection]],
                 url_resolver: UrlResolver,
                 db_conn: Optional[sqlalchemy.engine.Connection] = None,
                 db_engine: Optional[sqlalchemy.engine.Engine] = None) -> None:
        db, db_conn, db_engine = check_db_conn_engine(db=db, db_conn=db_conn, db_engine=db_engine)
        self.conn_opened_here = False
        self.db = db
        self._db_conn = db_conn
        self.db_engine = db_engine
        self.url_resolver = url_resolver

    def get_db_conn(self) -> sqlalchemy.engine.Connection:
        if self._db_conn is None:
            self._db_conn = self.db_engine.connect()
            self.conn_opened_here = True
            logger.debug(f"Opened connection to database within {self} because none was provided.")
        return self._db_conn

    def set_db_conn(self, db_conn: Optional[sqlalchemy.engine.Connection]) -> None:
        self._db_conn = db_conn

    def del_db_conn(self) -> None:
        if self.conn_opened_here:
            self.db_conn.close()

    db_conn = property(get_db_conn, set_db_conn, del_db_conn)

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> None:
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
        for loc in locs:
            # This came from a FilesystemDirectoryUrl, so it had better be...
            assert isinstance(loc, FilesystemFileUrl)
            filename = loc.local_file_path
            sql = load_options.generate_load_data_sql(filename=filename,
                                                      table_name=table,
                                                      schema_name=schema)
            logger.info(f"Loading to MySQL with options: {load_options}")
            logger.info(str(sql))
            self.db_conn.execute(sql)
            logger.info("MySQL LOAD DATA complete.")
        return None

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
            # MySQL supports a healthy amount of load types, but
            # doesn't support loading compressed files.
            DelimitedRecordsFormat(variant='bluelabs',
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

    def __del__(self) -> None:
        self.del_db_conn()
