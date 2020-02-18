from sqlalchemy_redshift.commands import UnloadFromSelect
from ...records.records_directory import RecordsDirectory
import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy.schema import Table
import logging
from records_mover.db.quoting import quote_schema_and_table
from .records_unload import redshift_unload_options
from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat
)
from typing import Union, Callable, Optional, ContextManager, List
from ...url.base import BaseDirectoryUrl
from botocore.credentials import Credentials
from ..errors import CredsDoNotSupportS3Export
from ...records.hints import complain_on_unhandled_hints
from ..unloader import Unloader


logger = logging.getLogger(__name__)


class RedshiftUnloader(Unloader):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 table: Callable[[str, str], Table],
                 temporary_s3_directory_loc: Callable[[], ContextManager[BaseDirectoryUrl]],
                 **kwargs) -> None:
        super().__init__(db=db)
        self.table = table
        self.temporary_s3_directory_loc = temporary_s3_directory_loc

    def unload_to_s3_directory(self,
                               schema: str,
                               table: str,
                               unload_plan: RecordsUnloadPlan,
                               directory: RecordsDirectory) -> int:
        unhandled_hints = set()
        if isinstance(unload_plan.records_format, DelimitedRecordsFormat):
            unhandled_hints = set(unload_plan.records_format.hints.keys())
        processing_instructions = unload_plan.processing_instructions
        redshift_options = redshift_unload_options(unhandled_hints,
                                                   unload_plan.records_format,
                                                   processing_instructions.fail_if_cant_handle_hint)
        if isinstance(unload_plan.records_format, DelimitedRecordsFormat):
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, unload_plan.records_format.hints)
        # http://sqlalchemy-redshift.readthedocs.io/en/latest/commands.html
        loc = directory.loc
        if not callable(getattr(loc, 'aws_creds')):
            raise NotImplementedError('Redshift can only load from an S3 bucket')
        else:
            aws_creds: Optional[Credentials] = loc.aws_creds()  # type: ignore
            if aws_creds is None:
                raise CredsDoNotSupportS3Export('Please provide AWS credentials '
                                                '(run "aws configure")')
            select = text(f"SELECT * FROM {quote_schema_and_table(self.db, schema, table)}")
            unload = UnloadFromSelect(select=select,
                                      access_key_id=aws_creds.access_key,
                                      secret_access_key=aws_creds.secret_key,
                                      session_token=aws_creds.token, manifest=True,
                                      unload_location=directory.loc.url, **redshift_options)
            self.db.execute(unload)
            out = self.db.execute(text("SELECT pg_last_unload_count()"))
            rows: Optional[int] = out.scalar()
            assert rows is not None
            logger.info(f"Just unloaded {rows} rows")
            out.close()

            return rows

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> int:
        logger.info("Starting Redshift unload...")
        if directory.scheme == 's3':
            s3_directory = directory
            return self.unload_to_s3_directory(schema, table, unload_plan, s3_directory)
        else:
            with self.temporary_s3_directory_loc() as temp_s3_loc:
                s3_directory = RecordsDirectory(records_loc=temp_s3_loc)
                out = self.unload_to_s3_directory(schema, table, unload_plan, s3_directory)
                directory.copy_from(temp_s3_loc)
                return out

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return [DelimitedRecordsFormat(variant='bluelabs'), ParquetRecordsFormat()]

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        try:
            unload_plan = RecordsUnloadPlan(records_format=target_records_format)
            unhandled_hints = set()
            if isinstance(unload_plan.records_format, DelimitedRecordsFormat):
                unhandled_hints = set(unload_plan.records_format.hints.keys())
            processing_instructions = unload_plan.processing_instructions
            redshift_unload_options(unhandled_hints,
                                    unload_plan.records_format,
                                    processing_instructions.fail_if_cant_handle_hint)
            if isinstance(unload_plan.records_format, DelimitedRecordsFormat):
                complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                            unhandled_hints,
                                            unload_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False
