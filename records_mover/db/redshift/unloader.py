from contextlib import contextmanager
from sqlalchemy_redshift.commands import UnloadFromSelect
from ...records.records_directory import RecordsDirectory
import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy.schema import Table
import logging
from records_mover.db.quoting import quote_schema_and_table
from records_mover.logging import register_secret
from .records_unload import redshift_unload_options
from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat
)
from typing import Union, Callable, Optional, List, Iterator
from ...url.base import BaseDirectoryUrl
from botocore.credentials import Credentials
from ..errors import CredsDoNotSupportS3Export, NoTemporaryBucketConfiguration
from ...records.delimited import complain_on_unhandled_hints
from ..unloader import Unloader


logger = logging.getLogger(__name__)


class RedshiftUnloader(Unloader):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 table: Callable[[str, str], Table],
                 s3_temp_base_loc: Optional[BaseDirectoryUrl],
                 **kwargs) -> None:
        super().__init__(db=db)
        self.table = table
        self.s3_temp_base_loc = s3_temp_base_loc

    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with self.temporary_s3_directory_loc() as temp_loc:
            yield temp_loc

    @contextmanager
    def temporary_s3_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        if self.s3_temp_base_loc is None:
            raise NoTemporaryBucketConfiguration('Please provide a scratch S3 URL in your config '
                                                 '(e.g., set SCRATCH_S3_URL to an s3:// URL)')
        else:
            with self.s3_temp_base_loc.temporary_directory() as temp_loc:
                yield temp_loc

    def unload_to_s3_directory(self,
                               schema: str,
                               table: str,
                               unload_plan: RecordsUnloadPlan,
                               directory: RecordsDirectory) -> Optional[int]:
        logger.info(f"Starting Redshift unload to {directory.loc} as "
                    f"{unload_plan.records_format}...")
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
            #
            # Upon error, an exception is raised with the full SQL -
            # including the AWS creds inside.  Let's register those
            # with the logger so they get redacted.
            #
            register_secret(aws_creds.token)
            register_secret(aws_creds.secret_key)
            select = text(f"SELECT * FROM {quote_schema_and_table(self.db, schema, table)}")
            unload = UnloadFromSelect(select=select,
                                      access_key_id=aws_creds.access_key,
                                      secret_access_key=aws_creds.secret_key,
                                      session_token=aws_creds.token, manifest=True,
                                      unload_location=directory.loc.url, **redshift_options)
            try:
                self.db.execute(unload)
                out = self.db.execute(text("SELECT pg_last_unload_count()"))
                rows: Optional[int] = out.scalar()
                assert rows is not None
                logger.info(f"Just unloaded {rows} rows")
                out.close()
            except sqlalchemy.exc.DatabaseError as e:
                if 'SSL SYSCALL error: Operation timed out' in str(e):
                    # Large database UNLOADs can take hours, and it's
                    # likely the connection between the client and
                    # server will get disconnected.  In this
                    # circumstance, we want to wait until unload is
                    # complete and then proceed.
                    logger.info("Server disconnected - awaiting completion "
                                "of work by checking S3 bucket...")
                    directory.await_completion(log_level=logging.DEBUG,
                                               ms_between_polls=5000,
                                               manifest_filename='manifest')
                    # pg_last_unload_count() doesn't work
                    # after this situation - the query shows up as
                    rows = None
                    logger.info("Unload complete.")
                else:
                    raise

            return rows

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> Optional[int]:
        if directory.scheme == 's3':
            s3_directory = directory
            return self.unload_to_s3_directory(schema, table, unload_plan, s3_directory)
        else:
            with self.temporary_s3_directory_loc() as temp_s3_loc:
                s3_directory = RecordsDirectory(records_loc=temp_s3_loc)
                out = self.unload_to_s3_directory(schema, table, unload_plan, s3_directory)
                directory.copy_from(temp_s3_loc)
                return out

    def can_unload_to_scheme(self, scheme: str) -> bool:
        if scheme == 's3':
            return True
        # Otherwise we'll need a temporary bucket configured for
        # Redshift to unload into
        return self.s3_temp_base_loc is not None

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return [DelimitedRecordsFormat(variant='bluelabs'), ParquetRecordsFormat()]

    def can_unload_format(self, target_records_format: BaseRecordsFormat) -> bool:
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
