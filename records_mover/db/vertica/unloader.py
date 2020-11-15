from records_mover.db.quoting import quote_value
import sqlalchemy
from contextlib import contextmanager
from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ...url.base import BaseDirectoryUrl
from ..errors import (NoTemporaryBucketConfiguration, LoadUnloadError,
                      CredsDoNotSupportS3Export, DatabaseDoesNotSupportS3Export)
from ...records.records_directory import RecordsDirectory
from .export_sql import vertica_export_sql
from .records_export_options import vertica_export_options
from ...records.delimited import complain_on_unhandled_hints
from ..unloader import Unloader
import logging
from typing import Iterator, Optional, Union, List, TYPE_CHECKING
if TYPE_CHECKING:
    from botocore.credentials import Credentials  # noqa


logger = logging.getLogger(__name__)


class VerticaUnloader(Unloader):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 s3_temp_base_loc: Optional[BaseDirectoryUrl]) -> None:
        super().__init__(db=db)
        self.s3_temp_base_loc = s3_temp_base_loc

    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        if self.s3_temp_base_loc is None:
            raise NoTemporaryBucketConfiguration('Please provide a scratch S3 URL in your config '
                                                 '(e.g., set SCRATCH_S3_URL to an s3:// URL)')
        else:
            with self.s3_temp_base_loc.temporary_directory() as temp_loc:
                yield temp_loc

    def aws_creds_sql(self, aws_id: str, aws_secret: str) -> str:
        return """
            ALTER SESSION SET UDPARAMETER FOR awslib aws_id={aws_id};
            ALTER SESSION SET UDPARAMETER FOR awslib aws_secret={aws_secret};
        """.format(aws_id=quote_value(self.db.engine, aws_id),
                   aws_secret=quote_value(self.db.engine, aws_secret))

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> Optional[int]:
        if not self.s3_export_available():
            raise NotImplementedError('S3 currently required for Vertica bulk unload')
        try:
            if directory.scheme == 's3':
                return self.unload_to_s3_directory(schema, table, unload_plan, directory)
            else:
                with self.temporary_unloadable_directory_loc() as temp_s3_loc:
                    s3_directory = RecordsDirectory(records_loc=temp_s3_loc)
                    out = self.unload_to_s3_directory(schema, table, unload_plan, s3_directory)
                    directory.copy_from(temp_s3_loc)
                    return out
        except LoadUnloadError as e:
            msg = str(e)
            if msg:
                logger.warning(msg)
            raise NotImplementedError('S3 currently required for Vertica bulk unload')

    def s3_temp_bucket_available(self) -> bool:
        return self.s3_temp_base_loc is not None

    def s3_export_available(self) -> bool:
        out = self.db.execute("SELECT lib_name from user_libraries where lib_name = 'awslib'")
        available = len(list(out.fetchall())) == 1
        if not available:
            logger.info("Not attempting S3 export - no access to awslib in Vertica")
        return available

    def unload_to_s3_directory(self,
                               schema: str,
                               table: str,
                               unload_plan: RecordsUnloadPlan,
                               directory: RecordsDirectory) -> int:
        if not isinstance(unload_plan.records_format, DelimitedRecordsFormat):
            raise NotImplementedError("This only supports delimited mode for now")

        loc = directory.loc
        if not callable(getattr(loc, 'aws_creds')):
            raise NotImplementedError('No AWS creds loaded into location')

        aws_creds: Optional['Credentials'] = directory.loc.aws_creds()  # type: ignore
        if aws_creds is None:
            raise CredsDoNotSupportS3Export('Please provide AWS credentials (run "aws configure")')
        if aws_creds.token is not None:
            raise CredsDoNotSupportS3Export("Vertica does not support AWS security tokens--"
                                            "please create a dedicated credential")
        unhandled_hints = set(unload_plan.records_format.hints.keys())
        processing_instructions = unload_plan.processing_instructions
        try:
            s3_sql = self.aws_creds_sql(aws_creds.access_key, aws_creds.secret_key)
            self.db.execute(s3_sql)
        except sqlalchemy.exc.ProgrammingError as e:
            raise DatabaseDoesNotSupportS3Export(str(e))

        vertica_options = vertica_export_options(unhandled_hints, unload_plan)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    unload_plan.records_format.hints)

        export_sql = vertica_export_sql(db_engine=self.db.engine,
                                        table=table,
                                        schema=schema,
                                        s3_url=directory.loc.url,
                                        **vertica_options)
        logger.info(export_sql)
        export_result = self.db.execute(export_sql).fetchall()
        directory.save_preliminary_manifest()
        export_count = 0
        for record in export_result:
            export_count += int(record.rows)
        return export_count

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return [DelimitedRecordsFormat(variant='vertica')]

    def can_unload_to_scheme(self, scheme: str) -> bool:
        if not self.s3_export_available():
            return False
        return scheme == 's3' or self.s3_temp_bucket_available()

    def can_unload_format(self, target_records_format: BaseRecordsFormat) -> bool:
        try:
            unload_plan = RecordsUnloadPlan(records_format=target_records_format)
            if not isinstance(unload_plan.records_format, DelimitedRecordsFormat):
                return False

            unhandled_hints = set(unload_plan.records_format.hints.keys())
            processing_instructions = unload_plan.processing_instructions
            vertica_export_options(unhandled_hints, unload_plan)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints,
                                        unload_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False
