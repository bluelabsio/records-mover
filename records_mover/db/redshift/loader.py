from contextlib import contextmanager
from sqlalchemy_redshift.commands import CopyCommand
from records_mover.logging import register_secret
from ..loader import LoaderFromRecordsDirectory
from ...records.records_directory import RecordsDirectory
from ...records.records_format import BaseRecordsFormat, DelimitedRecordsFormat, AvroRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
import sqlalchemy
from sqlalchemy.schema import Table
import logging
from .records_copy import redshift_copy_options
from ...records.load_plan import RecordsLoadPlan
from ..errors import CredsDoNotSupportS3Import, NoTemporaryBucketConfiguration
from typing import Optional, Union, List, Iterator
from ...url import BaseDirectoryUrl
from botocore.credentials import Credentials
from ...records.delimited import complain_on_unhandled_hints

logger = logging.getLogger(__name__)


class RedshiftLoader(LoaderFromRecordsDirectory):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 meta: sqlalchemy.MetaData,
                 s3_temp_base_loc: Optional[BaseDirectoryUrl])\
            -> None:
        self.db = db
        self.meta = meta
        self.s3_temp_base_loc = s3_temp_base_loc

    @contextmanager
    def temporary_s3_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        if self.s3_temp_base_loc is None:
            raise NoTemporaryBucketConfiguration('Please provide a scratch S3 URL in your config '
                                                 '(e.g., set SCRATCH_S3_URL to an s3:// URL)')
        else:
            with self.s3_temp_base_loc.temporary_directory() as temp_loc:
                yield temp_loc

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:

        if directory.scheme != 's3':
            with self.temporary_s3_directory_loc() as temp_s3_loc:
                s3_directory = directory.copy_to(temp_s3_loc)
                return self.load(schema=schema,
                                 table=table,
                                 load_plan=load_plan,
                                 directory=s3_directory)

        to = Table(table, self.meta, schema=schema)  # no autoload
        unhandled_hints = set()
        if isinstance(load_plan.records_format, DelimitedRecordsFormat):
            unhandled_hints = set(load_plan.records_format.hints.keys())
        processing_instructions = load_plan.processing_instructions
        redshift_options = redshift_copy_options(unhandled_hints,
                                                 load_plan.records_format,
                                                 processing_instructions.fail_if_cant_handle_hint,
                                                 processing_instructions.fail_if_row_invalid,
                                                 processing_instructions.max_failure_rows)
        logger.info(f"Copying to Redshift with options: {redshift_options}")
        if isinstance(load_plan.records_format, DelimitedRecordsFormat):
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
        # http://sqlalchemy-redshift.readthedocs.io/en/latest/commands.html
        loc = directory.loc
        if not callable(getattr(loc, 'aws_creds', None)):
            raise NotImplementedError('Redshift can only load from an S3 bucket')
        else:
            aws_creds: Optional[Credentials] = directory.loc.aws_creds()  # type: ignore
            if aws_creds is None:
                raise CredsDoNotSupportS3Import('Please provide AWS credentials '
                                                '(run "aws configure")')
            #
            # Upon error, an exception is raised with the full SQL -
            # including the AWS creds inside.  Let's register those
            # with the logger so they get redacted.
            #
            register_secret(aws_creds.token)
            register_secret(aws_creds.secret_key)

            copy = CopyCommand(to=to, data_location=directory.loc.url + '_manifest',
                               access_key_id=aws_creds.access_key,
                               secret_access_key=aws_creds.secret_key,
                               session_token=aws_creds.token, manifest=True,
                               region=directory.loc.region,  # type: ignore
                               empty_as_null=True,
                               **redshift_options)  # type: ignore
            logger.info(f"Starting Redshift COPY from {directory}...")
            redshift_pid: int = self.db.execute("SELECT pg_backend_pid();").scalar()
            try:
                self.db.execute(copy)
            except sqlalchemy.exc.InternalError:
                # Upon a load erorr, we receive:
                #
                #  sqlalchemy.exc.InternalError:
                #  (psycopg2.errors.InternalError_) Load into table 'tablename'
                #    failed.  Check 'stl_load_errors' system table for details.
                logger.warning("Caught load error - "
                               "for details, run this query: "
                               f"SELECT * FROM stl_load_errors WHERE session={redshift_pid}")
                raise
            logger.info("Redshift COPY complete.")
            return None  # redshift doesn't give reliable info on load results

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            unhandled_hints = set()
            records_format = load_plan.records_format
            if isinstance(records_format, DelimitedRecordsFormat):
                unhandled_hints = set(records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            redshift_copy_options(unhandled_hints,
                                  records_format,
                                  processing_instructions.fail_if_cant_handle_hint,
                                  processing_instructions.fail_if_row_invalid,
                                  processing_instructions.max_failure_rows)
            if isinstance(records_format, DelimitedRecordsFormat):
                complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                            unhandled_hints,
                                            records_format.hints)
            return True
        except NotImplementedError:
            return False

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            # Redshift is pretty flexible with date/time/timezone
            # parsing, so let's use them and go back to saner defaults
            # than those of MS Excel - e.g., the default csv and bigquery
            # formats don't include timezones.
            DelimitedRecordsFormat(variant='csv',
                                   hints={
                                       'dateformat': 'YYYY-MM-DD',
                                       'timeonlyformat': 'HH24:MI:SS',
                                       'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                                       'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                                   }),
            # 'bigquery' and 'csv' support both newlines in strings as
            # well as empty strings (but not timezones)
            DelimitedRecordsFormat(variant='bigquery'),
            DelimitedRecordsFormat(variant='csv'),
            # The default 'bluelabs' format can't represent empty
            # strings when exported via Pandas (see
            # https://github.com/pandas-dev/pandas/issues/15891) - but
            # it can with this flag.  Unfortunately, Redshift doesn't
            # support newlines in strings when using this format.
            DelimitedRecordsFormat(variant='bluelabs', hints={
                'quoting': 'all',
            }),
            # Supports newlines in strings, but not empty strings.
            DelimitedRecordsFormat(variant='bluelabs'),
            AvroRecordsFormat(),
        ]

    def best_scheme_to_load_from(self) -> str:
        return 's3'

    def temporary_loadable_directory_scheme(self) -> str:
        return 's3'

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with self.temporary_s3_directory_loc() as temp_loc:
            yield temp_loc

    def has_temporary_loadable_directory_loc(self) -> bool:
        return self.s3_temp_base_loc is not None
