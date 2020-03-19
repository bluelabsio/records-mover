from sqlalchemy_redshift.commands import CopyCommand
from ...records.records_directory import RecordsDirectory
from ...records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
import sqlalchemy
from sqlalchemy.schema import Table
import logging
from .records_copy import redshift_copy_options
from ...records.load_plan import RecordsLoadPlan
from ..errors import CredsDoNotSupportS3Import
from typing import Optional, Union, Callable, ContextManager, List
from ...url import BaseDirectoryUrl
from botocore.credentials import Credentials
from ...records.hints import complain_on_unhandled_hints

logger = logging.getLogger(__name__)


class RedshiftLoader:
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 meta: sqlalchemy.MetaData,
                 temporary_s3_directory_loc: Callable[[], ContextManager[BaseDirectoryUrl]])\
            -> None:
        self.db = db
        self.meta = meta
        self.temporary_s3_directory_loc = temporary_s3_directory_loc

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
            raise NotImplementedError('Teach me how to load '
                                      f'{load_plan.records_format.format_type} format')

        if directory.scheme != 's3':
            with self.temporary_s3_directory_loc() as temp_s3_loc:
                s3_directory = directory.copy_to(temp_s3_loc)
                return self.load(schema=schema,
                                 table=table,
                                 load_plan=load_plan,
                                 directory=s3_directory)

        to = Table(table, self.meta, schema=schema)  # no autoload
        unhandled_hints = set(load_plan.records_format.hints.keys())
        processing_instructions = load_plan.processing_instructions
        redshift_options = redshift_copy_options(unhandled_hints, load_plan.records_format.hints,
                                                 processing_instructions.fail_if_cant_handle_hint,
                                                 processing_instructions.fail_if_row_invalid,
                                                 processing_instructions.max_failure_rows)
        logger.info(f"Copying to Redshift with options: {redshift_options}")
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
            copy = CopyCommand(to=to, data_location=directory.loc.url + '_manifest',
                               access_key_id=aws_creds.access_key,
                               secret_access_key=aws_creds.secret_key,
                               session_token=aws_creds.token, manifest=True,
                               region=directory.loc.region,  # type: ignore
                               empty_as_null=True,
                               **redshift_options)
            logger.info(f"Starting Redshift COPY from {directory}...")
            self.db.execute(copy)
            logger.info("Redshift COPY complete.")
            return None  # redshift doesn't give reliable info on load results

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False
            unhandled_hints = set(load_plan.records_format.hints.keys())
            processing_instructions = load_plan.processing_instructions
            redshift_copy_options(unhandled_hints, load_plan.records_format.hints,
                                  processing_instructions.fail_if_cant_handle_hint,
                                  processing_instructions.fail_if_row_invalid,
                                  processing_instructions.max_failure_rows)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
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
        ]
