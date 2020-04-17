from sqlalchemy_redshift.commands import CopyCommand
from ...records.records_directory import RecordsDirectory
from ...records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ...records.processing_instructions import ProcessingInstructions
import sqlalchemy
from sqlalchemy.schema import Table
import psycopg2
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
# analytics=> \d+ stl_load_errors
#                                            Table "pg_catalog.stl_load_errors"
#      Column      |            Type             | Collation | Nullable | Default | Storage  | Stats target | Description
# -----------------+-----------------------------+-----------+----------+---------+----------+--------------+-------------
#  userid          | integer                     |           | not null |         | plain    |              |
#  slice           | integer                     |           | not null |         | plain    |              |
#  tbl             | integer                     |           | not null |         | plain    |              |
#  starttime       | timestamp without time zone |           | not null |         | plain    |              |
#  session         | integer                     |           | not null |         | plain    |              |
#  query           | integer                     |           | not null |         | plain    |              |
#  filename        | character(256)              |           | not null |         | extended |              |
#  line_number     | bigint                      |           | not null |         | plain    |              |
#  colname         | character(127)              |           | not null |         | extended |              |
#  type            | character(10)               |           | not null |         | extended |              |
#  col_length      | character(10)               |           | not null |         | extended |              |
#  position        | integer                     |           | not null |         | plain    |              |
#  raw_line        | character(1024)             |           | not null |         | extended |              |
#  raw_field_value | character(1024)             |           | not null |         | extended |              |
#  err_code        | integer                     |           | not null |         | plain    |              |
#  err_reason      | character(100)              |           | not null |         | extended |              |
# Replica Identity: ???

# analytics=>

# analytics=> select * from stl_load_errors where session=11400;
# -[ RECORD 1 ]---+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# userid          | 149
# slice           | 4
# tbl             | 5544604
# starttime       | 2020-04-16 18:58:17.742116
# session         | 11400
# query           | 9694339
# filename        | s3://vince-scratch/V0_cRRU9Tb0/data013.csv.gz
# line_number     | 2
# colname         | election_date
# type            | int8
# col_length      | 0
# position        | 45
# raw_line        | WI,WI-8848269,20200317.0,,,Absentee,20200416,20200407.0,,,
# raw_field_value | 20200407.0
# err_code        | 1207
# err_reason      | Invalid digit, Value '.', Pos 8, Type: Long

# analytics=> \q


            # TODO: sqlalchemy.exc.InternalError: (psycopg2.errors.InternalError_) Load into table 'ts_early_and_absentee_0416' failed.  Check 'stl_load_errors' system table for details.
            redshift_pid = self.db.execute("SELECT pg_backend_pid();").fetchone()[0]
            try:
                self.db.execute(copy) # TODO: handle and report back
            except sqlalchemy.exc.InternalError:
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
