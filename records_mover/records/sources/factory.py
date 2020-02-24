from ..deprecated import warn_deprecated
import pathlib
from contextlib import contextmanager
from sqlalchemy.engine import Engine
from ..records_format import BaseRecordsFormat
from ..schema import RecordsSchema
from ..records_directory import RecordsDirectory
from ...db import DBDriver
from ..processing_instructions import ProcessingInstructions
from ...url.resolver import UrlResolver
from .fileobjs import FileobjsSource  # noqa
from .uninferred_fileobjs import UninferredFileobjsRecordsSource
from .data_url import DataUrlRecordsSource
from .directory import RecordsDirectoryRecordsSource
from .. import RecordsHints, BootstrappingRecordsHints
from .base import (SupportsRecordsDirectory, SupportsMoveToRecordsDirectory,  # noqa
                   SupportsToFileobjsSource, RecordsSource)
from typing import Mapping, IO, Callable, Iterator, Optional, Union, Iterable, TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    from .google_sheets import GoogleSheetsRecordsSource  # noqa ditto
    # with pandas, which an optional addition for clients of this
    # library
    from pandas import DataFrame  # noqa
    from .dataframes import DataframesRecordsSource  # noqa
    from .table import TableRecordsSource  # noqa


class RecordsSources(object):
    """
    These methods produce objects representing the source of a records move.

    Example use:

    .. code-block:: python
       records = session.records
       db_engine = session.get_default_db_engine()
       url = 's3://some-bucket/some-directory/'
       source = records.sources.directory_from_url(url=url)
       target = records.targets.table(schema_name='myschema',
                                      table_name='mytable',
                                      db_engine=db_engine)
       results = records.move(source, target)
    """
    def __init__(self,
                 db_driver: Callable[[Engine], DBDriver],
                 url_resolver: UrlResolver) -> None:
        self.db_driver = db_driver
        self.url_resolver = url_resolver

    @contextmanager
    def dataframe(self,
                  df: 'DataFrame',
                  schema_name: Optional[str]=None,
                  table_name: Optional[str]=None,
                  db_engine: Optional[Engine]=None,
                  processing_instructions: ProcessingInstructions=
                  ProcessingInstructions(),
                  records_schema: Optional[RecordsSchema]=None,
                  include_index: bool=False) -> Iterator['DataframesRecordsSource']:
        """
        :param df: Pandas dataframe to move data from.
        :param schema_name: Obsolete - not used.
        :param table_name: Obsolete - not used.
        :param db_engine: Obsolete - not used.
        :param processing_instructions: Instructions used during creation of the schema SQL.
        :param records_schema: Description of the column names and types of the records.
        :param include_index: If true, the Pandas dataframe index column will be included in
        the move.
        """

        warn_deprecated(schema_name=schema_name, table_name=table_name, db_engine=db_engine)
        from .dataframes import DataframesRecordsSource  # noqa
        yield DataframesRecordsSource(dfs=[df],
                                      records_schema=records_schema,
                                      processing_instructions=processing_instructions,
                                      include_index=include_index)

    @contextmanager
    def dataframes(self,
                   dfs: Iterable['DataFrame'],
                   processing_instructions: ProcessingInstructions=
                   ProcessingInstructions(),
                   records_schema: Optional[RecordsSchema]=None,
                   include_index: bool=False) -> Iterator['DataframesRecordsSource']:
        """

        :param dfs: Iterator of Pandas dataframes to move data from -- all data from
                    these dataframes will be added to the same table.
        :param processing_instructions: Instructions used during creation of the schema SQL.
        :param records_schema: Description of the column names and types of the records.
        :param include_index: If true, the Pandas dataframe index column will be included in
        the move.
        """
        from .dataframes import DataframesRecordsSource  # noqa
        yield DataframesRecordsSource(dfs=dfs,
                                      records_schema=records_schema,
                                      processing_instructions=processing_instructions,
                                      include_index=include_index)

    @contextmanager
    def fileobjs(self,
                 target_names_to_input_fileobjs: Mapping[str, IO[bytes]],
                 records_format: Optional[BaseRecordsFormat]=None,
                 initial_hints: Optional[BootstrappingRecordsHints]=None,
                 records_schema: Optional[RecordsSchema]=None)\
            -> Iterator[Union[UninferredFileobjsRecordsSource, FileobjsSource]]:
        """
        :param target_names_to_input_fileobjs: Filenames mapping to streams of data file.
        :param records_format: Description of the format of the data files.
        :param initial_hints: If records_format is not provided, the format of the file
        will be determined automatically.  If that effort fails, you can help it out by
        providing the 'compression', 'quoting', 'header-row', 'field-delimiter', 'encoding', and
        'escape' hints (or the appropriate subsets) in this dictionary.
        :param records_schema: Description of the column names and types of the records.
        """
        if records_schema is None or records_format is None:
            yield UninferredFileobjsRecordsSource(
                target_names_to_input_fileobjs=target_names_to_input_fileobjs,
                records_format=records_format,
                records_schema=records_schema,
                initial_hints=initial_hints)
        else:
            yield FileobjsSource(
                target_names_to_input_fileobjs=target_names_to_input_fileobjs,
                records_format=records_format,
                records_schema=records_schema)

    @contextmanager
    def data_url(self,
                 input_url: str,
                 records_format: Optional[BaseRecordsFormat]=None,
                 initial_hints: Optional[BootstrappingRecordsHints]=None,
                 records_schema: Optional[RecordsSchema]=None)\
            -> Iterator[DataUrlRecordsSource]:
        """
        :param input_url: Location of the data file.  Must be a URL format understood by the
        records_mover.url library.
        :param records_format: Description of the format of the data files.
        :param initial_hints: If records_format is not provided, the format of the file
        will be determined automatically.  If that effort fails, you can help it out by
        providing the 'compression', 'quoting', 'header-row', 'field-delimiter', 'encoding', and
        'escape' hints (or the appropriate subsets) in this dictionary.
        :param records_schema: Description of the column names and types of the records.
        """
        yield DataUrlRecordsSource(input_url=input_url,
                                   url_resolver=self.url_resolver,
                                   records_format=records_format,
                                   records_schema=records_schema,
                                   initial_hints=initial_hints)

    @contextmanager
    def table(self,
              db_engine: Engine,
              schema_name: str,
              table_name: str) -> Iterator['TableRecordsSource']:
        """
        :param db_engine: Database engine to pull data from.
        :param schema_name: Schema name of a table to get data from.
        :param table_name: Table name of a table to get data from.
        """
        from .table import TableRecordsSource  # noqa
        yield TableRecordsSource(schema_name=schema_name,
                                 table_name=table_name,
                                 url_resolver=self.url_resolver,
                                 driver=self.db_driver(db_engine))

    @contextmanager
    def directory_from_url(self,
                           url: str,
                           hints: RecordsHints={},
                           fail_if_dont_understand: bool=True)\
            -> Iterator[RecordsDirectoryRecordsSource]:
        """
        :param url: Location of the records directory.  Must be a URL format understood by the
        records_mover.url library, and must be a directory URL that ends with a '/'.
        :param hints: Any additional hints that should override the description of the data files
        already in the records directory.
        :param fail_if_dont_understand: If True, and a part of the
        RecordsFormat is not understood while processing, then
        immediately fail and raise an exception.  Otherwise, ignore
        the misunderstood instruction (e.g., ignore the hint, assume
        default variant, etc etc)
        """
        directory_loc = self.url_resolver.directory_url(url)
        directory = RecordsDirectory(records_loc=directory_loc)
        fail = fail_if_dont_understand
        yield RecordsDirectoryRecordsSource(directory=directory,
                                            fail_if_dont_understand=fail,
                                            override_hints=hints,
                                            url_resolver=self.url_resolver)

    @contextmanager
    def local_file(self,
                   filename: str,
                   records_format: Optional[BaseRecordsFormat]=None,
                   initial_hints: Optional[BootstrappingRecordsHints]=None,
                   records_schema: Optional[RecordsSchema]=None)\
            -> Iterator[DataUrlRecordsSource]:
        """
        :param filename: File path (relative or absolute) of the data file to load.
        :param records_format: Description of the format of the data files.
        :param initial_hints: If records_format is not provided, the format of the file
        will be determined automatically.  If that effort fails, you can help it out by
        providing the 'compression', 'quoting', 'header-row', 'field-delimiter', 'encoding', and
        'escape' hints (or the appropriate subsets) in this dictionary.
        :param records_schema: Description of the column names and types of the records.
        """
        url = pathlib.Path(filename).resolve().as_uri()
        with self.data_url(input_url=url,
                           records_format=records_format,
                           records_schema=records_schema,
                           initial_hints=initial_hints) as source:
            yield source

    @contextmanager
    def google_sheet(self,
                     spreadsheet_id: str,
                     sheet_name_or_range: str,
                     google_cloud_creds:
                     'google.auth.credentials.Credentials',
                     out_of_band_column_headers: Optional[Iterable[str]]=None,
                     header_translator: Optional[Callable[[str], str]]=None) ->\
            Iterator['GoogleSheetsRecordsSource']:
        """:param spreadsheet_id: This is the xyz in
        https://docs.google.com/spreadsheets/d/xyz/edit?ts=5be5b383#gid=abc
        :param sheet_name_or_range: This is the label of the particular tab within the Google Sheets
        spreadsheet where the data should go, or a valid Google Sheets-style range formula
        :param google_cloud_creds: This is the dict form of the JSON containing private keys and
                                   client IDs and whatnot for Google Cloud Platform access.
        :param out_of_band_column_headers: If provided, we'll use these
        column names instead of the first row of the spreadsheet.  If
        set, the first row will be treated as data.
        :param header_translator: If provided, header names pulled from
        the sheet will be translated through this function.  Not used
        if out_of_band_column_headers is set.
        """
        from .google_sheets import GoogleSheetsRecordsSource  # noqa
        yield GoogleSheetsRecordsSource(spreadsheet_id=spreadsheet_id,
                                        sheet_name_or_range=sheet_name_or_range,
                                        google_cloud_creds=google_cloud_creds,
                                        out_of_band_column_headers=out_of_band_column_headers,
                                        header_translator=header_translator)
