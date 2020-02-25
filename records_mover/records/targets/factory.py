import pathlib
from ...url.resolver import UrlResolver
from sqlalchemy.engine import Engine, Connection
from ...db import DBDriver
from contextlib import contextmanager
from ..records_format import BaseRecordsFormat
from .table import TableRecordsTarget
from .fileobj import FileobjTarget
from .directory_from_url import DirectoryFromUrlRecordsTarget
from .spectrum import SpectrumRecordsTarget
from .data_url import DataUrlTarget
from typing import Callable, Iterator, Optional, Union, Dict, List, IO, TYPE_CHECKING
from ..existing_table_handling import ExistingTableHandling
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    from .google_sheets import GoogleSheetsRecordsTarget  # noqa
    from .table import TableRecordsTarget  # noqa
    from .directory_from_url import DirectoryFromUrlRecordsTarget  # noqa


class RecordsTargets(object):
    """
    These methods produce objects representing the target of a records move.

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
                 url_resolver: UrlResolver,
                 db_driver: Callable[[Union[Engine, Connection]], DBDriver]) -> None:
        self.url_resolver = url_resolver
        self.db_driver = db_driver

    @contextmanager
    def directory_from_url(self,
                           output_url: str,
                           records_format:
                           Optional[BaseRecordsFormat]=None) ->\
            Iterator['DirectoryFromUrlRecordsTarget']:
        """
        :param output_url: Location to write the records directory.  Must be a URL format
        understood by the records_mover.url library, and must be a directory URL that ends with a
        '/'.
        :param records_format: Description of the format of the data files to write out.
        """
        from .directory_from_url import DirectoryFromUrlRecordsTarget  # noqa
        yield DirectoryFromUrlRecordsTarget(output_url=output_url,
                                            records_format=records_format,
                                            url_resolver=self.url_resolver)

    @contextmanager
    def table(self,
              db_engine: Engine,
              schema_name: str,
              table_name: str,
              existing_table_handling: ExistingTableHandling=
              ExistingTableHandling.DELETE_AND_OVERWRITE,
              drop_and_recreate_on_load_error: bool=False,
              add_user_perms_for: Optional[Dict[str, List[str]]]=None,
              add_group_perms_for: Optional[Dict[str, List[str]]]=None) -> \
            Iterator['TableRecordsTarget']:
        """
        :param db_engine: Database engine to write data to.

        :param schema_name: Schema name of a table to write data to.

        :param table_name: Table name of a table to write data to.

        :param existing_table_handling: When loading into a database
        table, controls how any existing table found will be handled.

        :param drop_and_recreate_on_load_error: If True, table load errors
        will attempt to be addressed by dropping the target table and
        reloading the incoming data.

        :param add_user_perms_for: If specified, a table's permissions
        will be set for the specified users. Format should be like
        {'all': ['username1', 'username2'], 'select': ['username3', 'username4']}

        :param add_group_perms_for: If specified, a table's permissions
        will be set for the specified group. Format should be like
        {'all': ['group1', 'group2'], 'select': ['group3', 'group4']}
        """
        from .table import TableRecordsTarget  # noqa
        yield TableRecordsTarget(schema_name=schema_name,
                                 table_name=table_name,
                                 db_engine=db_engine,
                                 db_driver=self.db_driver,
                                 existing_table_handling=existing_table_handling,
                                 drop_and_recreate_on_load_error=drop_and_recreate_on_load_error,
                                 add_user_perms_for=add_user_perms_for,
                                 add_group_perms_for=add_group_perms_for)

    @contextmanager
    def google_sheet(self,
                     spreadsheet_id: str,
                     sheet_name: str,
                     google_cloud_creds:
                     'google.auth.credentials.Credentials') ->\
            Iterator['GoogleSheetsRecordsTarget']:
        """
        :param spreadsheet_id: This is the xyz in
        https://docs.google.com/spreadsheets/d/xyz/edit?ts=5be5b383#gid=abc
        :param sheet_name: This is the label of the particular tab within the Google Sheets
         spreadsheet where the data should go.
        :param google_cloud_creds: Instance of google.auth.credentials.Credentials for Google Cloud
                                   Platform access.
        """
        # see the 'gsheets' extras_require option in setup.py - needed for this!
        from .google_sheets import GoogleSheetsRecordsTarget  # noqa
        yield GoogleSheetsRecordsTarget(spreadsheet_id=spreadsheet_id,
                                        sheet_name=sheet_name,
                                        google_cloud_creds=google_cloud_creds)

    @contextmanager
    def fileobj(self,
                output_fileobj: IO[bytes],
                records_format: BaseRecordsFormat) -> Iterator[FileobjTarget]:
        """
        :param output_fileobj: Stream where the file shoud be written to
        :param records_format: Description of the format of the data files.
        """
        yield FileobjTarget(fileobj=output_fileobj, records_format=records_format)

    @contextmanager
    def data_url(self,
                 output_url: str,
                 records_format: Optional[BaseRecordsFormat]=None) -> Iterator[DataUrlTarget]:
        """
        :param output_url: Location of the data file to write.  Must be a URL format understood by
        the records_mover.url library.
        :param records_format: Description of the required format of the data file to write, or
               None for no preference (may be faster depending on the source).
        """
        output_loc = self.url_resolver.file_url(output_url)
        yield DataUrlTarget(output_loc=output_loc,
                            records_format=records_format)

    @contextmanager
    def local_file(self,
                   filename: str,
                   records_format: Optional[BaseRecordsFormat]=None) ->\
            Iterator['DataUrlTarget']:
        """
        :param filename: File path (relative or absolute) of the data file to unload to.
        :param records_format: Description of the required format of the data file to write, or
               None for no preference (may be faster depending on the source).
        """
        url = pathlib.Path(filename).resolve().as_uri()
        with self.data_url(output_url=url, records_format=records_format) as source:
            yield source

    @contextmanager
    def spectrum(self,
                 schema_name: str,
                 table_name: str,
                 db_engine: Engine,
                 spectrum_base_url: Optional[str]=None,
                 spectrum_rdir_url: Optional[str]=None,
                 existing_table_handling: ExistingTableHandling=
                 ExistingTableHandling.TRUNCATE_AND_OVERWRITE) ->\
            Iterator['SpectrumRecordsTarget']:
        """
        :param schema_name: Schema name of a table to write data to.

        :param table_name: Table name of a table to write data to.

        :param db_engine: Database engine to write data to.

        :param spectrum_base_url: Root S3 URL under which a simple
        directory structure will be created for files to be stored, if
        spectrum_rdir_url is not specified.  Note that when using the
        mover CLI, db-facts may be used to provide a default.

        :param spectrum_rdir_url: S3 URL where a records directory
        with files will be stored; otherwise, use db-facts default if
        exists.  If this is not specified, spectrum_base_url must be.

        :param existing_table_handling: When loading into a database
        table, controls how any existing table found will be handled.

        """
        yield SpectrumRecordsTarget(schema_name=schema_name,
                                    table_name=table_name,
                                    db_engine=db_engine,
                                    db_driver=self.db_driver,
                                    url_resolver=self.url_resolver,
                                    spectrum_base_url=spectrum_base_url,
                                    spectrum_rdir_url=spectrum_rdir_url,
                                    existing_table_handling=existing_table_handling)
