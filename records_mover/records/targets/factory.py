import pathlib
from ...url.resolver import UrlResolver
from ..records_format import BaseRecordsFormat
from .fileobj import FileobjTarget
from .directory_from_url import DirectoryFromUrlRecordsTarget
from .data_url import DataUrlTarget
from typing import Callable, Optional, Union, Dict, List, IO, TYPE_CHECKING
from ..existing_table_handling import ExistingTableHandling
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    from sqlalchemy.engine import Engine, Connection  # noqa
    from ...db import DBDriver  # noqa
    from .spectrum import SpectrumRecordsTarget  # noqa
    from .google_sheets import GoogleSheetsRecordsTarget  # noqa
    from .table import TableRecordsTarget  # noqa
    from .directory_from_url import DirectoryFromUrlRecordsTarget  # noqa


class RecordsTargets(object):
    """These methods produce objects representing the target of a records
    move.  The objects can be used as the 'target' argument to
    :meth:`records_mover.records.move`

    This object should be pulled from the 'targets' property of the
    'records' property on a :class:`records_mover.Session` object
    instead of being constructed directly.

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
                 db_driver: Callable[[Union['Engine', 'Connection']], 'DBDriver']) -> None:
        self.url_resolver = url_resolver
        self.db_driver = db_driver

    def directory_from_url(self,
                           output_url: str,
                           records_format:
                           Optional[BaseRecordsFormat]=None) ->\
            'DirectoryFromUrlRecordsTarget':
        """Represents a Records Directory pointed to by a URL as a target.

        :param output_url: Location to write the records directory.  Must be a URL format
           understood by the records_mover.url library, and must be a directory URL that ends with
           a '/'.
        :param records_format: Description of the format of the data files to write out.  If not
           specified, an efficient format for bulk moves will be chosen.
        """
        from .directory_from_url import DirectoryFromUrlRecordsTarget  # noqa
        return DirectoryFromUrlRecordsTarget(output_url=output_url,
                                             records_format=records_format,
                                             url_resolver=self.url_resolver)

    def table(self,
              db_engine: 'Engine',
              schema_name: str,
              table_name: str,
              existing_table_handling: ExistingTableHandling=
              ExistingTableHandling.DELETE_AND_OVERWRITE,
              drop_and_recreate_on_load_error: bool=False,
              add_user_perms_for: Optional[Dict[str, List[str]]]=None,
              add_group_perms_for: Optional[Dict[str, List[str]]]=None) -> \
            'TableRecordsTarget':
        """Represents a SQLALchemy-accessible database table as as a target.

        :param db_engine: SQLAlchemy database engine to write data to.

        :param schema_name: Schema name of a table to write data to.

        :param table_name: Table name of a table to write data to.

        :param existing_table_handling: When loading into a database table, controls how any
           existing table found will be handled.  This must be a
           :class:`records_mover.records.ExistingTableHandling` object.

        :param drop_and_recreate_on_load_error: If True, table load errors will attempt to be
           addressed by dropping the target table and reloading the incoming data.

        :param add_user_perms_for: If specified, a table's permissions will be set for the
           specified users. Format should be like {'all': ['username1', 'username2'], 'select':
           ['username3', 'username4']}

        :param add_group_perms_for: If specified, a table's permissions will be set for the
           specified group. Format should be like {'all': ['group1', 'group2'], 'select': ['group3',
           'group4']}
        """
        from .table import TableRecordsTarget  # noqa
        return TableRecordsTarget(schema_name=schema_name,
                                  table_name=table_name,
                                  db_engine=db_engine,
                                  db_driver=self.db_driver,
                                  existing_table_handling=existing_table_handling,
                                  drop_and_recreate_on_load_error=drop_and_recreate_on_load_error,
                                  add_user_perms_for=add_user_perms_for,
                                  add_group_perms_for=add_group_perms_for)

    def google_sheet(self,
                     spreadsheet_id: str,
                     sheet_name: str,
                     google_cloud_creds:
                     'google.auth.credentials.Credentials') ->\
            'GoogleSheetsRecordsTarget':
        """Represents a sheet in a Google Sheets spreadsheet as a target, via
        the Google Sheets API.

        :param spreadsheet_id: This is the xyz in
           https://docs.google.com/spreadsheets/d/xyz/edit?ts=5be5b383#gid=abc
        :param sheet_name: This is the label of the particular tab within the Google Sheets
           spreadsheet where the data should go.
        :param google_cloud_creds: Credentials object for Google Cloud Platform access.

        """
        # see the 'gsheets' extras_require option in setup.py - needed for this!
        from .google_sheets import GoogleSheetsRecordsTarget  # noqa
        return GoogleSheetsRecordsTarget(spreadsheet_id=spreadsheet_id,
                                         sheet_name=sheet_name,
                                         google_cloud_creds=google_cloud_creds)

    def fileobj(self,
                output_fileobj: IO[bytes],
                records_format: BaseRecordsFormat) -> FileobjTarget:
        """Represents a stream of data files bytes as a target.

        :param output_fileobj: Stream where the file shoud be written to.
        :param records_format: Description of the format of the data files to write out.  If not
           specified, an efficient format for bulk moves will be chosen.
        """
        return FileobjTarget(fileobj=output_fileobj, records_format=records_format)

    def data_url(self,
                 output_url: str,
                 records_format: Optional[BaseRecordsFormat]=None) -> DataUrlTarget:
        """Represents a URL pointer to a data file as a target.

        :param output_url: Location of the data file to write.  Must be a URL format understood by
           the records_mover.url library corresponding to a file, not a directory (i.e., not ending
           with a '/')
        :param records_format: Description of the format of the data files to write out.  If not
           specified, an efficient format for bulk moves will be chosen.
        """
        output_loc = self.url_resolver.file_url(output_url)
        return DataUrlTarget(output_loc=output_loc,
                             records_format=records_format)

    def local_file(self,
                   filename: str,
                   records_format: Optional[BaseRecordsFormat]=None) ->\
            'DataUrlTarget':
        """Represents a data file on the local filesystem as a target.

        :param filename: File path (relative or absolute) of the data file to unload to.
        :param records_format: Description of the format of the data files to write out.  If not
           specified, an efficient format for bulk moves will be chosen.
        """
        url = pathlib.Path(filename).resolve().as_uri()
        return self.data_url(output_url=url, records_format=records_format)

    def spectrum(self,
                 schema_name: str,
                 table_name: str,
                 db_engine: 'Engine',
                 spectrum_base_url: Optional[str]=None,
                 spectrum_rdir_url: Optional[str]=None,
                 existing_table_handling: ExistingTableHandling=
                 ExistingTableHandling.TRUNCATE_AND_OVERWRITE) ->\
            'SpectrumRecordsTarget':
        """
        Represents a location in Amazon Redshift Spectrum as a target.

        :param schema_name: Schema name of a table to write data to.

        :param table_name: Table name of a table to write data to.

        :param db_engine: SQLAlchemy database engine to write data to.

        :param spectrum_base_url: Root S3 URL under which a simple directory structure will be
           created for files to be stored, if spectrum_rdir_url is not specified.  Note that when
           using the mover CLI, db-facts may be used to provide a default.

        :param spectrum_rdir_url: S3 URL where a records directory with files will be stored;
           otherwise, use db-facts default if exists.  If this is not specified, spectrum_base_url
           must be.

        :param existing_table_handling: When loading into a database table, controls how any
           existing table found will be handled.  This must be a
           :class:`records_mover.records.ExistingTableHandling` object.
        """
        from .spectrum import SpectrumRecordsTarget  # noqa

        return SpectrumRecordsTarget(schema_name=schema_name,
                                     table_name=table_name,
                                     db_engine=db_engine,
                                     db_driver=self.db_driver,
                                     url_resolver=self.url_resolver,
                                     spectrum_base_url=spectrum_base_url,
                                     spectrum_rdir_url=spectrum_rdir_url,
                                     existing_table_handling=existing_table_handling)
