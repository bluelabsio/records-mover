from .base import SupportsToDataframesSource
from contextlib import contextmanager
import itertools
import google.auth.credentials
import collections
from apiclient.discovery import build
import google_auth_httplib2
import httplib2
from typing import Any, Iterator, Optional, Callable, Iterable, List
from .dataframes import DataframesRecordsSource
from ..processing_instructions import ProcessingInstructions

SheetsService = Any


class GoogleSheetsRecordsSource(SupportsToDataframesSource):
    def __init__(self,
                 spreadsheet_id: str,
                 sheet_name_or_range: str,
                 google_cloud_creds: google.auth.credentials.Credentials,
                 out_of_band_column_headers: Optional[Iterable[str]]=None,
                 header_translator: Optional[Callable[[str], str]]=None) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name_or_range = sheet_name_or_range
        self.google_cloud_creds = google_cloud_creds
        self.out_of_band_column_headers = out_of_band_column_headers
        self.header_translator = header_translator

    def _authorize(self) -> google_auth_httplib2.AuthorizedHttp:
        credentials = self.google_cloud_creds
        http = httplib2.Http()
        authed_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=http)
        return authed_http

    def _get_service(self) -> SheetsService:
        http_authorized = self._authorize()
        return build('sheets', 'v4', http=http_authorized)

    @contextmanager
    def to_dataframes_source(self,
                             processing_instructions: ProcessingInstructions) \
            -> Iterator[DataframesRecordsSource]:
        from pandas import DataFrame

        service = self._get_service()

        #
        # https://developers.google.com/sheets/api/reference/rest/v4/ValueRenderOption
        #
        # Values will be calculated, but not formatted in the
        # reply. For example, if A1 is 1.23 and A2 is =A1 and
        # formatted as currency, then A2 would return the number 1.23.
        value_render_option = 'UNFORMATTED_VALUE'
        #
        # https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
        #
        # Instructs date, time, datetime, and duration fields to be
        # output as strings in their given number format (which is
        # dependent on the spreadsheet locale).
        date_time_render_option = 'FORMATTED_STRING'

        result = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_name_or_range,
            majorDimension="COLUMNS",  # pull in col order for easy and efficient pandasing
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option,
        ).execute()
        values = result.get('values', [])

        column_headers: Iterable[str]
        data_by_column: List[List[Any]]
        if self.out_of_band_column_headers is not None:
            column_headers = self.out_of_band_column_headers
            data_by_column = values
        else:
            # Use column headers from sheet
            #
            # Apply column header translator if one was passed in

            raw_column_headers = [str(col[0]) for col in values]
            # https://stackoverflow.com/questions/9835762/how-do-i-find-the-duplicates-in-a-list-and-create-another-list-with-them
            duplicates = [
                item
                for item, count in collections.Counter(raw_column_headers).items()
                if count > 1
            ]
            if duplicates:
                raise TypeError("The following column names are duplicated - "
                                f"please make them unique: {duplicates}")
            column_headers = [
                self.header_translator(header)
                for header in raw_column_headers
            ] if self.header_translator else raw_column_headers
            data_by_column = [col[1:] for col in values]

        # Generate data frame from raw google sheet results
        df = DataFrame(
            list(itertools.zip_longest(*[
                map(lambda x: None if x == '' else x, col) for col in data_by_column
            ])),
            columns=column_headers
        )
        yield DataframesRecordsSource(dfs=[df],
                                      processing_instructions=processing_instructions)
