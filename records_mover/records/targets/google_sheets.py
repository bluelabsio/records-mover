from ...utils.retry import google_sheets_retry
from .base import SupportsMoveFromDataframes
import math
import google.auth.credentials
import numpy as np
from ..results import MoveResult
from ..processing_instructions import ProcessingInstructions
from apiclient.discovery import build
from googleapiclient.errors import HttpError
import google_auth_httplib2
import httplib2
from typing import Any, Union, List
from ..sources.dataframes import DataframesRecordsSource
import logging

logger = logging.getLogger(__name__)

SheetsService = Any
CellData = Union[int, str, float]


class GoogleSheetsRecordsTarget(SupportsMoveFromDataframes):
    def __init__(self,
                 spreadsheet_id: str,
                 sheet_name: str,
                 google_cloud_creds: google.auth.credentials.Credentials) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.google_cloud_creds = google_cloud_creds

    @google_sheets_retry()
    def _create_sheet(self,
                      service: SheetsService,
                      spreadsheet_id: str,
                      sheet_name: str) -> None:
        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#AddSheetRequest
        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties
        batch_update_spreadsheet_request_body = {
            'requests': [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name
                        }
                    }
                }
            ],
        }

        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                     body=batch_update_spreadsheet_request_body)
        request.execute()

    @google_sheets_retry()
    def _clear_sheet(self,
                     service: SheetsService,
                     spreadsheet_id: str,
                     sheet_name: str) -> None:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            body={}
        ).execute()

    def _clear_or_create_sheet(self,
                               service: SheetsService,
                               spreadsheet_id: str,
                               sheet_name: str) -> None:
        try:
            self._clear_sheet(service, spreadsheet_id, sheet_name)
        except HttpError:
            self._create_sheet(service, spreadsheet_id, sheet_name)

    @google_sheets_retry()
    def _naive_load_to_sheet(self,
                             service: SheetsService,
                             sheet_id: str,
                             sheet_name: str,
                             data: List[List[CellData]]) -> None:
        # assumes an empty sheet; starts in the upper left corner, cell A1
        body = {
            'values': data
        }

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=sheet_name,
            valueInputOption='RAW',
            body=body,
        ).execute()

    def _authorize(self) -> google_auth_httplib2.AuthorizedHttp:
        credentials = self.google_cloud_creds
        http = httplib2.Http()
        authed_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=http)
        return authed_http

    def _get_service(self) -> SheetsService:
        http_authorized = self._authorize()
        return build('sheets', 'v4', http=http_authorized)

    def as_json_serializable(self, cell: Any) -> Any:
        if isinstance(cell, np.generic):
            native = np.asscalar(cell)
        else:
            native = cell
        if isinstance(cell, float) and math.isnan(native):
            native = 'NaN'
        return native

    def move_from_dataframes_source(self,
                                    dfs_source: DataframesRecordsSource,
                                    processing_instructions:
                                    ProcessingInstructions) -> MoveResult:
        dfs_iterator = iter(dfs_source.dfs)
        df = next(dfs_iterator)
        if next(dfs_iterator, None) is not None:
            raise NotImplementedError("Please teach me how to handle "
                                      "more than one dataframe at a time")
        data = df.to_records(index=dfs_source.include_index)
        # these need to be regular Python types so that we can
        # serialize them to JSON over the network as we send them to
        # Google Sheets.
        json_encodable_data = [[self.as_json_serializable(cell) for cell in row] for row in data]
        service = self._get_service()
        self._clear_or_create_sheet(service, self.spreadsheet_id, self.sheet_name)
        self._naive_load_to_sheet(service, self.spreadsheet_id, self.sheet_name,
                                  json_encodable_data)
        return MoveResult(move_count=len(data), output_urls=None)
