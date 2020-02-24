#!/usr/bin/env python3

from records_mover import Session
import google_auth_httplib2
from googleapiclient.errors import HttpError
from apiclient.discovery import build
import googleapiclient
import httplib2
from datetime import datetime, timedelta
from typing import Any, List
import sys
import logging

logger = logging.getLogger(__name__)


SheetsService = Any


def delete_sheet_by_id(service: SheetsService,
                       spreadsheet_id: str,
                       sheet_id: int) -> None:
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#DeleteSheetRequest
    batch_update_spreadsheet_request_body = {
        'requests': [
            {
                "deleteSheet": {
                    "sheetId": sheet_id
                }
            }
        ],
    }

    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                 body=batch_update_spreadsheet_request_body)
    try:
        request.execute()
    except HttpError as e:
        logger.info(e)
        # continue on - likely because something else in parallel hit
        # this at once, and even if not, we can garbage collect this
        # sheet in the next attempt with the next batch of tests


def is_old(sheet_name: str) -> bool:
    sheet_name_components = sheet_name.split('_')
    sheet_epoch_str = sheet_name_components[-1]  # last element
    sheet_name = "_".join(sheet_name_components[:-1])  # all but last element

    sheet_epoch = int(sheet_epoch_str)
    sheet_datetime = datetime.fromtimestamp(sheet_epoch)
    one_day_ago_datetime = datetime.now() - timedelta(days=1)
    return sheet_datetime < one_day_ago_datetime


def find_old_test_sheets(service: SheetsService,
                         spreadsheet_id: str) -> List[int]:
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
    request =\
        service.spreadsheets().get(spreadsheetId=spreadsheet_id,
                                   includeGridData=False)
    response = request.execute()
    sheets = [
        sheet['properties']
        for sheet in response['sheets']
        if sheet['properties']['title'].startswith('target_') and
        is_old(sheet['properties']['title'])
    ]
    return [sheet['sheetId'] for sheet in sheets]


def purge_old_test_sheets(cred_name: str,
                          spreadsheet_id: str) -> None:
    session = Session()

    creds = session.creds

    gsheet_creds = creds.google_sheets(cred_name)

    http_authorized = authorize(gsheet_creds)

    service = build('sheets', 'v4', http=http_authorized)

    sheet_ids = find_old_test_sheets(service,
                                     spreadsheet_id)

    for sheet_id in sheet_ids:
        try:
            delete_sheet_by_id(service,
                               spreadsheet_id,
                               sheet_id)
        except googleapiclient.errors.HttpError:
            print(f"Could not delete sheet {sheet_id} (another purger running at the same time?)")


def authorize(credentials):
    http = httplib2.Http()
    authed_http = google_auth_httplib2.AuthorizedHttp(
        credentials, http=http)
    return authed_http


if __name__ == '__main__':
    cred_name: str = sys.argv[1]
    spreadsheet_id: str = sys.argv[2]

    purge_old_test_sheets(cred_name, spreadsheet_id)
