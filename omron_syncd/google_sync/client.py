from __future__ import annotations

from typing import List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsClient:
    def __init__(
        self,
        service_account_json: str,
        spreadsheet_id: str,
        worksheet_name: str,
    ):
        creds = Credentials.from_service_account_file(
            service_account_json,
            scopes=SCOPES,
        )

        self.service = build(
            "sheets",
            "v4",
            credentials=creds,
            cache_discovery=False,
        )

        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name

    def append_rows(self, rows: List[List[str]]) -> None:
        if not rows:
            return

        body = {
            "values": rows,
        }

        request = self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.worksheet_name}!A:A",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )

        request.execute()

