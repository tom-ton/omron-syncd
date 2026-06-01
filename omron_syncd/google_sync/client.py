from __future__ import annotations

import http.client
import logging
import ssl
import time
from typing import Any, List

from google.auth.exceptions import TransportError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


LOGGER = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# googleapiclient's built-in HTTP retries do not cover TLS drops and
# RemoteDisconnected during token refresh (httplib2 / stdlib ssl).
_RETRIABLE: tuple[type[BaseException], ...] = (
    http.client.RemoteDisconnected,
    ssl.SSLError,
    ConnectionError,
    TimeoutError,
    TransportError,
)


def _exception_chain(exc: BaseException) -> List[BaseException]:
    """Collect related exceptions following __cause__ / __context__ links."""
    seen: set[int] = set()
    out: List[BaseException] = []
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        out.append(cur)
        cur = cur.__cause__ or cur.__context__
    return out


def _is_retriable_transport_error(exc: BaseException) -> bool:
    return any(isinstance(e, _RETRIABLE) for e in _exception_chain(exc))


def _execute_with_retries(
    request: Any,
    *,
    attempts: int = 5,
    base_delay_s: float = 1.0,
) -> Any:
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return request.execute()
        except Exception as e:
            last = e
            if attempt >= attempts or not _is_retriable_transport_error(e):
                raise
            delay = min(base_delay_s * (2 ** (attempt - 1)), 30.0)
            LOGGER.warning(
                "Google Sheets API transient error (attempt %d/%d), "
                "retrying in %.1fs: %s",
                attempt,
                attempts,
                delay,
                e,
            )
            time.sleep(delay)
    assert last is not None
    raise last


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
            num_retries=5,
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

        _execute_with_retries(request)

