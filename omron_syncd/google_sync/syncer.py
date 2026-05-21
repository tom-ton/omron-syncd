"""Append-only CSV → Google Sheets syncer.

For each configured user, reads the omblepy-produced per-user CSV,
filters out rows whose ``datetime`` is <= the last-synced datetime
persisted on disk, and appends the remainder to the configured Google
Sheets worksheet.

State (``last_datetime`` per user) lives in JSON files next to the
CSVs so we survive daemon restarts and don't re-upload everything
each cycle. The append-then-save ordering means a state-save failure
after a successful append produces duplicate rows on the next run
(easier to detect / clean up than missing rows); a
state-save-before-append ordering would risk silent data loss
instead.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Dict, List

from .client import GoogleSheetsClient
from .config import GoogleSheetsConfig, GoogleSheetsUserConfig
from .state import SyncStateStore


LOGGER = logging.getLogger(__name__)


class CsvToGoogleSheetsSyncer:
    def __init__(
        self,
        config: GoogleSheetsConfig,
        state_dir: Path,
    ):
        self.config = config
        self.state_store = SyncStateStore(state_dir)

        self.user_configs: Dict[str, GoogleSheetsUserConfig] = {
            u.user: u for u in config.users
        }

        self.locks: Dict[str, asyncio.Lock] = {
            u.user: asyncio.Lock() for u in config.users
        }

        # Cache one client per user. Service-account-issued OAuth tokens
        # auto-refresh inside the client, so long-lived instances are
        # fine and save ~500 ms of credentials + discovery-doc work per
        # sync. Instances are lazily created on first use because
        # construction does I/O (reads the SA JSON, builds the discovery
        # service) and we'd rather fail at sync-time than at startup.
        self._clients: Dict[str, GoogleSheetsClient] = {}

    def _get_client(self, user: str) -> GoogleSheetsClient:
        client = self._clients.get(user)
        if client is None:
            cfg = self.user_configs[user]
            client = GoogleSheetsClient(
                service_account_json=str(cfg.service_account_json),
                spreadsheet_id=cfg.spreadsheet_id,
                worksheet_name=cfg.worksheet_name,
            )
            self._clients[user] = client
        return client

    async def sync_all(self) -> None:
        """Sync every configured user. Failures per user are isolated:
        one user's exception does not prevent another's sync.
        """
        for user in self.user_configs:
            try:
                await self.sync_user(user)
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "Google Sheets sync failed for user %s", user
                )

    async def sync_user(self, user: str) -> None:
        if user not in self.user_configs:
            return

        async with self.locks[user]:
            await asyncio.to_thread(self._sync_user_blocking, user)

    def _sync_user_blocking(self, user: str) -> None:
        cfg = self.user_configs[user]

        if not cfg.csv_path.exists():
            LOGGER.warning("CSV file does not exist: %s", cfg.csv_path)
            return

        last_dt = self.state_store.get_last_datetime(user)

        rows_to_append: List[List[str]] = []
        newest_dt = last_dt

        # NOTE: lexicographic comparison on the datetime string works
        # only because omblepy writes "YYYY-MM-DD HH:MM:SS" (ISO-like,
        # zero-padded, no timezone). Don't change the CSV format
        # upstream without updating this comparison.
        with cfg.csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                row_dt = row["datetime"]

                if last_dt and row_dt <= last_dt:
                    continue

                rows_to_append.append(
                    [
                        row["datetime"],
                        row["dia"],
                        row["sys"],
                        row["bpm"],
                        row["mov"],
                        row["ihb"],
                    ]
                )

                # Defensive against an unsorted CSV — omblepy currently
                # sorts ascending but we shouldn't rely on it.
                if newest_dt is None or row_dt > newest_dt:
                    newest_dt = row_dt

        if not rows_to_append:
            LOGGER.info("No new rows to sync for user %s", user)
            return

        LOGGER.info(
            "Syncing %d rows to Google Sheets for user %s",
            len(rows_to_append),
            user,
        )

        client = self._get_client(user)
        client.append_rows(rows_to_append)

        if newest_dt:
            self.state_store.save_last_datetime(user, newest_dt)

        LOGGER.info("Google Sheets sync completed for user %s", user)
