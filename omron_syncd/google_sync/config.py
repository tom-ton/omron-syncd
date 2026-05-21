from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class GoogleSheetsUserConfig:
    user: str
    csv_path: Path
    spreadsheet_id: str
    worksheet_name: str
    service_account_json: Path


@dataclass
class GoogleSheetsConfig:
    enabled: bool
    users: List[GoogleSheetsUserConfig]


TRUE_VALUES = {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in TRUE_VALUES


def load_google_sheets_config(output_dir: str) -> GoogleSheetsConfig:
    enabled = _env_bool("OMRON_SYNCD_GSHEETS_ENABLED", False)

    if not enabled:
        return GoogleSheetsConfig(enabled=False, users=[])

    raw_users = os.getenv("OMRON_SYNCD_GSHEETS_USERS", "")
    user_names = [u.strip() for u in raw_users.split(",") if u.strip()]

    users: List[GoogleSheetsUserConfig] = []

    for user in user_names:
        prefix = f"OMRON_SYNCD_GSHEETS_{user.upper()}"

        csv_name = os.environ[f"{prefix}_CSV"]
        spreadsheet_id = os.environ[f"{prefix}_SPREADSHEET_ID"]
        worksheet_name = os.getenv(f"{prefix}_WORKSHEET", "Sheet1")
        sa_json = os.environ[f"{prefix}_SA_JSON"]

        users.append(
            GoogleSheetsUserConfig(
                user=user,
                csv_path=Path(output_dir) / csv_name,
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name,
                service_account_json=Path(sa_json),
            )
        )

    return GoogleSheetsConfig(enabled=True, users=users)

