from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


class SyncStateStore:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, user: str) -> Path:
        return self.state_dir / f"google-sync-{user}.json"

    def get_last_datetime(self, user: str) -> Optional[str]:
        path = self._state_path(user)

        if not path.exists():
            return None

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        return data.get("last_datetime")

    def save_last_datetime(self, user: str, dt: str) -> None:
        path = self._state_path(user)

        with path.open("w", encoding="utf-8") as f:
            json.dump({"last_datetime": dt}, f)

