"""Runtime configuration for the omron-syncd daemon.

Configuration is read from environment variables (typically set in the
systemd unit) so the daemon stays a single static Python file with no
external config-file parser.

All time values are in seconds unless noted.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass, field


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"env var {name}={raw!r} is not an int") from e


def _env_path(name: str, default: pathlib.Path) -> pathlib.Path:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return pathlib.Path(os.path.expanduser(raw))


def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return list(default)
    try:
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except ValueError as e:
        raise ValueError(
            f"env var {name}={raw!r} must be comma-separated ints"
        ) from e


@dataclass(frozen=True)
class Config:
    # --- BLE target ---------------------------------------------------------
    device_mac: str
    """MAC of the BP monitor, upper-case colon form (e.g. F1:95:48:45:81:6B)."""

    device_driver: str = "HEM-7380T1-EBK"
    """omblepy `-d` argument; matches a file in deviceSpecific/."""

    manufacturer_id: int = 0x020E
    """Bluetooth SIG company id Omron uses in advertisement mfr-data."""

    new_data_flag_mask: int = 0x40
    """Bit in mfr-data[1] that means 'unsynced records present'."""

    adapter: str | None = None
    """BlueZ adapter name (e.g. 'hci0'). None = bleak default."""

    # --- Trigger logic ------------------------------------------------------
    debounce_threshold: int = 2
    """Consecutive new-data adv reports required before connecting."""

    min_sync_interval_s: int = 60
    """Minimum seconds between successful sync attempts."""

    backoff_after_fail_s: list[int] = field(
        default_factory=lambda: [5, 30, 120, 300, 900]
    )
    """Backoff schedule applied after consecutive sync failures.
    Index = fail streak (clamped to last entry)."""

    time_sync_interval_s: int = 24 * 3600
    """Minimum age of last successful time-sync before piggy-backing -t
    onto the next data sync."""

    # --- Subprocess / paths -------------------------------------------------
    omblepy_dir: pathlib.Path = pathlib.Path("/opt/omblepy")
    """Directory containing omblepy.py (and deviceSpecific/)."""

    omblepy_python: str = "python3"
    """Python interpreter to invoke omblepy with."""

    output_dir: pathlib.Path = pathlib.Path.home() / ".local/share/omron-bp"
    """CWD for the omblepy subprocess; CSVs/JSONs land here."""

    lock_path: pathlib.Path = pathlib.Path.home() / ".cache/omron-syncd.lock"
    """Inter-process lock so daemon and manual omblepy invocations don't
    fight over the BLE connection."""

    sync_subprocess_timeout_s: int = 180
    """Hard cap on a single omblepy invocation."""

    # --- Logging ------------------------------------------------------------
    log_level: str = "INFO"
    """Root log level for the daemon (DEBUG/INFO/WARNING/ERROR)."""

    @classmethod
    def from_env(cls) -> "Config":
        mac = _env_str("OMRON_SYNCD_MAC", "")
        if not mac:
            raise SystemExit(
                "OMRON_SYNCD_MAC is required (e.g. F1:95:48:45:81:6B)"
            )
        return cls(
            device_mac=mac.upper(),
            device_driver=_env_str("OMRON_SYNCD_DRIVER", "HEM-7380T1-EBK"),
            manufacturer_id=_env_int("OMRON_SYNCD_MFR_ID", 0x020E),
            new_data_flag_mask=_env_int("OMRON_SYNCD_NEW_FLAG", 0x40),
            adapter=os.environ.get("OMRON_SYNCD_ADAPTER") or None,
            debounce_threshold=_env_int("OMRON_SYNCD_DEBOUNCE", 2),
            min_sync_interval_s=_env_int("OMRON_SYNCD_MIN_INTERVAL", 60),
            backoff_after_fail_s=_env_int_list(
                "OMRON_SYNCD_BACKOFF", [5, 30, 120, 300, 900]
            ),
            time_sync_interval_s=_env_int(
                "OMRON_SYNCD_TIME_SYNC_INTERVAL", 24 * 3600
            ),
            omblepy_dir=_env_path(
                "OMRON_SYNCD_OMBLEPY_DIR", pathlib.Path("/opt/omblepy")
            ),
            omblepy_python=_env_str("OMRON_SYNCD_PYTHON", "python3"),
            output_dir=_env_path(
                "OMRON_SYNCD_OUTPUT_DIR",
                pathlib.Path.home() / ".local/share/omron-bp",
            ),
            lock_path=_env_path(
                "OMRON_SYNCD_LOCK",
                pathlib.Path.home() / ".cache/omron-syncd.lock",
            ),
            sync_subprocess_timeout_s=_env_int(
                "OMRON_SYNCD_SYNC_TIMEOUT", 180
            ),
            log_level=_env_str("OMRON_SYNCD_LOG_LEVEL", "INFO").upper(),
        )
