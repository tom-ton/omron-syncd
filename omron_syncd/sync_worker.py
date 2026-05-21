"""Sync worker: invokes omblepy as a subprocess with the right flags,
under a file lock, with backoff and a time-sync piggy-back rule.
"""

from __future__ import annotations

import asyncio
import enum
import errno
import fcntl
import logging
import os
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

from .config import Config
from .scanner import TriggerEvent

if TYPE_CHECKING:
    from .google_sync import CsvToGoogleSheetsSyncer


class SyncResult(enum.Enum):
    OK = "ok"
    FAILED = "failed"
    # Lock was held by another process (e.g. user running omblepy.py -p
    # manually). Not a sync failure — don't increment backoff.
    SKIPPED = "skipped"


logger = logging.getLogger("omron-syncd.worker")


@contextmanager
def _flock(path) -> Iterator[Optional[int]]:
    """Non-blocking exclusive flock. Yields the open fd on success,
    None if the lock could not be acquired (held by another process).

    On success the fd is closed (releasing the lock) on context exit.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                os.close(fd)
                yield None
                return
            os.close(fd)
            raise
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
            yield fd
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)
    except Exception:
        # Defensive: on any unexpected failure ensure fd is closed.
        try:
            os.close(fd)
        except OSError:
            pass
        raise


class SyncWorker:
    """Wraps the omblepy subprocess; tracks success/failure history so
    the scanner's trigger logic can apply correct backoff.
    """

    def __init__(
        self,
        config: Config,
        google_syncer: Optional["CsvToGoogleSheetsSyncer"] = None,
    ):
        self._config = config
        self._google_syncer = google_syncer
        self._loop_lock = asyncio.Lock()
        self._last_attempt_ts: float = 0.0
        self._last_success_ts: float = 0.0
        self._last_time_sync_ts: float = 0.0
        self._fail_streak: int = 0
        # Optional callback set by daemon.py so the scanner can re-arm
        # its debounce after a failed sync (success doesn't need a
        # callback — the device clears its adv flag and the scanner
        # naturally re-arms on the next 0x01 adv).
        self._on_failure_callback = None
        # When True, regular trigger() calls are no-ops. Used by the
        # daemon to hold off normal -n triggers until the startup full
        # sync (do_full_sync) completes, so they don't race and pull
        # only new records before historical CSVs are seeded.
        self._triggers_suspended: bool = False

    def set_on_failure(self, cb) -> None:
        self._on_failure_callback = cb

    def suspend_triggers(self) -> None:
        self._triggers_suspended = True

    def resume_triggers(self) -> None:
        self._triggers_suspended = False

    @property
    def fail_streak(self) -> int:
        return self._fail_streak

    @property
    def last_success_ts(self) -> float:
        return self._last_success_ts

    def _min_interval_now(self) -> int:
        if self._fail_streak == 0:
            return self._config.min_sync_interval_s
        idx = min(self._fail_streak - 1, len(self._config.backoff_after_fail_s) - 1)
        return self._config.backoff_after_fail_s[idx]

    def _should_time_sync(self, now: float) -> bool:
        if self._config.time_sync_interval_s <= 0:
            return False
        return (now - self._last_time_sync_ts) >= self._config.time_sync_interval_s

    async def _push_to_google_if_enabled(self) -> None:
        """Run the Google Sheets sync, if configured. Called OUTSIDE the
        BLE locks (both ``_loop_lock`` and the file flock) so a slow
        Google API call doesn't keep BLE coordination locks held.
        ``CsvToGoogleSheetsSyncer.sync_all`` already isolates per-user
        failures, so this never raises.
        """
        if self._google_syncer is None:
            return
        await self._google_syncer.sync_all()

    async def trigger(self, evt: TriggerEvent) -> None:
        """Entry point called from scanner. Applies rate-limit, then
        runs the sync (under both an asyncio lock and a file lock).
        """
        if self._triggers_suspended:
            logger.debug(
                "trigger received but triggers suspended (startup full "
                "sync in progress); ignoring"
            )
            return

        now = time.monotonic()
        wait_remaining = self._min_interval_now() - (now - self._last_attempt_ts)
        if self._last_attempt_ts and wait_remaining > 0:
            logger.info(
                "trigger received but rate-limit active "
                "(%.0fs remaining; fail_streak=%d)",
                wait_remaining,
                self._fail_streak,
            )
            return

        if self._loop_lock.locked():
            logger.debug("sync already in progress in-process, ignoring trigger")
            return

        async with self._loop_lock:
            wall_now = time.time()
            do_time_sync = self._should_time_sync(wall_now)
            result = await self._run_omblepy(
                time_sync=do_time_sync, incremental=True
            )
            if result is SyncResult.SKIPPED:
                # Don't update last_attempt_ts; we never actually attempted.
                # Re-arm scanner so the next adv burst can retry.
                if self._on_failure_callback is not None:
                    try:
                        self._on_failure_callback()
                    except Exception:  # noqa: BLE001
                        logger.exception("on_failure callback raised")
                return
            self._last_attempt_ts = time.monotonic()
            if result is SyncResult.OK:
                self._fail_streak = 0
                self._last_success_ts = time.monotonic()
                if do_time_sync:
                    self._last_time_sync_ts = wall_now
                    logger.info("sync ok (with time-sync)")
                else:
                    logger.info("sync ok")
            else:
                self._fail_streak += 1
                next_wait = self._min_interval_now()
                logger.warning(
                    "sync failed (fail_streak=%d); next attempt allowed in %ds",
                    self._fail_streak,
                    next_wait,
                )
                if self._on_failure_callback is not None:
                    try:
                        self._on_failure_callback()
                    except Exception:  # noqa: BLE001
                        logger.exception("on_failure callback raised")

        # Google Sheets push lives OUTSIDE _loop_lock and the file flock
        # so that a slow HTTPS round-trip doesn't block the next BLE
        # trigger or stall any manual omblepy.py invocation.
        if result is SyncResult.OK:
            await self._push_to_google_if_enabled()

    async def do_full_sync(self) -> SyncResult:
        """One-off full sync (omblepy without -n), invoked at daemon
        startup when no output CSVs are present. Reads all 100
        ring-buffer slots per user so the CSV is seeded with the
        complete on-device history. Bypasses the rate-limit (it's a
        startup operation, not part of the trigger loop) but still
        takes both the asyncio and file locks.
        """
        if self._loop_lock.locked():
            logger.debug(
                "sync already in progress in-process; full sync deferred"
            )
            return SyncResult.SKIPPED

        async with self._loop_lock:
            self._last_attempt_ts = time.monotonic()
            wall_now = time.time()
            do_time_sync = self._should_time_sync(wall_now)
            result = await self._run_omblepy(
                time_sync=do_time_sync, incremental=False
            )
            if result is SyncResult.OK:
                self._fail_streak = 0
                self._last_success_ts = time.monotonic()
                if do_time_sync:
                    self._last_time_sync_ts = wall_now
                    logger.info("full sync ok (with time-sync)")
                else:
                    logger.info("full sync ok")
            elif result is SyncResult.FAILED:
                self._fail_streak += 1
                logger.warning(
                    "full sync failed (fail_streak=%d); regular triggers "
                    "will retry on the next adv burst",
                    self._fail_streak,
                )

        # Mirror trigger(): Google push happens outside the BLE locks.
        if result is SyncResult.OK:
            await self._push_to_google_if_enabled()
        return result

    async def _run_omblepy(
        self, *, time_sync: bool, incremental: bool
    ) -> SyncResult:
        cfg = self._config
        cfg.output_dir.mkdir(parents=True, exist_ok=True)
        omblepy_script = cfg.omblepy_dir / "omblepy.py"
        if not omblepy_script.is_file():
            logger.error(
                "omblepy.py not found at %s — set OMRON_SYNCD_OMBLEPY_DIR",
                omblepy_script,
            )
            return SyncResult.FAILED
        cmd: list[str] = [
            cfg.omblepy_python,
            str(omblepy_script),
            "-d", cfg.device_driver,
            "-m", cfg.device_mac,
        ]
        if incremental:
            cmd.append("-n")
            # Incremental syncs fire after every measurement; the
            # timestamped CSV backups omblepy writes by default would
            # otherwise accumulate forever. Skip them. The startup
            # full sync (incremental=False) is rare and is allowed to
            # backup the existing CSVs (defensive: that's the path
            # that overwrites a possibly-non-empty CSV with whatever
            # comes off the device, so a one-shot backup before the
            # write has actual value).
            cmd.append("--noBackup")
        if time_sync:
            cmd.append("-t")

        # Run with cwd=output_dir so omblepy's hard-coded
        # `pathlib.Path("user{N}.csv")` writes land in the right place.
        logger.info("running omblepy: %s (cwd=%s)", " ".join(cmd), cfg.output_dir)

        with _flock(cfg.lock_path) as fd:
            if fd is None:
                logger.info(
                    "lock %s held by another process; skipping this sync "
                    "(no backoff applied)",
                    cfg.lock_path,
                )
                return SyncResult.SKIPPED
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(cfg.output_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    # PYTHONUNBUFFERED so omblepy's logging.StreamHandler
                    # flushes line-by-line into our pipe rather than block-
                    # buffering. Without this, output only appears when
                    # omblepy exits, which makes the daemon look stuck.
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
            except FileNotFoundError as e:
                logger.error("failed to launch omblepy: %s", e)
                return SyncResult.FAILED

            try:
                rc = await asyncio.wait_for(
                    self._stream_subprocess(proc),
                    timeout=cfg.sync_subprocess_timeout_s,
                )
            except asyncio.TimeoutError:
                logger.error(
                    "omblepy timed out after %ds; killing",
                    cfg.sync_subprocess_timeout_s,
                )
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()
                return SyncResult.FAILED

        if rc == 0:
            return SyncResult.OK
        logger.error("omblepy exited with code %s", rc)
        return SyncResult.FAILED

    async def _stream_subprocess(self, proc) -> int:
        """Drain the subprocess's stdout line-by-line, logging each line
        as it arrives. Returns the exit code once stdout EOFs and the
        process is reaped.
        """
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip("\r\n")
            if text:
                logger.info("omblepy: %s", text)
        return await proc.wait()
