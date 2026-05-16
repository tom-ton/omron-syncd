"""Top-level daemon: wires Config + AdvScanner + SyncWorker together,
sets up logging, installs signal handlers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import signal
import sys

from .config import Config
from .scanner import AdvScanner, TriggerEvent
from .sync_worker import SyncResult, SyncWorker


logger = logging.getLogger("omron-syncd")


_NOISY_LIBRARY_LOGGERS = ("bleak", "dbus_fast", "dbus_next", "asyncio")


def _startup_full_sync_needed(output_dir: pathlib.Path) -> bool:
    """A "fresh install" is one where NEITHER user1.csv NOR user2.csv
    exists in the output directory. omblepy writes both (even if just
    the header) on every sync, so absence of both is a reliable signal
    that the daemon has never produced any output here yet. If only
    one is missing it's user-fiddling, not our problem.
    """
    user1 = output_dir / "user1.csv"
    user2 = output_dir / "user2.csv"
    return not (user1.exists() or user2.exists())


async def _initial_full_sync(
    scanner: AdvScanner,
    worker: SyncWorker,
    stop_event: asyncio.Event,
) -> None:
    """Wait for the first advertisement from the target device (so we
    know it's currently reachable, not in a deep-sleep gap), then run
    a full sync via omblepy. Re-enables regular triggers on the worker
    afterwards regardless of outcome.
    """
    logger.info(
        "waiting for first adv from target device before startup full sync"
    )
    waiter = asyncio.create_task(scanner.wait_for_first_adv())
    stopper = asyncio.create_task(stop_event.wait())
    try:
        done, pending = await asyncio.wait(
            [waiter, stopper], return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        if stop_event.is_set():
            logger.info("startup full sync aborted (shutdown)")
            return

        logger.info("device is advertising; running startup full sync")
        try:
            result = await worker.do_full_sync()
        except Exception:  # noqa: BLE001
            logger.exception("startup full sync raised; resuming triggers")
            return
        if result is SyncResult.OK:
            logger.info("startup full sync complete; resuming normal triggers")
        else:
            logger.warning(
                "startup full sync result=%s; resuming normal triggers "
                "(next 0x41 burst will retry via -n)",
                result.value,
            )
    finally:
        worker.resume_triggers()


def _setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name, logging.INFO)
    handler = logging.StreamHandler()
    # systemd-journald prefixes timestamps itself; keep our format minimal
    # but include logger name so scanner/worker are distinguishable.
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # bleak's bluezdbus.manager (and the underlying dbus_fast) emit one
    # DEBUG line per inbound DBus PropertiesChanged signal, for *every*
    # BLE device the adapter sees - not just our target MAC. That drowns
    # out our own scanner DEBUG lines. Clamp them well above DEBUG even
    # when our own level is DEBUG. Override via OMRON_SYNCD_LIB_LOG_LEVEL
    # (e.g. set to DEBUG when actually debugging bleak itself).
    lib_level_name = os.environ.get("OMRON_SYNCD_LIB_LOG_LEVEL", "WARNING").upper()
    lib_level = getattr(logging, lib_level_name, logging.WARNING)
    for noisy in _NOISY_LIBRARY_LOGGERS:
        logging.getLogger(noisy).setLevel(lib_level)


async def _amain() -> int:
    config = Config.from_env()
    _setup_logging(config.log_level)

    logger.info(
        "omron-syncd starting (mac=%s driver=%s output=%s omblepy=%s)",
        config.device_mac, config.device_driver,
        config.output_dir, config.omblepy_dir,
    )

    worker = SyncWorker(config)
    stop_event = asyncio.Event()

    async def _on_trigger(evt: TriggerEvent) -> None:
        await worker.trigger(evt)

    async def _on_confirmation() -> None:
        # Currently informational only; the scanner already resets its
        # debounce internally on a no-new-data adv.
        if worker.last_success_ts:
            logger.debug("device adv shows new-data flag cleared (post-sync)")

    scanner = AdvScanner(
        config=config,
        on_trigger=_on_trigger,
        on_confirmation=_on_confirmation,
    )
    worker.set_on_failure(scanner.re_arm)

    # If this looks like a fresh install (no CSVs in output_dir),
    # suspend normal triggers and schedule a one-off full sync that
    # runs as soon as the device proves it's reachable (first adv).
    # The suspend MUST happen before scanner.run() so that an adv
    # burst arriving immediately on startup can't race us into running
    # an -n sync before the full sync seeds the historical CSVs.
    if _startup_full_sync_needed(config.output_dir):
        logger.info(
            "no CSVs in %s; will run a startup full sync after first adv",
            config.output_dir,
        )
        worker.suspend_triggers()
        asyncio.create_task(_initial_full_sync(scanner, worker, stop_event))

    loop = asyncio.get_running_loop()

    def _signal_handler(signame: str) -> None:
        if stop_event.is_set():
            logger.warning("second %s received; force exit", signame)
            sys.exit(1)
        logger.info("received %s; shutting down", signame)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
        except NotImplementedError:
            # Windows etc. — daemon is Linux-only in practice.
            pass

    try:
        await scanner.run(stop_event)
    except Exception:  # noqa: BLE001
        logger.exception("scanner crashed; daemon exiting non-zero")
        return 1
    return 0


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 0
    sys.exit(rc)


if __name__ == "__main__":
    main()
