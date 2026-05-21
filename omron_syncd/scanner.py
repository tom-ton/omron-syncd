"""BLE advertisement scanner with new-data flag detection and debouncing.

Implements the trigger half of the daemon. We don't open a BLE connection
here; we only watch advertisement reports and call out to the sync worker
when the conditions are met.

The empirical basis for the manufacturer-data layout and the
"two consecutive 0x40 reports" debounce is documented in the omblepy
repo's `ANDROID_ADV_ANALYSIS.md` and `DAEMON_BRIEF.md` (sections 6 and
7.2).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from bleak.exc import BleakDBusError

from .config import Config


# BlueZ takes a moment to process the DBus disconnect after the previous
# daemon instance exits. A quick restart (e.g. manual `systemctl restart`,
# which doesn't honour RestartSec) can race that grace period and see the
# stale scan registration as "still in progress". Retry a handful of times
# with exponential backoff before giving up; total worst-case wait below
# is 31s, less than systemd's default TimeoutStartSec.
_SCAN_START_RETRY_DELAYS_S = (1.0, 2.0, 4.0, 8.0, 16.0)
_DBUS_ERR_IN_PROGRESS = "org.bluez.Error.InProgress"


logger = logging.getLogger("omron-syncd.scanner")


@dataclass
class TriggerEvent:
    """Carries the decoded manufacturer-data state at trigger time.

    Only ``status_byte`` is interpreted; the counters X and Y are passed
    through verbatim because their semantics aren't fully understood
    (see decode_manufacturer_data docstring).
    """

    status_byte: int
    counter_x: Optional[int]
    counter_y: Optional[int]


SyncCallback = Callable[[TriggerEvent], Awaitable[None]]
ConfirmationCallback = Callable[[], Awaitable[None]]


def decode_manufacturer_data(mfr_bytes: bytes) -> Optional[TriggerEvent]:
    """Decode the 12-byte payload Omron uses inside its mfr-data blob.

    Layout (after the 2-byte company id, which bleak strips for us):
        byte 0   : 0x06 constant
        byte 1   : status flags (bit 0 = BPM type, bit 6 = new data)
        byte 2-3 : u16-le counter X (monotonic, +1 per new measurement)
        byte 4-5 : u16-le counter Y (also monotonic, +1 per new
                   measurement; X-Y has been constantly 2 in every
                   observed capture so far)
        byte 6-11: reserved (0x00)

    Empirically, neither X nor Y is reset by a successful sync; only
    the bit-6 flag in the status byte flips back to 0. So the only
    field the trigger logic should care about is ``status_byte``.

    We only require >=2 bytes (the status byte) to be useful; the
    counters are returned best-effort for logging/diagnostics.
    """
    if mfr_bytes is None or len(mfr_bytes) < 2:
        return None
    status = mfr_bytes[1]
    x = y = None
    if len(mfr_bytes) >= 4:
        x = int.from_bytes(mfr_bytes[2:4], "little")
    if len(mfr_bytes) >= 6:
        y = int.from_bytes(mfr_bytes[4:6], "little")
    return TriggerEvent(status_byte=status, counter_x=x, counter_y=y)


class AdvDebouncer:
    """Counts consecutive new-data adv reports and fires once per burst.

    The omblepy fork now reliably clears the adv "new data" flag
    (status byte bit 6) after a successful sync — see DAEMON_BRIEF.md
    §6 and §7.3 in the omblepy repo. So the debounce is back to its
    original Android-app-mimicking shape: count consecutive 0x41
    reports, fire once when threshold is reached, re-arm on the next
    0x01 (no-new-data) adv.

    Double-firing inside the ~5 s window between omblepy disconnect
    and the device re-advertising the cleared flag is prevented by
    the sync worker's ``min_sync_interval_s`` rate-limit, not by
    debouncer state.
    """

    def __init__(self, threshold: int, new_flag_mask: int):
        self._threshold = threshold
        self._mask = new_flag_mask
        self._consecutive = 0
        self._already_fired = False

    @property
    def consecutive(self) -> int:
        return self._consecutive

    def observe(self, status_byte: int) -> bool:
        """Feed a status byte; return True iff this observation should
        trigger a sync.
        """
        has_new_data = bool(status_byte & self._mask)
        if not has_new_data:
            if self._consecutive or self._already_fired:
                logger.debug("device reports no-new-data, clearing debounce")
            self._consecutive = 0
            self._already_fired = False
            return False

        self._consecutive += 1
        if self._already_fired:
            return False
        if self._consecutive < self._threshold:
            logger.debug(
                "new-data adv %d/%d (status=0x%02x)",
                self._consecutive, self._threshold, status_byte,
            )
            return False
        self._already_fired = True
        return True

    def force_re_arm(self) -> None:
        """After a FAILED sync attempt, drop the fire latch so the next
        adv burst can re-trigger us without having to wait for the
        device to clear the flag.
        """
        self._already_fired = False


class AdvScanner:
    """Wraps BleakScanner, filters to the configured device MAC, decodes
    Omron manufacturer-data, and fans out trigger events.
    """

    def __init__(
        self,
        config: Config,
        on_trigger: SyncCallback,
        on_confirmation: Optional[ConfirmationCallback] = None,
    ):
        self._config = config
        self._on_trigger = on_trigger
        self._on_confirmation = on_confirmation
        self._debouncer = AdvDebouncer(
            threshold=config.debounce_threshold,
            new_flag_mask=config.new_data_flag_mask,
        )
        self._mac_target = config.device_mac.upper()
        self._scanner: Optional[BleakScanner] = None
        # Serializes scheduled sync callbacks; the worker also takes its own
        # asyncio + file lock, but this stops us from queueing many duplicate
        # trigger awaitables when an adv burst fires repeatedly.
        self._fire_lock = asyncio.Lock()
        # Throttle key for adv-decode DEBUG lines: only log when (status,
        # x, y) differs from the last adv we logged (RSSI fluctuates on
        # every packet and is just noise). Avoids drowning DEBUG during
        # the device's ~10-Hz post-measurement burst.
        self._last_logged_adv_key: Optional[tuple[int, Optional[int], Optional[int]]] = None
        # Set on the FIRST adv we receive from the target MAC, used by
        # the startup-full-sync coroutine to align its omblepy invocation
        # with a moment when the device is demonstrably advertising
        # (i.e. not in a deep-sleep gap that would time out the connect).
        self._first_adv_event = asyncio.Event()

    def re_arm(self) -> None:
        """Called by the worker after a failed sync so we can re-trigger
        without waiting for the device to clear the flag."""
        self._debouncer.force_re_arm()

    async def wait_for_first_adv(self) -> None:
        """Block until at least one advertisement from the target MAC
        has been received. Useful for the startup-full-sync coroutine
        to know the device is currently reachable before invoking
        omblepy.
        """
        await self._first_adv_event.wait()

    def _on_adv(self, device: BLEDevice, adv: AdvertisementData) -> None:
        if device.address.upper() != self._mac_target:
            return
        if not self._first_adv_event.is_set():
            logger.info("first adv from target received")
            self._first_adv_event.set()
        mfr = adv.manufacturer_data.get(self._config.manufacturer_id)
        if mfr is None:
            return
        decoded = decode_manufacturer_data(mfr)
        if decoded is None:
            return

        adv_key = (decoded.status_byte, decoded.counter_x, decoded.counter_y)
        if self._last_logged_adv_key != adv_key:
            logger.debug(
                "adv from %s rssi=%s status=0x%02x x=%s y=%s",
                device.address,
                adv.rssi,
                decoded.status_byte,
                decoded.counter_x,
                decoded.counter_y,
            )
            self._last_logged_adv_key = adv_key

        had_new_data = bool(decoded.status_byte & self._config.new_data_flag_mask)
        if not had_new_data and self._on_confirmation is not None:
            asyncio.create_task(self._on_confirmation())

        if not self._debouncer.observe(decoded.status_byte):
            return

        logger.info(
            "new-data debounce reached (%d consecutive); triggering sync "
            "(status=0x%02x x=%s y=%s)",
            self._debouncer.consecutive,
            decoded.status_byte,
            decoded.counter_x,
            decoded.counter_y,
        )
        asyncio.create_task(self._dispatch(decoded))

    async def _dispatch(self, evt: TriggerEvent) -> None:
        # Coalesce concurrent triggers; if a sync is already in flight,
        # callers can still queue, but they'll observe a fresh latch via
        # the worker's own dedup.
        async with self._fire_lock:
            try:
                await self._on_trigger(evt)
            except Exception:  # noqa: BLE001 -- we never want to crash the loop
                logger.exception("sync callback raised; loop continues")

    async def run(self, stop_event: asyncio.Event) -> None:
        kwargs: dict = {"detection_callback": self._on_adv}
        if self._config.adapter:
            kwargs["adapter"] = self._config.adapter
        # Active scanning gives us full mfr-data on most BlueZ adapters.
        # Passive would be lower-power but on Linux often returns no
        # mfr-data, defeating the purpose.
        kwargs["scanning_mode"] = "active"

        logger.info(
            "starting BLE scanner for %s (adapter=%s, mfr_id=0x%04x)",
            self._mac_target,
            self._config.adapter or "default",
            self._config.manufacturer_id,
        )
        self._scanner = BleakScanner(**kwargs)
        await self._start_with_retry(stop_event)
        if stop_event.is_set():
            # Shutdown signalled while we were still trying to start.
            self._scanner = None
            return
        try:
            await stop_event.wait()
        finally:
            logger.info("stopping BLE scanner")
            try:
                await self._scanner.stop()
            except Exception:  # noqa: BLE001
                logger.exception("scanner stop raised")
            self._scanner = None

    async def _start_with_retry(self, stop_event: asyncio.Event) -> None:
        """Call ``self._scanner.start()`` with retries on the BlueZ
        "InProgress" race (see comment on ``_SCAN_START_RETRY_DELAYS_S``).
        All other exceptions propagate immediately. Shutdown signals
        interrupt the backoff so we don't pointlessly wait out the full
        retry window when systemd is trying to stop us.
        """
        for attempt, delay in enumerate(_SCAN_START_RETRY_DELAYS_S, start=1):
            try:
                await self._scanner.start()
                if attempt > 1:
                    logger.info(
                        "BLE scanner started on attempt %d", attempt
                    )
                return
            except BleakDBusError as e:
                err_name = getattr(e, "dbus_error", None)
                if err_name != _DBUS_ERR_IN_PROGRESS:
                    raise
                logger.warning(
                    "BlueZ reports a scan already in progress "
                    "(attempt %d/%d); retrying in %.0fs. Usually means "
                    "BlueZ hasn't yet released the previous instance's "
                    "scan registration.",
                    attempt,
                    len(_SCAN_START_RETRY_DELAYS_S),
                    delay,
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                else:
                    logger.info(
                        "shutdown signalled during scanner start retry"
                    )
                    return
        # Out of retries — one last attempt; let whatever error escapes.
        await self._scanner.start()
