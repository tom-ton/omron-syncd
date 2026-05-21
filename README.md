# omron-syncd

Always-on background service for a Raspberry Pi that watches an Omron
HEM-7380T1-EBK blood-pressure monitor over BLE and pulls fresh
measurements without any user interaction. Replaces the OMRON Connect
Android app for "I just want my BP readings on the Pi" use cases.

This package is intentionally thin: it scans BLE advertisements, decodes
Omron's manufacturer-data "new data available" flag, debounces, and
shells out to a forked [`omblepy`](https://github.com/tom-ton/omblepy/tree/feature/hem-7380t1-ebk)
to do the actual sync. All BLE protocol logic lives in `omblepy`.

> Architectural rationale, BLE protocol notes, and the "why two
> consecutive 0x40 reports?" debounce are documented in `DAEMON_BRIEF.md`
> and `ANDROID_ADV_ANALYSIS.md` inside the omblepy repo.

## Status

- v1: scanner + debouncer + subprocess sync worker, file-locked, with
  exponential-ish failure backoff and an opportunistic daily time-sync
  piggy-back.
- Optional [Google Sheets sync](#google-sheets-sync-optional) for
  pushing each user's CSV to a per-user spreadsheet.
- Tested device: Omron HEM-7380T1-EBK (M7 Intelli IT AFib, EU SKU).
- Other Omron devices supported by `omblepy` likely also work; just set
  `OMRON_SYNCD_DRIVER` to the right driver name and double-check that
  the manufacturer-data flag mask still matches.

## Architecture

```
┌──────────────┐     adv reports      ┌─────────────────┐
│  BleakScanner│ ───────────────────► │  AdvDebouncer   │
└──────────────┘   (mfr-data byte 1)  └────────┬────────┘
                                               │ trigger (after N
                                               │ consecutive 0x40s)
                                       ┌───────▼──────────────────┐
                                       │      SyncWorker          │
                                       │   (asyncio + flock)      │
                                       └───────┬──────────────────┘
                                               │ subprocess
                                       ┌───────▼──────────────────┐
                                       │ omblepy.py -n --noBackup │
                                       │      [ -t once/day ]     │
                                       └───────┬──────────────────┘
                                               │
                                       user{1,2}.csv / .json
                                               │
                                  (after locks release, on OK)
                                               ▼
                                       ┌──────────────────────────┐
                                       │ CsvToGoogleSheetsSyncer  │
                                       │  (per-user, optional)    │
                                       └──────────────────────────┘
```

On the very first start (no CSVs in the output dir) the SyncWorker runs
a one-off **full sync** instead — `omblepy.py` without `-n` and without
`--noBackup` — to seed the on-disk history. See [Startup full sync
(fresh-install bootstrap)](#startup-full-sync-fresh-install-bootstrap)
below.

Once a day (configurable via `OMRON_SYNCD_TIME_SYNC_INTERVAL`, default
86 400 s), the next sync that fires also piggy-backs `-t` to update the
device's RTC. This piggy-back rule means we never connect just to set
the clock — `-t` always rides along on a sync that was going to happen
anyway.

### Startup full sync (fresh-install bootstrap)

On daemon startup the scanner checks the configured `OMRON_SYNCD_OUTPUT_DIR`
for `user1.csv` and `user2.csv`. If **neither** exists (= fresh install,
since omblepy always writes both with at least a header on any successful
sync) the daemon will:

1. **Suspend regular `-n` triggers** so a `0x41` adv burst arriving
   immediately on startup can't race ahead and pull only new records,
   leaving the historical 100 ring-buffer slots stranded on the device.
2. **Wait for the first adv from the target MAC**, which proves the
   device is currently advertising (rather than in a deep-sleep gap
   that would time out the connect attempt).
3. **Run a single full sync** (omblepy without `-n`, optionally with
   `-t` if `OMRON_SYNCD_TIME_SYNC_INTERVAL` has elapsed). This seeds
   the CSVs with the complete on-device history.
4. **Resume normal triggers**, regardless of whether the full sync
   succeeded. If it failed, the next `0x41` burst will retry via the
   normal `-n` path.

This bootstrap runs at most once per daemon process. If you want to
force it again (e.g. after manually deleting CSVs), just stop the
daemon, `rm` the CSVs, and start it again.

### Trigger gating

Per `DAEMON_BRIEF.md` §6 and §7.3 in the omblepy repo, the device sets
bit 6 of the adv status byte when a new measurement is taken and
clears it ~5 s after a successful omblepy disconnect (the EBK driver
now replays the OMRON Android post-sync ack sequence — the early
versions of the daemon worked around a missing-ack bug by tracking
counter X, but that workaround is no longer needed).

Current trigger logic:

* `status & 0x40 == 0`: no trigger, reset debounce.
* `status & 0x40 == 1`: increment consecutive counter; fire once when
  it reaches `OMRON_SYNCD_DEBOUNCE` (default 2). Re-armed on the next
  `0x01` (no-data) adv.

Double-firing inside the ~5 s window between omblepy disconnect and
the device re-advertising the cleared flag is prevented by
`OMRON_SYNCD_MIN_INTERVAL` (default 60 s) at the sync-worker level,
not by debouncer state. Counter X / Y are still decoded and shown in
DEBUG logs for diagnostic purposes but are not used in the trigger
decision.

Failed syncs apply exponential-ish backoff at the sync-worker level
and re-arm the debouncer so the next adv burst can retrigger.

### Google Sheets sync (optional)

After every successful omblepy run, the daemon can append the new rows
of each per-user CSV to a Google Sheets worksheet. This is opt-in via
`OMRON_SYNCD_GSHEETS_ENABLED=true`; if unset (the default) the daemon
never instantiates the syncer or talks to any Google API. The
`google-api-python-client` / `google-auth` deps themselves are
unconditional `pyproject.toml` requirements — keeping them out of the
import graph isn't worth the optional-extras complexity for a
single-Pi daemon.

Design points worth knowing:

- **One spreadsheet per user, one service account per user.** Per-user
  config is fully independent so you can share a sheet with each user
  separately without leaking other users' data.
- **Incremental, append-only.** Per-user state files
  (`<output_dir>/google-sync-<user>.json`) record the latest `datetime`
  pushed; subsequent runs append only rows newer than that.
  Append-then-save ordering means a state-save failure after a
  successful append produces duplicate rows on the next run (easy to
  spot/clean) rather than silent data loss.
- **Runs OUTSIDE the BLE locks.** The Google API call happens after
  `omblepy` has exited and both the asyncio loop lock and the file
  flock have been released. A slow HTTPS round-trip therefore cannot
  block the next adv-driven sync or a manual `omblepy.py` invocation.
- **One `GoogleSheetsClient` per user, cached for the daemon's
  lifetime.** Service-account OAuth tokens auto-refresh, so long-lived
  instances are safe and avoid re-parsing the SA JSON on every sync.
- **Per-user failures are isolated.** An exception while syncing
  user1 doesn't stop user2 from being attempted in the same cycle.

### Setting up Google Sheets sync

1. **Create a Google Cloud project** (or reuse one). Enable the
   "Google Sheets API" under APIs & Services.
2. **Create a service account** under IAM & Admin → Service Accounts,
   give it no roles, then under "Keys" generate a JSON key. Save the
   JSON file somewhere only root can read on the Pi, e.g.
   `/etc/omron-syncd/user1-sa.json`. Lock down permissions:

   ```bash
   sudo install -d -m 0700 -o root -g root /etc/omron-syncd
   sudo install -m 0600 -o root -g root user1-sa.json /etc/omron-syncd/
   ```
3. **Create the spreadsheet** in Google Sheets and copy its ID from
   the URL (`https://docs.google.com/spreadsheets/d/<ID>/edit`). The
   first row should be headers: `datetime, dia, sys, bpm, mov, ihb`.
4. **Share the spreadsheet** with the service account's email
   (something like `name@project.iam.gserviceaccount.com`), as
   "Editor". Repeat for each user.
5. **Set the env vars** in the systemd unit (see
   `systemd/omron-syncd.service` for the commented template). Minimal
   example for one user:

   ```
   Environment=OMRON_SYNCD_GSHEETS_ENABLED=true
   Environment=OMRON_SYNCD_GSHEETS_USERS=user1
   Environment=OMRON_SYNCD_GSHEETS_USER1_CSV=user1.csv
   Environment=OMRON_SYNCD_GSHEETS_USER1_SPREADSHEET_ID=1abc...
   Environment=OMRON_SYNCD_GSHEETS_USER1_WORKSHEET=Sheet1
   Environment=OMRON_SYNCD_GSHEETS_USER1_SA_JSON=/etc/omron-syncd/user1-sa.json
   ```
6. **Reload + restart.** `sudo systemctl daemon-reload && sudo
   systemctl restart omron-syncd`. The next successful sync logs
   `Syncing N rows to Google Sheets for user user1` followed by
   `Google Sheets sync completed for user user1`.

### Re-syncing existing rows to Google Sheets

If you ever need to re-push everything (sheet was wiped, or you
changed worksheet), stop the daemon, delete the state file, and start
again — the next sync will treat every row as "new":

```bash
sudo systemctl stop omron-syncd
sudo rm /var/lib/omron-bp/google-sync-user1.json
sudo systemctl start omron-syncd
```

## Install (Raspberry Pi)

Tested on Raspberry Pi OS Bookworm (Linux 5.15+, BlueZ 5.66+, Python 3.11).

### 1. Install BlueZ + system deps

```bash
sudo apt update
sudo apt install -y bluez python3-venv git
```

### 2. Clone omblepy (the patched fork)

```bash
sudo mkdir -p /opt
sudo git clone -b feature/hem-7380t1-ebk \
    https://github.com/tom-ton/omblepy.git /opt/omblepy
```

### 3. Clone and install omron-syncd

```bash
sudo git clone https://github.com/tom-ton/omron-syncd.git /opt/omron-syncd
sudo python3 -m venv /opt/omron-syncd/.venv
sudo /opt/omron-syncd/.venv/bin/pip install \
    -r /opt/omblepy/requirements.txt \
    -e /opt/omron-syncd
```

### 4. Pair the BP monitor (one-off)

The OS-level BlueZ bond is **not** created by the daemon; you must do
this once before enabling the service.

```bash
cd /opt/omblepy
# put the device in pairing mode (long-press connect button until
# the BLE-pair icon shows)
sudo /opt/omron-syncd/.venv/bin/python3 ./omblepy.py \
    -p -d HEM-7380T1-EBK -m F1:95:48:45:81:6B
```

You should see the omblepy log report a successful pair-finalization
and exit. Verify the bond exists:

```bash
sudo ls /var/lib/bluetooth/*/F1:95:48:45:81:6B/
# should contain at least: info, attributes
```

### 5. Verify a manual sync works

Take a measurement on the device, then:

```bash
sudo mkdir -p /var/lib/omron-bp
cd /var/lib/omron-bp
sudo /opt/omron-syncd/.venv/bin/python3 /opt/omblepy/omblepy.py \
    -d HEM-7380T1-EBK -m F1:95:48:45:81:6B -n
```

A `user1.csv` should appear (or `user2.csv` depending on which user
slot the measurement landed in).

### 6. Install and enable the systemd unit

Edit `systemd/omron-syncd.service` to match your MAC if it differs from
the default, then:

```bash
sudo cp /opt/omron-syncd/systemd/omron-syncd.service \
    /etc/systemd/system/omron-syncd.service
sudo systemctl daemon-reload
sudo systemctl enable --now omron-syncd.service
sudo journalctl -u omron-syncd.service -f
```

You should see the scanner come up and start emitting advertisement
debug lines (with `OMRON_SYNCD_LOG_LEVEL=DEBUG`) or stay quiet at INFO
until you take a measurement.

## Configuration

All knobs are environment variables, set in the systemd unit
(`Environment=...`). Anything not listed has the default shown.

| Variable | Default | Purpose |
|----------|---------|---------|
| `OMRON_SYNCD_MAC` | _(required)_ | BLE MAC of the BP monitor. |
| `OMRON_SYNCD_DRIVER` | `HEM-7380T1-EBK` | omblepy `-d` device name. |
| `OMRON_SYNCD_OMBLEPY_DIR` | `/opt/omblepy` | Where `omblepy.py` lives. |
| `OMRON_SYNCD_PYTHON` | `python3` | Interpreter for the omblepy subprocess. |
| `OMRON_SYNCD_OUTPUT_DIR` | `~/.local/share/omron-bp` | CWD for omblepy; CSVs land here. |
| `OMRON_SYNCD_LOCK` | `~/.cache/omron-syncd.lock` | flock to serialise daemon vs manual omblepy. |
| `OMRON_SYNCD_ADAPTER` | _(empty)_ | BlueZ adapter name, e.g. `hci0`. |
| `OMRON_SYNCD_MFR_ID` | `526` (`0x020E`) | Bluetooth SIG company id used by Omron. |
| `OMRON_SYNCD_NEW_FLAG` | `64` (`0x40`) | Bit in mfr-data\[1\] meaning "new data". |
| `OMRON_SYNCD_DEBOUNCE` | `2` | Consecutive 0x40 reports before connecting. |
| `OMRON_SYNCD_MIN_INTERVAL` | `60` | Min seconds between successful syncs. |
| `OMRON_SYNCD_BACKOFF` | `5,30,120,300,900` | Backoff schedule per fail-streak index. |
| `OMRON_SYNCD_TIME_SYNC_INTERVAL` | `86400` | How often to piggy-back `-t` (0 = never). |
| `OMRON_SYNCD_SYNC_TIMEOUT` | `180` | Hard timeout for one omblepy invocation. |
| `OMRON_SYNCD_LOG_LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR. |
| `OMRON_SYNCD_LIB_LOG_LEVEL` | `WARNING` | Level clamp for noisy library loggers (bleak, dbus_fast, asyncio). Bump to DEBUG only when debugging bleak itself. |

### Google Sheets sync (only read if `OMRON_SYNCD_GSHEETS_ENABLED=true`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `OMRON_SYNCD_GSHEETS_ENABLED` | `false` | Master switch for the Google Sheets uploader. |
| `OMRON_SYNCD_GSHEETS_USERS` | _(empty)_ | Comma-separated list of user names that map to the per-user blocks below, e.g. `user1,user2`. |
| `OMRON_SYNCD_GSHEETS_<USER>_CSV` | _(required)_ | CSV filename relative to `OMRON_SYNCD_OUTPUT_DIR`, e.g. `user1.csv`. |
| `OMRON_SYNCD_GSHEETS_<USER>_SPREADSHEET_ID` | _(required)_ | The `<ID>` from `docs.google.com/spreadsheets/d/<ID>/edit`. |
| `OMRON_SYNCD_GSHEETS_<USER>_WORKSHEET` | `Sheet1` | Worksheet (tab) name inside the spreadsheet. |
| `OMRON_SYNCD_GSHEETS_<USER>_SA_JSON` | _(required)_ | Absolute path to the service-account JSON key with edit access to the spreadsheet. |

State files (`<output_dir>/google-sync-<user>.json`) are managed by
the daemon — don't hand-edit unless you want to force re-upload (see
[Re-syncing existing rows to Google Sheets](#re-syncing-existing-rows-to-google-sheets)).

## Failure modes

| Symptom in log | Likely cause | What to do |
|----------------|--------------|------------|
| `omblepy: ... PIN or Key Missing` | Device-side bond was wiped (e.g. user did "Reset Device"). | Re-run step 4 (`omblepy.py -p`). |
| `omblepy: ... no response to 0x11 unlock` | Rare BlueZ encryption glitch. | Daemon retries automatically; if it persists, restart `bluetooth.service`. |
| `lock ... held by another process` | A manual `omblepy.py` is running. | Expected; daemon will pick up on next adv burst. |
| `omblepy timed out after 180s` | BLE link stalled mid-transfer. | Daemon backs off and retries. Investigate adapter health if frequent. |
| `omblepy: ... endTransmission status: 0xe5` | Should not happen on the current EBK driver — indicates a regression in the settings-write payload size. | Investigate the driver; do not ignore. |
| `BleakDBusError: [org.bluez.Error.InProgress] Operation already in progress` at scanner start | BlueZ discovery state machine wedged from a previous interrupted operation (omblepy connect aborted, daemon SIGKILLed, etc.). The systemd unit's `ExecStartPre=hciconfig hci0 down/up` lines normally clear this automatically before every start; if it persists, bluetoothd itself is stuck. | `sudo systemctl restart bluetooth`, then `sudo systemctl restart omron-syncd`. |
| `Google Sheets sync failed for user ...` | API outage, permissions changed, or sheet/worksheet renamed/deleted. | Daemon logs the exception traceback; the next successful omblepy sync will retry. State file is only advanced on a successful append, so no rows are lost. |

## Repository layout

```
omron-syncd/
├── README.md
├── pyproject.toml
├── omron_syncd/
│   ├── __init__.py
│   ├── __main__.py     – `python -m omron_syncd` entry
│   ├── config.py       – env-driven dataclass
│   ├── scanner.py      – BleakScanner + adv decoder + debouncer
│   ├── sync_worker.py  – subprocess + flock + backoff
│   ├── daemon.py       – wires it all together, signal handling
│   └── google_sync/    – optional CSV → Google Sheets uploader
│       ├── __init__.py
│       ├── config.py   – env-driven per-user dataclasses
│       ├── client.py   – thin googleapiclient wrapper
│       ├── state.py    – per-user "last datetime synced" JSON store
│       └── syncer.py   – CSV diff + append + state update
└── systemd/
    └── omron-syncd.service
```

## Development notes

- The daemon and `omblepy` are intentionally split via subprocess (not
  `import`) so that omblepy can be upgraded independently and so a
  crash inside omblepy can never take down the scanner.
- The startup overhead of one `python3 ./omblepy.py` invocation is
  ~250 ms on a Pi 4; that's negligible compared to the actual BLE
  transfer (~2–5 s for `-n`, ~30 s for full sync). Don't optimise.
- `_run_omblepy` shells out with `cwd=output_dir` because omblepy's CSV
  paths are hard-coded relative to cwd. Patching that upstream would
  let us drop this hack.
- Incremental (`-n`) syncs pass `--noBackup` because the daemon fires
  one per measurement; the default per-sync timestamped backup CSVs
  would accumulate forever. The one-off startup full sync does NOT
  pass `--noBackup` — it's the rare path that overwrites a possibly-
  non-empty CSV with whatever comes off the device, so a one-shot
  defensive backup before the write has actual value.
- The omblepy subprocess is launched with `PYTHONUNBUFFERED=1` and its
  stdout is read line-by-line as it arrives, so omblepy's own log
  lines stream into journald in real time. Without this, `proc.communicate()`
  blocks until exit and ~30 s of full-sync output appears in one burst,
  making the daemon look hung.
- The scanner uses `scanning_mode="active"` because BlueZ frequently
  drops manufacturer-data on passive scans, which would defeat the
  whole trigger.
- `bleak` and `dbus_fast` are clamped to WARNING by default (see
  `OMRON_SYNCD_LIB_LOG_LEVEL`) because at DEBUG they emit one D-Bus
  signal log per BLE adv from any device the adapter sees, which
  drowns our own debug output.
- The systemd unit unconditionally bounces `hci0` (down/sleep/up) in
  `ExecStartPre` before launching the daemon. This costs ~3 s but
  makes startup bulletproof against the BlueZ "discovery wedged"
  state that follows an interrupted omblepy connect or a SIGKILLed
  previous instance (see the failure-modes table for the specific
  error). The in-process retry in `scanner._start_with_retry` is a
  belt-and-braces complement that handles the much narrower DBus
  disconnect race when systemd restarts the unit faster than BlueZ
  can release the previous connection.
- The Google Sheets push is awaited (not fire-and-forget) but happens
  outside both BLE locks. Awaiting gives natural backpressure: if the
  Sheets API is slow or failing, you get one queued upload per sync
  rather than an unbounded fan-out of background tasks. The daemon
  isn't slowed by Sheets latency in any meaningful way because the
  scanner keeps running and the BLE locks are already released.
- `sync_worker.py` imports the `CsvToGoogleSheetsSyncer` type only
  under `TYPE_CHECKING`; at runtime it just holds an opaque optional
  object passed in by the daemon. The intent isn't to skip the
  `googleapiclient` import (it's a hard dep — see above) but to keep
  the worker module decoupled and importable in isolation. The
  syncer is only constructed when `OMRON_SYNCD_GSHEETS_ENABLED=true`,
  so an API misconfiguration on a disabled feature can never crash
  the daemon at startup.
