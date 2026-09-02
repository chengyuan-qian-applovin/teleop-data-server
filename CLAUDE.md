# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-process FastAPI + SQLite server coordinating multi-headset teleop data collection: scenes to record, live collector presence, idempotent episode intake, and periodic backup to GCS. One instance runs per fleet; collectors (IsaacLab `sharpa_duo` scripts) talk to it over HTTP with a shared token.

## Commands

All runtime settings live in `fleet.env` (gitignored — holds the token; sample in README Setup). Both `start.sh` and the systemd unit read it; changing a setting later is: edit `fleet.env`, then `sudo systemctl restart duo-fleet`.

```bash
./start.sh                # foreground run (creates .venv + installs deps on first use)
./start.sh service        # generate + install + start the duo-fleet systemd unit (sudo)
./stop.sh                 # stop the service and any manual runs; --disable also removes from boot
journalctl -u duo-fleet   # service logs; sync results appear as "[sync] ok/failed" lines

# Load scenes into the db (also updates sha/size for existing scene_ids, keeps history):
.venv/bin/python -m fleet_server.seed --data-dir <FLEET_DATA_DIR> --scene-dir <dir> [--target N]
```

There is no test suite or linter. Verify changes by running a second instance against the real data dir on a spare port and hitting the API:

```bash
FLEET_DATA_DIR=... FLEET_TOKEN=test .venv/bin/uvicorn fleet_server.app:app \
    --host 127.0.0.1 --port 8114 --workers 1
curl -H "X-Fleet-Token: test" http://127.0.0.1:8114/api/status
```

Reading the shared SQLite from another process while a server runs is safe (WAL mode); so is running the seed script against a live server's data dir.

## Architecture

Three modules under `fleet_server/`:

- `db.py` — the state store and the concurrency model. One SQLite connection behind one process-wide lock; every public method is one atomic critical section. `app.py` — all HTTP endpoints plus the server-rendered dashboard at `/`. `sync.py` — background task (started via FastAPI lifespan when `FLEET_SYNC_DEST` is set) that pushes the data dir to GCS every `FLEET_SYNC_INTERVAL_S`.

Invariants the design depends on — do not break these:

- **Exactly one uvicorn worker** (`--workers 1`). All coordination correctness comes from serializing state in one process; a second worker means a second lock and a second connection.
- **No stored counters.** Progress is `COUNT(*)` over `episodes`, computed inside the critical section — never cached in a column.
- **Episodes are idempotent by `episode_uuid`** (collector-generated; file name and db key). Upload is atomic (temp file + rename), and metadata commit 409s until the file exists. Retries must never be able to duplicate or half-commit.
- **Presence is never a lock.** `workers` rows just describe who is where; stale rows (no heartbeat for `WORKER_TTL_S` = 120s) drop out of view. Nothing may ever reserve a scene.
- **The live db file is never copied raw.** Backups go through `Database.backup_db()` (SQLite backup API under the lock). The sync is add-only: nothing is ever deleted at the GCS destination.

## Data layout convention

Under `FLEET_DATA_DIR`: `fleet.sqlite3`, flat `scenes/<scene_id>` (scene_id = file basename), and `episodes/<uuid>.hdf5`. Every scene is a **self-contained `.usdz` package** — flattened composition, geometry and textures inside, no external references except the runtime-resolved `OmniPBR.mdl`. There is no asset tree anywhere. Loose `*.json` docs describing the scene set (e.g. `scene_instruct.json`) live in `scenes/` next to the scene files — `/api/docs` serves them from there and the scenes rsync backs them up; the seeder ignores them (it only takes `.usda`/`.usd`/`.usdz`).

Scene sets from the generator are not self-contained; the README's "File organization convention" section documents the usd-core conversion pipeline (dependency closure → flatten → usdz → verify mesh/point counts, traversing with instance proxies). Scenes whose upstream asset geometry is missing compose to zero meshes — exclude them.

## When changing the API

Update the endpoint table in README.md in the same change. The README's design-notes section explains *why* the concurrency model is race-free — read it before touching `db.py`.
