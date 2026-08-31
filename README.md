# Duo Fleet Server

Centralized coordination for multi-headset teleop data collection (the
`sharpa_duo` scripts in IsaacLab). One small HTTP server is the single source
of truth for:

- **Scenes** — the USDA files to collect, each with a target number of
  successful episodes ("quota", soft: extra episodes are always accepted).
- **Presence** — which collectors are working on which scenes *right now*
  (informational, never exclusive: any number of collectors may share a scene).
- **Episodes** — one row + one HDF5 file per labeled trajectory, keyed by a
  collector-generated UUID so retries are idempotent.

Collectors check in at startup, download the scenes they will work on, and
report every trajectory the moment it is voice-labeled. A dashboard at `/`
shows live progress.

## Setup

Needs Python ≥ 3.10. On the machine that will host the server:

```bash
git clone <this repo> duo-fleet-server && cd duo-fleet-server
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export FLEET_DATA_DIR=$PWD/fleet_data     # db + scene/episode files (default ./fleet_data)
export FLEET_TOKEN=change-me              # shared secret; unset = no auth (LAN only!)
uvicorn fleet_server.app:app --host 0.0.0.0 --port 8080 --workers 1
```

**`--workers 1` is required.** All coordination state is serialized inside one
process (one SQLite database behind one lock); that is what makes concurrent
collectors race-free. Multiple workers would each open their own state. One
worker is far more than enough — the request rate is a few per minute per
collector.

Then open `http://<server>:8080/` for the dashboard and give collectors the
URL + token.

### Data layout (`FLEET_DATA_DIR`)

```
fleet_data/
  fleet.sqlite3        # scenes / workers / episodes / collectors tables
  scenes/<scene_id>    # the scene USDA files, scene_id = file basename
  episodes/<uuid>.hdf5 # one file per labeled trajectory
```

Back up by copying the whole directory (stop the server, or use
`sqlite3 fleet.sqlite3 ".backup ..."` for the db while running). Episode files
are immutable once committed; syncing `episodes/` to training storage is safe
at any time.

### Seeding scenes

Two ways to load the scene set and per-scene targets:

```bash
# On the server machine (scenes already copied/rsync'd there):
python -m fleet_server.seed --data-dir $FLEET_DATA_DIR --scene-dir ~/scenes --target 20
python -m fleet_server.seed --data-dir $FLEET_DATA_DIR --scene-list ~/scenes/scene_list.json

# Over HTTP from a collector machine (IsaacLab checkout):
./isaaclab.sh -p scripts/environments/teleoperation/sharpa_duo/fleet_push_scenes.py \
    --fleet_server http://<server>:8080 --fleet_token change-me \
    --scene_list scripts/environments/teleoperation/sharpa_duo/scenes/scene_list.json --target 20
```

Re-seeding an existing scene replaces its file (new sha256) and keeps its
episode history. Adjust one scene later with:

```bash
curl -X PATCH -H "X-Fleet-Token: $FLEET_TOKEN" -H "Content-Type: application/json" \
    -d '{"target_successes": 40, "priority": 5}' http://<server>:8080/api/scenes/<scene_id>
```

(`{"retired": true}` hides a scene from suggestions without deleting its data.)

### Running as a service (systemd)

```ini
# /etc/systemd/system/duo-fleet.service
[Unit]
Description=Duo fleet server
After=network.target

[Service]
User=fleet
WorkingDirectory=/opt/duo-fleet-server
Environment=FLEET_DATA_DIR=/opt/duo-fleet-server/fleet_data
Environment=FLEET_TOKEN=change-me
ExecStart=/opt/duo-fleet-server/.venv/bin/uvicorn fleet_server.app:app --host 0.0.0.0 --port 8080 --workers 1
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## API

All `/api/*` endpoints except `/api/health` require `X-Fleet-Token` when
`FLEET_TOKEN` is set. Bodies and responses are JSON.

| Method & path | Purpose |
|---|---|
| `GET /` | Live HTML dashboard (no token, read-only, refreshes every 5 s). |
| `GET /api/health` | Liveness probe. |
| `POST /api/checkin` `{collector_id}` | Startup sync: registers the collector, clears its stale presence, returns the full status snapshot. |
| `POST /api/heartbeat` `{collector_id, scene_id?}` | Keeps the collector (and its scene presence) marked live; presence goes stale after 120 s of silence. |
| `GET /api/status` | The status snapshot (scenes with progress + live workers, collectors). |
| `GET /api/suggest?n=&collector_id=` | Up to `n` under-target scenes, best first (priority, then fewest active workers, then most remaining). Advice only — nothing is reserved. |
| `POST /api/workers` `{collector_id, scene_id}` | Declare "I am working on this scene"; returns the scene row (uri-less: fetch the file separately). |
| `DELETE /api/workers?collector_id=&scene_id=` | Withdraw presence (all scenes when `scene_id` omitted). |
| `GET /api/scenes` | All scene rows with progress. |
| `PUT /api/scenes/{id}/file?target_successes=&priority=&task_description=` | Upload/replace a scene file (raw body) and upsert its row. |
| `GET /api/scenes/{id}/file` | Download a scene file. |
| `PATCH /api/scenes/{id}` | Change `target_successes` / `priority` / `task_description` / `retired`. |
| `PUT /api/episodes/{uuid}/file` | Upload one trajectory HDF5 (raw body; atomic, idempotent). |
| `POST /api/episodes` `{episode_uuid, scene_id, collector_id, success, ...}` | Commit the episode's metadata. 409 until its file is uploaded; UPSERT by uuid, so retries are safe. Returns the scene's progress. |
| `GET /api/episodes?scene_id=&limit=` | Recent episode rows. |
| `GET /api/episodes/{uuid}/file` | Download one trajectory. |

## Design notes (why this is race-free)

- **Single writer.** One process, one SQLite connection, one lock: every API
  call is one atomic critical section. Two collectors reporting the last
  episode of a scene simultaneously both commit; the counts stay exact.
- **No stored counters.** Progress is always `COUNT(*)` over `episodes`,
  computed inside the same critical section — there is no counter to lose an
  update on.
- **Idempotent episodes.** The collector generates `episode_uuid` at label
  time; both the file name and the db row key on it. A retry after a network
  timeout overwrites itself, never duplicates.
- **File before metadata.** The HDF5 is uploaded (atomically: temp file +
  rename) before the metadata commit; a half-done upload is invisible and a
  metadata-first retry gets a 409 telling the client to redo the upload.
- **Presence, not locks.** `workers` rows describe who is collecting where;
  nothing is reserved, so a crashed collector can never strand a scene. Stale
  rows (no heartbeat for 120 s) just drop out of the display and ranking.
- **Soft quotas.** Episodes past a scene's target are stored normally — with
  concurrent collectors on one scene the last episodes can race past the
  target, and that is surplus data, not an error.
