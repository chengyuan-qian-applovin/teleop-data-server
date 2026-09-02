# Teleop Data Server

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
git clone <this repo> teleop-data-server && cd teleop-data-server
$EDITOR fleet.env                # create it — all settings, sample below
./start.sh                       # foreground run (creates .venv on first use)
./start.sh service               # or: install + start the systemd service (sudo)
./stop.sh                        # stop the service and any foreground runs
./stop.sh --disable              # ...and don't start the service at boot anymore
```

Every setting lives in `fleet.env` (gitignored — it holds the token; keep it
`chmod 600`). The installed service reads that same file, so changing any
setting later is: edit `fleet.env`, then `sudo systemctl restart duo-fleet`.
A complete `fleet.env` (simple `KEY=value` lines — both bash and systemd read
it, so no quotes or spaces):

```bash
FLEET_HOST=0.0.0.0
FLEET_PORT=8080
FLEET_DATA_DIR=/absolute/path/to/fleet_data   # fleet.sqlite3 + scenes/ + episodes/
FLEET_TOKEN=change-me                         # X-Fleet-Token; empty = no auth (LAN only!)
FLEET_DEFAULT_TARGET=20                       # for scenes created without a target
FLEET_SYNC_DEST=                              # gs://bucket/prefix/; empty = no backup
FLEET_SYNC_INTERVAL_S=300
FLEET_GCLOUD=gcloud                           # path to gcloud if not on PATH
```

Or run uvicorn by hand:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export FLEET_DATA_DIR=$PWD/fleet_data     # db + scene/episode files (default ./fleet_data)
export FLEET_TOKEN=change-me              # shared secret; unset = no auth (LAN only!)
uvicorn fleet_server.app:app --host 0.0.0.0 --port 8099 --workers 1
```

**`--workers 1` is required.** All coordination state is serialized inside one
process (one SQLite database behind one lock); that is what makes concurrent
collectors race-free. Multiple workers would each open their own state. One
worker is far more than enough — the request rate is a few per minute per
collector.

Then open `http://<server>:8099/` for the dashboard and give collectors the
URL + token.

### Data layout (`FLEET_DATA_DIR`)

```
fleet_data/
  fleet.sqlite3        # scenes / workers / episodes / collectors tables
  scenes/<scene_id>    # self-contained scene packages, scene_id = file basename
  scenes/*.json        # loose JSON documents (e.g. scene_instruct.json),
                       # kept next to the scenes they describe
  episodes/<uuid>.hdf5 # one file per labeled trajectory
```

**File organization convention.** Every scene is one **self-contained
`.usdz` package**: composition flattened into a single layer, all geometry
and textures inside the package, no references to any other file. (The one
exception is `OmniPBR.mdl`, an NVIDIA material shipped with Omniverse/Isaac
that resolves from the runtime's own search paths.) A collector therefore
needs exactly one download per scene — there is no separate asset tree, on
the server or on collectors. JSON documents that describe the scene set
(task metadata etc.) live in `scenes/` next to the scene files; `/api/docs`
serves them from there and the scenes backup rsync carries them.

Scene sets from the generator are *not* self-contained — they reference a
mesh/texture tree (`02_mesh/...`), and some of those assets are themselves
wrappers pulling in further files (`configuration/*.usd`, texture PNGs).
Before seeding a new set, convert it with `usd-core`: fetch the full
dependency closure (`pxr.UsdUtils.ComputeAllDependencies`, iterating until
nothing but runtime `.mdl` refs is unresolved), flatten each scene
(`UsdStage.Flatten`), package it (`UsdUtils.CreateNewUsdzPackage` on the
flattened layer), and verify each package composes the same mesh/point
counts as its source scene (traverse with instance proxies — articulated
assets hide their meshes in instances). A scene whose asset geometry is
missing upstream composes to zero meshes and must be excluded.

Back up by copying the whole directory (stop the server, or use
`sqlite3 fleet.sqlite3 ".backup ..."` for the db while running). Episode files
are immutable once committed; syncing `episodes/` to training storage is safe
at any time.

Or let the server do it: set `FLEET_SYNC_DEST=gs://bucket/prefix/` and it
pushes the data dir there every `FLEET_SYNC_INTERVAL_S` (default 300) seconds —
the db as a consistent snapshot (SQLite backup API, taken under the process
lock), `scenes/` and `episodes/` via `gcloud storage rsync`
(in-flight `.part-*` uploads excluded, nothing ever deleted at the
destination). Needs a `gcloud`
authenticated for the bucket in the service user's account; point
`FLEET_GCLOUD` at the binary if it is not on PATH.

### Seeding scenes

Two ways to load the scene set and per-scene targets:

```bash
# On the server machine (scenes already copied/rsync'd there):
python -m fleet_server.seed --data-dir $FLEET_DATA_DIR --scene-dir ~/scenes --target 20
python -m fleet_server.seed --data-dir $FLEET_DATA_DIR --scene-list ~/scenes/scene_list.json

# Over HTTP from a collector machine (IsaacLab checkout):
./isaaclab.sh -p scripts/environments/teleoperation/sharpa_duo/fleet_push_scenes.py \
    --fleet_server http://<server>:8099 --fleet_token change-me \
    --scene_list scripts/environments/teleoperation/sharpa_duo/scenes/scene_list.json --target 20
```

Re-seeding an existing scene replaces its file (new sha256) and keeps its
episode history. Adjust one scene later with:

```bash
curl -X PATCH -H "X-Fleet-Token: $FLEET_TOKEN" -H "Content-Type: application/json" \
    -d '{"target_successes": 40, "priority": 5}' http://<server>:8099/api/scenes/<scene_id>
```

(`{"retired": true}` hides a scene from suggestions without deleting its data.)

### Running as a service (systemd)

`./start.sh service` generates and installs this unit (as `duo-fleet`),
pointing `EnvironmentFile` at the repo's `fleet.env`:

```ini
# /etc/systemd/system/duo-fleet.service
[Unit]
Description=Teleop Data Server
After=network.target

[Service]
User=<you>
WorkingDirectory=<repo>
EnvironmentFile=<repo>/fleet.env
ExecStart=<repo>/.venv/bin/uvicorn fleet_server.app:app --host ${FLEET_HOST} --port ${FLEET_PORT} --workers 1
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## API

All `/api/*` endpoints except `/api/health` require `X-Fleet-Token` when
`FLEET_TOKEN` is set. Bodies and responses are JSON.

| Method & path | Purpose |
|---|---|
| `GET /` | Live HTML dashboard (no token, read-only, refreshes every 5 s). Each scene row expands to show where its scene file lives on the server's disk. |
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
| `GET /api/docs` | The loose JSON documents in `scenes/` (e.g. `scene_instruct.json`). |
| `GET /api/docs/{name}` | Download one of those documents. |
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
