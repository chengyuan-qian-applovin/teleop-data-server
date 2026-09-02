"""The fleet coordination HTTP API and status dashboard.

Run with exactly one worker process (the state store serializes in-process):

    uvicorn fleet_server.app:app --host 0.0.0.0 --port 8099 --workers 1

Environment:

- ``FLEET_DATA_DIR``: where the SQLite db, scene files, and episode files live
  (default ``./fleet_data``).
- ``FLEET_TOKEN``: shared secret; when set, every ``/api/*`` request except
  ``/api/health`` must carry it in the ``X-Fleet-Token`` header. The dashboard
  at ``/`` stays open (read-only, server-rendered).
- ``FLEET_DEFAULT_TARGET``: target successes for scenes created without an
  explicit target (default 20).
- ``FLEET_SYNC_DEST``: a ``gs://bucket/prefix`` to back the data dir up to
  periodically (unset = no syncing). See ``sync.py``.
- ``FLEET_SYNC_INTERVAL_S``: seconds between syncs (default 300).
- ``FLEET_GCLOUD``: the ``gcloud`` binary to sync with (default: from PATH).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import html
import json
import os
import re
import time

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

from .db import WORKER_TTL_S, Database
from .sync import Syncer

DATA_DIR = os.environ.get("FLEET_DATA_DIR", "./fleet_data")
TOKEN = os.environ.get("FLEET_TOKEN", "")
DEFAULT_TARGET = int(os.environ.get("FLEET_DEFAULT_TARGET", "20"))
SYNC_DEST = os.environ.get("FLEET_SYNC_DEST", "")
SYNC_INTERVAL_S = float(os.environ.get("FLEET_SYNC_INTERVAL_S", "300"))
GCLOUD = os.environ.get("FLEET_GCLOUD", "gcloud")

db = Database(DATA_DIR, default_target=DEFAULT_TARGET)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    sync_task = None
    if SYNC_DEST:
        sync_task = asyncio.create_task(Syncer(db, SYNC_DEST, SYNC_INTERVAL_S, GCLOUD).run_forever())
    yield
    if sync_task:
        sync_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sync_task


app = FastAPI(title="Teleop Data Server", version="1.0", lifespan=lifespan)

_UUID_RE = re.compile(r"^[0-9a-f]{32}$")
_SCENE_ID_RE = re.compile(r"^[A-Za-z0-9._\-]{1,255}$")


def require_token(request: Request) -> None:
    if TOKEN and request.headers.get("x-fleet-token") != TOKEN:
        raise HTTPException(status_code=401, detail="missing or wrong X-Fleet-Token header")


def check_scene_id(scene_id: str) -> str:
    """Scene ids are flat file basenames; reject anything path-like."""
    if not _SCENE_ID_RE.match(scene_id):
        raise HTTPException(status_code=400, detail=f"invalid scene_id {scene_id!r}")
    return scene_id


def check_uuid(episode_uuid: str) -> str:
    if not _UUID_RE.match(episode_uuid):
        raise HTTPException(status_code=400, detail="episode_uuid must be 32 lowercase hex chars (uuid4().hex)")
    return episode_uuid


async def stream_to_file(request: Request, final_path: str) -> tuple[str, int]:
    """Stream the request body to ``final_path`` atomically; returns (sha256, size).

    The bytes land in a unique ``.part`` temp file first and are renamed into
    place only when complete, so a concurrent reader (or a crashed upload) can
    never observe a half-written file.
    """
    tmp_path = f"{final_path}.part-{os.getpid()}-{time.monotonic_ns()}"
    digest = hashlib.sha256()
    size = 0
    try:
        with open(tmp_path, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="empty upload")
        os.replace(tmp_path, final_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return digest.hexdigest(), size


# -- collectors ---------------------------------------------------------------------


class CollectorRef(BaseModel):
    collector_id: str
    scene_id: str | None = None


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "server_time": time.time()}


@app.post("/api/checkin", dependencies=[Depends(require_token)])
def checkin(body: CollectorRef) -> dict:
    return db.checkin(body.collector_id)


@app.post("/api/heartbeat", dependencies=[Depends(require_token)])
def heartbeat(body: CollectorRef) -> dict:
    if body.scene_id:
        check_scene_id(body.scene_id)
    return db.heartbeat(body.collector_id, body.scene_id)


@app.get("/api/status", dependencies=[Depends(require_token)])
def status() -> dict:
    return db.snapshot()


@app.get("/api/suggest", dependencies=[Depends(require_token)])
def suggest(n: int = 8, collector_id: str | None = None) -> dict:
    return {"scenes": db.suggest(max(1, min(n, 100)), collector_id)}


@app.post("/api/workers", dependencies=[Depends(require_token)])
def declare_worker(body: CollectorRef) -> dict:
    if not body.scene_id:
        raise HTTPException(status_code=400, detail="scene_id is required")
    check_scene_id(body.scene_id)
    scene = db.declare_worker(body.collector_id, body.scene_id)
    if scene is None:
        raise HTTPException(status_code=404, detail=f"unknown scene {body.scene_id!r}")
    return {"scene": scene}


@app.delete("/api/workers", dependencies=[Depends(require_token)])
def remove_worker(collector_id: str, scene_id: str | None = None) -> dict:
    return db.remove_worker(collector_id, scene_id)


# -- scenes -------------------------------------------------------------------------


@app.get("/api/scenes", dependencies=[Depends(require_token)])
def list_scenes() -> dict:
    return {"scenes": db.snapshot()["scenes"]}


@app.get("/api/scenes/{scene_id}/file", dependencies=[Depends(require_token)])
def download_scene(scene_id: str) -> FileResponse:
    check_scene_id(scene_id)
    path = db.scene_path(scene_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"no file stored for scene {scene_id!r}")
    return FileResponse(path, media_type="application/octet-stream", filename=scene_id)


@app.put("/api/scenes/{scene_id}/file", dependencies=[Depends(require_token)])
async def upload_scene(
    scene_id: str,
    request: Request,
    target_successes: int | None = None,
    priority: int | None = None,
    task_description: str | None = None,
) -> dict:
    check_scene_id(scene_id)
    sha256, size = await stream_to_file(request, db.scene_path(scene_id))
    return {"scene": db.upsert_scene(scene_id, sha256, size, target_successes, priority, task_description)}


class ScenePatch(BaseModel):
    target_successes: int | None = None
    priority: int | None = None
    task_description: str | None = None
    retired: bool | None = None


@app.patch("/api/scenes/{scene_id}", dependencies=[Depends(require_token)])
def patch_scene(scene_id: str, body: ScenePatch) -> dict:
    check_scene_id(scene_id)
    fields = body.model_dump()
    if fields.get("retired") is not None:
        fields["retired"] = int(fields["retired"])
    scene = db.patch_scene(scene_id, fields)
    if scene is None:
        raise HTTPException(status_code=404, detail=f"unknown scene {scene_id!r}")
    return {"scene": scene}


# -- episodes ------------------------------------------------------------------------


@app.put("/api/episodes/{episode_uuid}/file", dependencies=[Depends(require_token)])
async def upload_episode(episode_uuid: str, request: Request) -> dict:
    check_uuid(episode_uuid)
    sha256, size = await stream_to_file(request, db.episode_path(episode_uuid))
    return {"stored": True, "sha256": sha256, "size_bytes": size}


class EpisodeReport(BaseModel):
    episode_uuid: str
    scene_id: str
    collector_id: str
    success: bool
    embodiment: str | None = None
    num_steps: int | None = None
    meta: dict = {}


@app.post("/api/episodes", dependencies=[Depends(require_token)])
def report_episode(body: EpisodeReport) -> dict:
    """Commit one labeled episode's metadata; its file must already be uploaded.

    Enforcing file-before-metadata means a committed episode always has its
    data present; a metadata-only retry after a lost upload gets a 409 and the
    client redoes the (idempotent) upload.
    """
    check_uuid(body.episode_uuid)
    check_scene_id(body.scene_id)
    path = db.episode_path(body.episode_uuid)
    if not os.path.exists(path):
        raise HTTPException(
            status_code=409, detail=f"episode file for {body.episode_uuid} not uploaded yet; PUT its file first"
        )
    progress = db.record_episode(
        episode_uuid=body.episode_uuid,
        scene_id=body.scene_id,
        collector_id=body.collector_id,
        success=body.success,
        embodiment=body.embodiment,
        num_steps=body.num_steps,
        file_size=os.path.getsize(path),
        meta_json=json.dumps(body.meta),
    )
    return {"recorded": True, "progress": progress}


@app.get("/api/episodes", dependencies=[Depends(require_token)])
def list_episodes(scene_id: str | None = None, limit: int = 100) -> dict:
    return {"episodes": db.list_episodes(scene_id, max(1, min(limit, 1000)))}


@app.get("/api/episodes/{episode_uuid}/file", dependencies=[Depends(require_token)])
def download_episode(episode_uuid: str) -> FileResponse:
    check_uuid(episode_uuid)
    path = db.episode_path(episode_uuid)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"no file stored for episode {episode_uuid}")
    return FileResponse(path, media_type="application/octet-stream", filename=f"{episode_uuid}.hdf5")


# -- assets --------------------------------------------------------------------------

# sha256 of asset files, keyed by (rel path, size, mtime_ns) so an edited or
# replaced file re-hashes and an unchanged one never does. Assets are large
# and change rarely; without this every /api/assets call would re-read all
# of them.
_asset_sha_cache: dict[tuple[str, int, int], str] = {}


def _asset_sha256(rel: str, full: str, st: os.stat_result) -> str:
    key = (rel, st.st_size, st.st_mtime_ns)
    sha = _asset_sha_cache.get(key)
    if sha is None:
        digest = hashlib.sha256()
        with open(full, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                digest.update(chunk)
        sha = _asset_sha_cache[key] = digest.hexdigest()
    return sha


def _scene_refs(scene_id: str) -> list[str]:
    """Absolute paths of every file the scene's USDA references, deduped, in file order."""
    try:
        with open(db.scene_path(scene_id), encoding="utf-8", errors="replace") as f:
            refs = re.findall(r"@([^@\n]+)@", f.read())
    except OSError:
        return []
    out: list[str] = []
    seen = set()
    for ref in refs:
        p = os.path.normpath(os.path.join(db.scenes_dir, ref))
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


@app.get("/api/assets", dependencies=[Depends(require_token)])
def list_assets() -> dict:
    """Every asset file (path relative to assets/, size, sha256).

    Clients mirror the tree from this: download whatever is missing locally
    or has a different sha, into the same relative layout — scene files
    reference assets by relative path, so the layout is the contract.
    """
    base = db.assets_dir
    assets = []
    for root, _dirs, files in os.walk(base):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, base).replace(os.sep, "/")
            st = os.stat(full)
            assets.append({"path": rel, "size_bytes": st.st_size, "sha256": _asset_sha256(rel, full, st)})
    return {"assets": sorted(assets, key=lambda a: a["path"])}


@app.get("/api/scenes/{scene_id}/assets", dependencies=[Depends(require_token)])
def scene_assets(scene_id: str) -> dict:
    """The assets one scene needs: exactly what a client must have to load it.

    Each entry's ``path`` plugs straight into ``GET /api/assets/{path}`` and is
    the relative location to store it at (next to a ``scenes/`` dir). Entries
    with ``"missing": true`` are referenced by the scene but absent on the
    server; references outside ``assets/`` are reported the same way.
    """
    check_scene_id(scene_id)
    if not os.path.exists(db.scene_path(scene_id)):
        raise HTTPException(status_code=404, detail=f"no file stored for scene {scene_id!r}")
    base = os.path.realpath(db.assets_dir)
    assets = []
    for p in _scene_refs(scene_id):
        full = os.path.realpath(p)
        rel = os.path.relpath(full, base).replace(os.sep, "/")
        if full.startswith(base + os.sep) and os.path.isfile(full):
            st = os.stat(full)
            assets.append({"path": rel, "size_bytes": st.st_size, "sha256": _asset_sha256(rel, full, st), "missing": False})
        else:
            assets.append({"path": rel, "missing": True})
    return {"scene_id": scene_id, "assets": assets}


@app.get("/api/assets/{asset_path:path}", dependencies=[Depends(require_token)])
def download_asset(asset_path: str) -> FileResponse:
    base = os.path.realpath(db.assets_dir)
    full = os.path.realpath(os.path.join(base, asset_path))
    if not (full.startswith(base + os.sep) and os.path.isfile(full)):
        raise HTTPException(status_code=404, detail=f"no such asset {asset_path!r}")
    return FileResponse(full, media_type="application/octet-stream", filename=os.path.basename(full))


# -- docs ----------------------------------------------------------------------------

# Loose JSON documents kept in scenes/ next to the scene files (e.g.
# scene_instruct.json with per-scene task metadata) — that placement is the
# convention, so they ride along with the scenes/ backup rsync. Placed there
# by hand; served read-only.
_DOC_NAME_RE = re.compile(r"^[A-Za-z0-9._\-]{1,255}\.json$")


@app.get("/api/docs", dependencies=[Depends(require_token)])
def list_docs() -> dict:
    docs = [
        {"name": f, "size_bytes": os.path.getsize(os.path.join(db.scenes_dir, f))}
        for f in sorted(os.listdir(db.scenes_dir))
        if f.endswith(".json") and os.path.isfile(os.path.join(db.scenes_dir, f))
    ]
    return {"docs": docs}


@app.get("/api/docs/{name}", dependencies=[Depends(require_token)])
def download_doc(name: str) -> FileResponse:
    path = os.path.join(db.scenes_dir, name)
    if not (_DOC_NAME_RE.match(name) and os.path.isfile(path)):
        raise HTTPException(status_code=404, detail=f"no such doc {name!r}")
    return FileResponse(path, media_type="application/json", filename=name)


# -- dashboard -----------------------------------------------------------------------


def _scene_files(scene_id: str) -> list[tuple[str, str, bool]]:
    """(kind, absolute path, exists) for a scene's file and each asset it references."""
    path = db.scene_path(scene_id)
    rows = [("scene", path, os.path.exists(path))]
    rows += [("asset", p, os.path.isfile(p)) for p in _scene_refs(scene_id)]
    return rows


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    snap = db.snapshot()
    e = html.escape

    def bar(successes: int, target: int) -> str:
        pct = 0 if target <= 0 else min(100, round(100 * successes / target))
        color = "#22a06b" if successes >= target else "#2563eb"
        return (
            f'<div class="bar"><div style="width:{pct}%;background:{color}"></div></div>'
            f"<span>{successes}/{target}</span>"
        )

    scene_rows = []
    for s in snap["scenes"]:
        workers = ", ".join(e(w) for w in s["active_workers"]) or "&mdash;"
        cls = ' class="retired"' if s["retired"] else ""
        done = " done" if s["successes"] >= s["target_successes"] else ""
        file_items = []
        for kind, path, ok in _scene_files(s["scene_id"]):
            tail = "" if ok else ' <span class="miss">missing!</span>'
            file_items.append(f'<li><span class="kind">{kind}</span>{e(path)}{tail}</li>')
        files = "".join(file_items)
        scene_rows.append(
            f'<tr{cls}><td class="name{done}">'
            f'<details class="files" data-scene="{e(s["scene_id"])}">'
            f'<summary>{e(s["scene_id"])}</summary><ul>{files}</ul></details></td>'
            f'<td class="prog">{bar(s["successes"], s["target_successes"])}</td>'
            f'<td>{s["failures"]}</td><td>{s["priority"]}</td><td>{workers}</td>'
            f'<td class="desc">{e(s["task_description"] or "")}</td></tr>'
        )
    collector_rows = []
    for c in snap["collectors"]:
        ago = max(0, int(snap["server_time"] - c["last_seen"]))
        state = '<span class="on">online</span>' if c["online"] else f'<span class="off">offline {ago}s</span>'
        scenes = ", ".join(e(s) for s in c["active_scenes"]) or "&mdash;"
        collector_rows.append(
            f'<tr><td class="name">{e(c["collector_id"])}</td><td>{state}</td>'
            f'<td>{scenes}</td><td>{c["episodes_total"]}</td></tr>'
        )
    t = snap["totals"]
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="5"><title>Duo Fleet</title><style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #f6f7f9; color: #1c2430; }}
h1 {{ font-size: 1.4rem; }} h2 {{ font-size: 1.1rem; margin-top: 2rem; }}
table {{ border-collapse: collapse; background: #fff; width: 100%; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
th, td {{ text-align: left; padding: .45rem .8rem; border-bottom: 1px solid #e5e8ee; font-size: .9rem; }}
th {{ background: #eef0f4; }} .name {{ font-family: ui-monospace, monospace; font-size: .85rem; }}
.name.done {{ color: #22a06b; }} .retired {{ opacity: .45; }} .desc {{ color: #5c6675; font-size: .8rem; }}
.prog {{ min-width: 12rem; }} .bar {{ display: inline-block; width: 8rem; height: .6rem; background: #e5e8ee;
border-radius: 3px; margin-right: .5rem; vertical-align: middle; }} .bar div {{ height: 100%; border-radius: 3px; }}
.on {{ color: #22a06b; font-weight: 600; }} .off {{ color: #b45309; }}
.totals {{ color: #5c6675; margin-bottom: 1rem; }}
details.files summary {{ cursor: pointer; }}
details.files ul {{ list-style: none; margin: .35rem 0 .25rem; padding: 0 0 0 1rem; }}
details.files li {{ font-size: .75rem; color: #5c6675; white-space: nowrap; }}
details.files .kind {{ display: inline-block; min-width: 3.2em; color: #8a94a3; text-transform: uppercase;
font-size: .65rem; letter-spacing: .04em; }}
details.files .miss {{ color: #b0413e; font-weight: 600; }}
</style></head><body>
<h1>Duo Fleet — collection status</h1>
<div class="totals">{t["successes_toward_target"]}/{t["target_successes"]} successes toward target across
{t["scenes"]} scenes &middot; {t["episodes"]} episodes stored &middot; workers considered live for
{int(WORKER_TTL_S)}s &middot; auto-refreshes every 5s</div>
<h2>Scenes</h2>
<table><tr><th>Scene</th><th>Successes</th><th>Failures</th><th>Priority</th><th>Working now</th><th>Task</th></tr>
{"".join(scene_rows) or '<tr><td colspan="6">No scenes seeded yet.</td></tr>'}</table>
<h2>Collectors</h2>
<table><tr><th>Collector</th><th>State</th><th>Working on</th><th>Episodes reported</th></tr>
{"".join(collector_rows) or '<tr><td colspan="4">No collectors have checked in yet.</td></tr>'}</table>
<script>
// The page reloads every 5s (meta refresh); keep expanded scene rows expanded.
try {{
  const key = "duoFleetOpenScenes";
  const open = new Set(JSON.parse(sessionStorage.getItem(key) || "[]"));
  document.querySelectorAll("details.files").forEach((d) => {{
    if (open.has(d.dataset.scene)) d.open = true;
    d.addEventListener("toggle", () => {{
      if (d.open) open.add(d.dataset.scene); else open.delete(d.dataset.scene);
      sessionStorage.setItem(key, JSON.stringify([...open]));
    }});
  }});
}} catch (err) {{ /* storage unavailable: dropdowns still work, just not sticky */ }}
</script>
</body></html>"""
