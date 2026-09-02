"""Periodic one-way backup of the data dir to a ``gs://`` destination.

Enabled by setting ``FLEET_SYNC_DEST``; every ``FLEET_SYNC_INTERVAL_S`` the
server pushes:

- ``fleet.sqlite3`` — as a consistent snapshot taken with SQLite's backup API
  inside the database lock (the live WAL-mode file must never be copied raw),
- ``scenes/``, ``episodes/`` and ``assets/`` — via ``gcloud storage rsync``,
  skipping the ``.part-*`` temp files of uploads still in flight.

Nothing is ever deleted at the destination, so a scene removed locally (or a
whole restarted data dir) can only add to the bucket, never destroy history.
Sync failures are logged and retried at the next tick; the API is unaffected.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from .db import Database

# uvicorn only installs handlers for its own loggers; borrowing "uvicorn.error"
# puts sync lines in the same stream (stderr/journal) as the rest of the app.
log = logging.getLogger("uvicorn.error")


class Syncer:
    def __init__(self, db: Database, dest: str, interval_s: float = 300.0, gcloud: str = "gcloud"):
        self.db = db
        self.dest = dest.rstrip("/")
        self.interval_s = interval_s
        self.gcloud = gcloud
        self._db_snapshot = os.path.join(db.data_dir, "fleet.sqlite3.snapshot")

    async def _run(self, *args: str) -> None:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"{args[0]} exited {proc.returncode}: {out.decode(errors='replace')[-2000:]}")

    async def sync_once(self) -> None:
        await asyncio.to_thread(self.db.backup_db, self._db_snapshot)
        exclude = r".*\.part-.*"
        await self._run(
            self.gcloud, "storage", "rsync", "--recursive", f"--exclude={exclude}",
            self.db.scenes_dir, f"{self.dest}/scenes",
        )
        await self._run(
            self.gcloud, "storage", "rsync", "--recursive", f"--exclude={exclude}",
            self.db.episodes_dir, f"{self.dest}/episodes",
        )
        await self._run(
            self.gcloud, "storage", "rsync", "--recursive", f"--exclude={exclude}",
            self.db.assets_dir, f"{self.dest}/assets",
        )
        await self._run(self.gcloud, "storage", "cp", self._db_snapshot, f"{self.dest}/fleet.sqlite3")
        # Loose documents at the data dir root (e.g. scene_instruct.json).
        docs = sorted(
            os.path.join(self.db.data_dir, f) for f in os.listdir(self.db.data_dir) if f.endswith(".json")
        )
        if docs:
            await self._run(self.gcloud, "storage", "cp", *docs, f"{self.dest}/")

    async def run_forever(self) -> None:
        log.info("[sync] %s -> %s every %.0fs", self.db.data_dir, self.dest, self.interval_s)
        while True:
            started = time.monotonic()
            try:
                await self.sync_once()
                log.info("[sync] ok in %.1fs", time.monotonic() - started)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("[sync] failed (next attempt in %.0fs): %s", self.interval_s, exc)
            await asyncio.sleep(self.interval_s)
