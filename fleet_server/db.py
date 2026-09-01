"""SQLite state store for the fleet server.

All shared state lives in one SQLite database guarded by a single process-wide
lock: every read and every write runs as one serialized critical section, so
there are no read-modify-write races anywhere by construction. Throughput needs
are trivial (a handful of collectors reporting an episode every few minutes),
so serializing everything is the simplest correct design.

Correctness rules encoded here:

- No mutable progress counters: per-scene success/failure counts are always
  ``COUNT(*)`` over the ``episodes`` table, computed inside the same critical
  section as whatever decision uses them.
- ``episodes.episode_uuid`` is the idempotency key: reports are UPSERTs, so a
  collector retrying after a network timeout can only overwrite its own row.
- ``workers`` rows are presence, not reservations: many collectors may work on
  one scene; a row whose ``last_seen`` is stale simply stops being displayed
  (and stops counting toward the suggestion ranking). Nothing is ever blocked.
- Episodes are never rejected: an episode for an unknown scene creates a
  placeholder scene row, and exceeding a scene's target is fine (soft quota).
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time

# A workers/collectors row older than this is treated as offline/gone.
WORKER_TTL_S = 120.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scenes (
    scene_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0,
    target_successes INTEGER NOT NULL DEFAULT 20,
    priority INTEGER NOT NULL DEFAULT 0,
    task_description TEXT,
    retired INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS workers (
    scene_id TEXT NOT NULL,
    collector_id TEXT NOT NULL,
    since REAL NOT NULL,
    last_seen REAL NOT NULL,
    PRIMARY KEY (scene_id, collector_id)
);
CREATE TABLE IF NOT EXISTS collectors (
    collector_id TEXT PRIMARY KEY,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS episodes (
    episode_uuid TEXT PRIMARY KEY,
    scene_id TEXT NOT NULL,
    collector_id TEXT NOT NULL,
    success INTEGER NOT NULL,
    embodiment TEXT,
    num_steps INTEGER,
    file_size INTEGER NOT NULL DEFAULT 0,
    meta_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_episodes_scene ON episodes (scene_id);
"""


class Database:
    """The fleet state store; every public method is one atomic critical section."""

    def __init__(self, data_dir: str, default_target: int = 20):
        self.data_dir = os.path.abspath(data_dir)
        self.scenes_dir = os.path.join(self.data_dir, "scenes")
        self.episodes_dir = os.path.join(self.data_dir, "episodes")
        # Object assets (meshes etc.) the scene files reference. The server
        # never reads them; they live here so backups cover them.
        self.assets_dir = os.path.join(self.data_dir, "assets")
        for path in (self.data_dir, self.scenes_dir, self.episodes_dir, self.assets_dir):
            os.makedirs(path, exist_ok=True)
        self.default_target = default_target
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(os.path.join(self.data_dir, "fleet.sqlite3"), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    @contextlib.contextmanager
    def _txn(self):
        """One serialized transaction: commits on success, rolls back on error."""
        with self._lock, self._conn:
            yield self._conn

    def backup_db(self, dest_path: str) -> None:
        """Write a consistent snapshot of the database to ``dest_path`` (atomic).

        Uses SQLite's backup API inside the process lock, so the copy is a
        clean point-in-time image even though the live db is in WAL mode.
        """
        tmp = f"{dest_path}.part"
        if os.path.exists(tmp):
            os.remove(tmp)
        with self._lock:
            dest = sqlite3.connect(tmp)
            try:
                self._conn.backup(dest)
            finally:
                dest.close()
        os.replace(tmp, dest_path)

    # -- paths ----------------------------------------------------------------

    def scene_path(self, scene_id: str) -> str:
        return os.path.join(self.scenes_dir, os.path.basename(scene_id))

    def episode_path(self, episode_uuid: str) -> str:
        return os.path.join(self.episodes_dir, f"{episode_uuid}.hdf5")

    # -- collectors and presence ------------------------------------------------

    def checkin(self, collector_id: str) -> dict:
        """Register the collector, clear its stale presence rows, return a snapshot."""
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO collectors (collector_id, first_seen, last_seen) VALUES (?, ?, ?)"
                " ON CONFLICT(collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (collector_id, now, now),
            )
            # A checkin is a fresh start: presence left over from a crashed
            # previous run of this collector is wrong, drop it.
            conn.execute("DELETE FROM workers WHERE collector_id = ?", (collector_id,))
            return self._snapshot(conn)

    def heartbeat(self, collector_id: str, scene_id: str | None) -> dict:
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO collectors (collector_id, first_seen, last_seen) VALUES (?, ?, ?)"
                " ON CONFLICT(collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (collector_id, now, now),
            )
            if scene_id:
                conn.execute(
                    "INSERT INTO workers (scene_id, collector_id, since, last_seen) VALUES (?, ?, ?, ?)"
                    " ON CONFLICT(scene_id, collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                    (scene_id, collector_id, now, now),
                )
        return {"ok": True, "server_time": now}

    def declare_worker(self, collector_id: str, scene_id: str) -> dict | None:
        """Upsert presence of a collector on a scene; returns the scene row (or None)."""
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO workers (scene_id, collector_id, since, last_seen) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(scene_id, collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (scene_id, collector_id, now, now),
            )
            conn.execute(
                "INSERT INTO collectors (collector_id, first_seen, last_seen) VALUES (?, ?, ?)"
                " ON CONFLICT(collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (collector_id, now, now),
            )
            rows = self._scene_rows(conn, scene_id=scene_id)
        return rows[0] if rows else None

    def remove_worker(self, collector_id: str, scene_id: str | None) -> dict:
        with self._txn() as conn:
            if scene_id:
                conn.execute("DELETE FROM workers WHERE collector_id = ? AND scene_id = ?", (collector_id, scene_id))
            else:
                conn.execute("DELETE FROM workers WHERE collector_id = ?", (collector_id,))
        return {"ok": True}

    # -- scenes -------------------------------------------------------------------

    def upsert_scene(
        self,
        scene_id: str,
        sha256: str,
        size_bytes: int,
        target_successes: int | None,
        priority: int | None,
        task_description: str | None,
    ) -> dict:
        """Insert or update a scene row after its file was stored on disk."""
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO scenes (scene_id, sha256, size_bytes, target_successes, priority,"
                " task_description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(scene_id) DO UPDATE SET sha256 = excluded.sha256,"
                " size_bytes = excluded.size_bytes,"
                " target_successes = COALESCE(?, scenes.target_successes),"
                " priority = COALESCE(?, scenes.priority),"
                " task_description = COALESCE(?, scenes.task_description)",
                (
                    scene_id,
                    sha256,
                    size_bytes,
                    target_successes if target_successes is not None else self.default_target,
                    priority if priority is not None else 0,
                    task_description,
                    time.time(),
                    target_successes,
                    priority,
                    task_description,
                ),
            )
            return self._scene_rows(conn, scene_id=scene_id)[0]

    def patch_scene(self, scene_id: str, fields: dict) -> dict | None:
        allowed = {"target_successes", "priority", "task_description", "retired"}
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        with self._txn() as conn:
            if updates:
                sets = ", ".join(f"{k} = ?" for k in updates)
                cur = conn.execute(f"UPDATE scenes SET {sets} WHERE scene_id = ?", (*updates.values(), scene_id))
                if cur.rowcount == 0:
                    return None
            rows = self._scene_rows(conn, scene_id=scene_id)
        return rows[0] if rows else None

    def suggest(self, n: int, collector_id: str | None = None) -> list[dict]:
        """Under-target scenes, best-first.

        Ranking: highest priority, then fewest active workers (spread collectors
        out), then most remaining successes, then name for determinism. Scenes
        the asking collector is already working on rank as if it were absent, so
        re-asking is stable. This is advice, not assignment — nothing is locked.
        """
        with self._txn() as conn:
            rows = self._scene_rows(conn)
        candidates = [r for r in rows if not r["retired"] and r["successes"] < r["target_successes"]]

        def rank(row: dict):
            others = [w for w in row["active_workers"] if w != collector_id]
            remaining = row["target_successes"] - row["successes"]
            return (-row["priority"], len(others), -remaining, row["scene_id"])

        return sorted(candidates, key=rank)[:n]

    # -- episodes -------------------------------------------------------------------

    def record_episode(
        self,
        episode_uuid: str,
        scene_id: str,
        collector_id: str,
        success: bool,
        embodiment: str | None,
        num_steps: int | None,
        file_size: int,
        meta_json: str,
    ) -> dict:
        """UPSERT one labeled episode; returns the scene's progress after the insert.

        Also refreshes the collector's presence on the scene (reporting an
        episode is proof of working on it) and auto-creates a placeholder scene
        row for unknown scenes — data is never rejected.
        """
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO scenes (scene_id, target_successes, created_at) VALUES (?, ?, ?)"
                " ON CONFLICT(scene_id) DO NOTHING",
                (scene_id, self.default_target, now),
            )
            conn.execute(
                "INSERT INTO episodes (episode_uuid, scene_id, collector_id, success, embodiment,"
                " num_steps, file_size, meta_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(episode_uuid) DO UPDATE SET scene_id = excluded.scene_id,"
                " collector_id = excluded.collector_id, success = excluded.success,"
                " embodiment = excluded.embodiment, num_steps = excluded.num_steps,"
                " file_size = excluded.file_size, meta_json = excluded.meta_json",
                (episode_uuid, scene_id, collector_id, int(success), embodiment, num_steps, file_size, meta_json, now),
            )
            conn.execute(
                "INSERT INTO workers (scene_id, collector_id, since, last_seen) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(scene_id, collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (scene_id, collector_id, now, now),
            )
            conn.execute(
                "INSERT INTO collectors (collector_id, first_seen, last_seen) VALUES (?, ?, ?)"
                " ON CONFLICT(collector_id) DO UPDATE SET last_seen = excluded.last_seen",
                (collector_id, now, now),
            )
            row = self._scene_rows(conn, scene_id=scene_id)[0]
        return {
            "scene_id": scene_id,
            "successes": row["successes"],
            "failures": row["failures"],
            "target_successes": row["target_successes"],
            "at_quota": row["successes"] >= row["target_successes"],
        }

    def list_episodes(self, scene_id: str | None, limit: int) -> list[dict]:
        with self._txn() as conn:
            if scene_id:
                cur = conn.execute(
                    "SELECT * FROM episodes WHERE scene_id = ? ORDER BY created_at DESC LIMIT ?", (scene_id, limit)
                )
            else:
                cur = conn.execute("SELECT * FROM episodes ORDER BY created_at DESC LIMIT ?", (limit,))
            return [dict(r) for r in cur.fetchall()]

    # -- snapshots -----------------------------------------------------------------

    def snapshot(self) -> dict:
        with self._txn() as conn:
            return self._snapshot(conn)

    def _scene_rows(self, conn: sqlite3.Connection, scene_id: str | None = None) -> list[dict]:
        """Scene rows with derived progress counts and live worker lists."""
        stale = time.time() - WORKER_TTL_S
        where = "WHERE s.scene_id = ?" if scene_id else ""
        params = (scene_id,) if scene_id else ()
        cur = conn.execute(
            "SELECT s.*,"
            " (SELECT COUNT(*) FROM episodes e WHERE e.scene_id = s.scene_id AND e.success = 1) AS successes,"
            " (SELECT COUNT(*) FROM episodes e WHERE e.scene_id = s.scene_id AND e.success = 0) AS failures"
            f" FROM scenes s {where} ORDER BY s.scene_id",
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            cur = conn.execute(
                "SELECT collector_id FROM workers WHERE scene_id = ? AND last_seen > ? ORDER BY collector_id",
                (row["scene_id"], stale),
            )
            row["active_workers"] = [r["collector_id"] for r in cur.fetchall()]
            row["retired"] = bool(row["retired"])
        return rows

    def _snapshot(self, conn: sqlite3.Connection) -> dict:
        now = time.time()
        stale = now - WORKER_TTL_S
        scenes = self._scene_rows(conn)
        cur = conn.execute(
            "SELECT c.*, (SELECT COUNT(*) FROM episodes e WHERE e.collector_id = c.collector_id) AS episodes_total"
            " FROM collectors c ORDER BY c.collector_id"
        )
        collectors = []
        for r in cur.fetchall():
            row = dict(r)
            row["online"] = row["last_seen"] > stale
            wcur = conn.execute(
                "SELECT scene_id FROM workers WHERE collector_id = ? AND last_seen > ? ORDER BY scene_id",
                (row["collector_id"], stale),
            )
            row["active_scenes"] = [w["scene_id"] for w in wcur.fetchall()]
            collectors.append(row)
        total_target = sum(s["target_successes"] for s in scenes if not s["retired"])
        total_success = sum(min(s["successes"], s["target_successes"]) for s in scenes if not s["retired"])
        return {
            "server_time": now,
            "scenes": scenes,
            "collectors": collectors,
            "totals": {
                "scenes": len([s for s in scenes if not s["retired"]]),
                "successes_toward_target": total_success,
                "target_successes": total_target,
                "episodes": conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0],
            },
        }
