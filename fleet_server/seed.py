"""Seed the fleet database from scene files on the server's own disk.

Copies each scene USDA into the data dir and creates/updates its scene row.
Use this when the scenes are already on the server machine (e.g. rsync'd);
to push scenes over HTTP from a collector machine instead, use the
``fleet_push_scenes.py`` script that lives next to the teleop code.

Examples:

    # every .usda/.usd/.usdz under a directory, 20 successes each
    python -m fleet_server.seed --data-dir ./fleet_data --scene-dir ~/scenes --target 20

    # a scene_list.json (plain paths, or {"scenes": [...]} with optional
    # {"scene": ..., "task_description": ...} entries)
    python -m fleet_server.seed --data-dir ./fleet_data --scene-list ~/scenes/scene_list.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil

from .db import Database


def iter_entries(args: argparse.Namespace) -> list[tuple[str, str | None]]:
    """The (scene_path, task_description) pairs selected by the CLI arguments."""
    entries: list[tuple[str, str | None]] = []
    if args.scene_dir:
        for root, _dirs, files in os.walk(args.scene_dir):
            entries += [(os.path.join(root, f), None) for f in sorted(files) if f.endswith((".usda", ".usd", ".usdz"))]
    if args.scene_list:
        base = os.path.dirname(os.path.abspath(args.scene_list))
        with open(args.scene_list) as f:
            data = json.load(f)
        for entry in data["scenes"] if isinstance(data, dict) else data:
            if isinstance(entry, dict):
                path, description = entry.get("scene", ""), entry.get("task_description")
                description = str(description).strip().strip("'\"") if description else None
            else:
                path, description = entry, None
            if not os.path.isabs(path):
                path = os.path.join(base, path)
            entries.append((path, description))
    entries += [(p, None) for p in args.scenes]
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenes", nargs="*", help="Individual scene files to seed.")
    parser.add_argument("--data-dir", default=os.environ.get("FLEET_DATA_DIR", "./fleet_data"))
    parser.add_argument("--scene-dir", default=None, help="Seed every .usda/.usd under this directory (recursive).")
    parser.add_argument("--scene-list", default=None, help="Seed the scenes named in this scene_list JSON.")
    parser.add_argument("--target", type=int, default=None, help="Target successes per scene (default: keep/20).")
    parser.add_argument("--priority", type=int, default=None, help="Priority (higher = suggested first).")
    args = parser.parse_args()

    entries = iter_entries(args)
    if not entries:
        parser.error("nothing to seed: pass scene files, --scene-dir, or --scene-list")
    db = Database(args.data_dir)
    for path, description in entries:
        if not os.path.exists(path):
            print(f"[SEED] MISSING {path} — skipped")
            continue
        scene_id = os.path.basename(path)
        dest = db.scene_path(scene_id)
        if os.path.abspath(path) != os.path.abspath(dest):
            shutil.copyfile(path, dest)
        with open(dest, "rb") as f:
            sha256 = hashlib.sha256(f.read()).hexdigest()
        row = db.upsert_scene(scene_id, sha256, os.path.getsize(dest), args.target, args.priority, description)
        print(f"[SEED] {scene_id}: target {row['target_successes']}, {row['size_bytes']} bytes, sha {sha256[:12]}")
    print(f"[SEED] Done: {len(entries)} entries into {os.path.abspath(args.data_dir)}")


if __name__ == "__main__":
    main()
