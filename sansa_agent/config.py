# sansa_agent/config.py
"""
Local config and state.

Stored in a single JSON file. Location:
  - Linux/macOS: ~/.sansa-agent/config.json
  - Windows:     %LOCALAPPDATA%\\SansaAgent\\config.json

Contains:
  - server_url:   the Sansa base URL the agent talks to
  - agent_id:     assigned by Sansa at register time
  - credential:   long-lived bearer token
  - source_id:    the DataSource this agent feeds
  - watch_root:   the folder the agent watches
  - files:        per-path {hash, size, mtime, ctime, first_seen_at}
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def config_dir() -> Path:
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA", str(Path.home()))
        return Path(base) / "SansaAgent"
    return Path.home() / ".sansa-agent"


def config_path() -> Path:
    return config_dir() / "config.json"


def load() -> dict[str, Any]:
    path = config_path()
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save(data: dict[str, Any]) -> None:
    d = config_dir()
    d.mkdir(parents=True, exist_ok=True)
    # Write to a temp file then rename, so a crash mid-write doesn't
    # corrupt the config.
    tmp = config_path().with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(config_path())


def require_fields(*names: str) -> dict[str, Any]:
    data = load()
    missing = [n for n in names if n not in data or data[n] is None]
    if missing:
        raise SystemExit(
            f"agent is not configured; missing: {', '.join(missing)}. "
            f"Run `sansa-agent enroll` first."
        )
    return data