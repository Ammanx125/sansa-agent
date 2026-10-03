# sansa_agent/state.py
"""
Local file-state tracking.

The agent remembers the last observed {hash, size, mtime, ctime} per path.
On each scan, it compares the current state to what it remembers to
determine what changed.

This is the delta computation that keeps the agent from re-reporting
every file on every scan.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class FileDelta:
    path: str
    content_hash: str
    byte_size: int
    mtime: str
    ctime: str
    status: str  # new | changed | unchanged | deleted


def sha256_of(path: Path) -> str:
    """Stream the file through SHA-256. Bounded memory."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def scan(root: Path) -> dict[str, dict[str, Any]]:
    """
    Walk `root` recursively and return {relative_path: file_info}.

    file_info: {hash, size, mtime, ctime}
    mtime/ctime are ISO-8601 strings in UTC.
    """
    out: dict[str, dict[str, Any]] = {}
    if not root.exists():
        return out
    for entry in root.rglob("*"):
        if not entry.is_file():
            continue
        try:
            stat = entry.stat()
            rel = str(entry.relative_to(root)).replace("\\", "/")
            out[rel] = {
                "hash": sha256_of(entry),
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
                "ctime": datetime.fromtimestamp(stat.st_ctime, tz=UTC).isoformat(),
            }
        except OSError:
            # File disappeared mid-scan, or permission denied. Skip.
            continue
    return out


def compute_deltas(
    *, current: dict[str, dict[str, Any]], previous: dict[str, dict[str, Any]]
) -> list[FileDelta]:
    """
    Compare the current scan to the previous state. Returns only the files
    that are new, changed, or deleted.
    """
    deltas: list[FileDelta] = []

    for path, info in current.items():
        prev = previous.get(path)
        if prev is None:
            status = "new"
        elif prev["hash"] != info["hash"]:
            status = "changed"
        else:
            status = "unchanged"
        if status == "unchanged":
            continue
        deltas.append(FileDelta(
            path=path,
            content_hash=info["hash"],
            byte_size=info["size"],
            mtime=info["mtime"],
            ctime=info["ctime"],
            status=status,
        ))

    for path, prev in previous.items():
        if path not in current:
            deltas.append(FileDelta(
                path=path,
                content_hash=prev["hash"],
                byte_size=prev["size"],
                mtime=prev["mtime"],
                ctime=prev["ctime"],
                status="deleted",
            ))

    return deltas


def to_sync_payload(deltas: list[FileDelta]) -> list[dict[str, Any]]:
    return [
        {
            "path": d.path,
            "content_hash": d.content_hash,
            "byte_size": d.byte_size,
            "mtime": d.mtime,
            "ctime": d.ctime,
            "status": d.status,
        }
        for d in deltas
    ]