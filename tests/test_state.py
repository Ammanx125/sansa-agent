# sansa-agent/tests/test_state.py
from __future__ import annotations

import tempfile
from pathlib import Path

from sansa_agent.state import compute_deltas, scan


def test_scan_empty_dir():
    with tempfile.TemporaryDirectory() as d:
        assert scan(Path(d)) == {}


def test_scan_finds_files():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "a.txt").write_text("hello", encoding="utf-8")
        (root / "sub").mkdir()
        (root / "sub" / "b.txt").write_text("world", encoding="utf-8")

        result = scan(root)
        assert set(result.keys()) == {"a.txt", "sub/b.txt"}
        assert result["a.txt"]["size"] == 5


def test_compute_deltas_new_files():
    current = {
        "a.txt": {"hash": "x", "size": 1, "mtime": "t", "ctime": "t"},
    }
    deltas = compute_deltas(current=current, previous={})
    assert len(deltas) == 1
    assert deltas[0].status == "new"


def test_compute_deltas_unchanged_are_skipped():
    info = {"hash": "x", "size": 1, "mtime": "t", "ctime": "t"}
    deltas = compute_deltas(current={"a.txt": info}, previous={"a.txt": info})
    assert deltas == []


def test_compute_deltas_changed_and_deleted():
    prev_a = {"hash": "old", "size": 1, "mtime": "t", "ctime": "t"}
    prev_b = {"hash": "y", "size": 2, "mtime": "t", "ctime": "t"}
    cur_a = {"hash": "new", "size": 3, "mtime": "t2", "ctime": "t2"}

    deltas = compute_deltas(
        current={"a.txt": cur_a},
        previous={"a.txt": prev_a, "b.txt": prev_b},
    )
    by_path = {d.path: d for d in deltas}
    assert by_path["a.txt"].status == "changed"
    assert by_path["b.txt"].status == "deleted"