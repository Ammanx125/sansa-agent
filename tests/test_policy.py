# sansa-agent/tests/test_policy.py
import tempfile
from pathlib import Path

from sansa_agent.policy import Policy


def _policy(root: Path) -> Policy:
    return Policy(
        watch_root=root, max_upload_bytes=1_000_000, jobs_per_cycle=10
    )


def test_rejects_unknown_job_type():
    with tempfile.TemporaryDirectory() as d:
        p = _policy(Path(d))
        decision = p.check_job("run_shell", {})
        assert not decision.allowed
        assert "unknown job type" in decision.reason


def test_rejects_unimplemented_job_types():
    with tempfile.TemporaryDirectory() as d:
        p = _policy(Path(d))
        for job_type in ("rotate_credential", "update_config"):
            decision = p.check_job(job_type, {})
            assert not decision.allowed
            assert "unknown job type" in decision.reason


def test_rejects_path_outside_watch_root():
    with tempfile.TemporaryDirectory() as d:
        p = _policy(Path(d))
        decision = p.check_job("upload_file", {"path": "../etc/passwd"})
        assert not decision.allowed
        assert "escapes" in decision.reason


def test_rejects_absolute_path_outside_root():
    with tempfile.TemporaryDirectory() as d:
        p = _policy(Path(d))
        decision = p.check_job("upload_file", {"path": "/etc/passwd"})
        assert not decision.allowed


def test_rejects_oversized_file():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        big = root / "big.csv"
        big.write_bytes(b"x" * 2_000_000)
        p = Policy(watch_root=root, max_upload_bytes=1_000_000, jobs_per_cycle=10)
        decision = p.check_job("upload_file", {"path": "big.csv"})
        assert not decision.allowed
        assert "size cap" in decision.reason


def test_allows_valid_upload():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        p = _policy(root)
        decision = p.check_job("upload_file", {"path": "data.csv"})
        assert decision.allowed


def test_rejects_missing_file():
    with tempfile.TemporaryDirectory() as d:
        p = _policy(Path(d))
        decision = p.check_job("upload_file", {"path": "nothere.csv"})
        assert not decision.allowed
        assert "not found" in decision.reason