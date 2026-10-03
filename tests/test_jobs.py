import asyncio
import hashlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from sansa_agent.jobs import _execute_one
from sansa_agent.policy import Policy


def test_upload_uses_hash_of_uploaded_bytes(tmp_path: Path):
    content = b"current content"
    (tmp_path / "data.csv").write_bytes(content)
    client = AsyncMock()
    policy = Policy(watch_root=tmp_path, max_upload_bytes=100)

    asyncio.run(
        _execute_one(
            client=client,
            agent_id="agent-id",
            policy=policy,
            config={},
            job={
                "id": "job-id",
                "job_type": "upload_file",
                "params": {
                    "path": "data.csv",
                    "expected_hash": hashlib.sha256(content).hexdigest(),
                },
            },
        )
    )

    client.upload_content.assert_awaited_once_with(
        agent_id="agent-id",
        content_hash=hashlib.sha256(content).hexdigest(),
        content=content,
    )
    client.report_job_result.assert_awaited_once_with(
        agent_id="agent-id",
        job_id="job-id",
        status="completed",
        result={},
    )


def test_upload_rejects_content_with_unexpected_hash(tmp_path: Path):
    (tmp_path / "data.csv").write_bytes(b"changed content")
    client = AsyncMock()
    policy = Policy(watch_root=tmp_path, max_upload_bytes=100)

    asyncio.run(
        _execute_one(
            client=client,
            agent_id="agent-id",
            policy=policy,
            config={},
            job={
                "id": "job-id",
                "job_type": "upload_file",
                "params": {"path": "data.csv", "expected_hash": "expected-hash"},
            },
        )
    )

    client.upload_content.assert_not_awaited()
    report = client.report_job_result.await_args.kwargs
    assert report["status"] == "failed"
    assert "file changed before upload" in report["error"]


def test_upload_rechecks_size_of_bytes_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = tmp_path / "data.csv"
    target.write_bytes(b"small")
    original_read_bytes = Path.read_bytes
    client = AsyncMock()
    policy = Policy(watch_root=tmp_path, max_upload_bytes=10)

    def read_grown_file(path: Path) -> bytes:
        if path == target:
            return b"x" * 11
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", read_grown_file)

    asyncio.run(
        _execute_one(
            client=client,
            agent_id="agent-id",
            policy=policy,
            config={},
            job={
                "id": "job-id",
                "job_type": "upload_file",
                "params": {"path": "data.csv"},
            },
        )
    )

    client.upload_content.assert_not_awaited()
    report = client.report_job_result.await_args.kwargs
    assert report["status"] == "failed"
    assert "file exceeds size cap" in report["error"]
