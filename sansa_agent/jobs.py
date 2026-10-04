# sansa_agent/jobs.py
"""
Job polling, execution, and result reporting.

The agent polls for jobs, runs each through the local policy, executes
it if allowed, and reports the outcome.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from sansa_agent.client import SansaClient, SansaClientError
from sansa_agent.policy import Policy

logger = logging.getLogger("sansa_agent.jobs")


class JobExecutionError(Exception):
    """A job could not be safely completed."""


async def poll_and_execute_jobs(
    *,
    client: SansaClient,
    agent_id: str,
    policy: Policy,
    config: dict[str, Any],
) -> int:
    """
    Poll for jobs, execute them, report results. Returns the number of
    jobs processed.
    """
    try:
        jobs = await client.poll_jobs(agent_id=agent_id, limit=policy.jobs_per_cycle)
    except SansaClientError as exc:
        logger.warning("job poll failed: %s", exc)
        return 0

    if not jobs:
        return 0

    logger.info("received %d job(s)", len(jobs))
    processed = 0
    for job in jobs:
        await _execute_one(
            client=client,
            agent_id=agent_id,
            policy=policy,
            config=config,
            job=job,
        )
        processed += 1

    return processed


async def _execute_one(
    *,
    client: SansaClient,
    agent_id: str,
    policy: Policy,
    config: dict[str, Any],
    job: dict[str, Any],
) -> None:
    job_id = job["id"]
    job_type = job["job_type"]
    params = job.get("params") or {}

    decision = policy.check_job(job_type, params)
    if not decision.allowed:
        logger.warning("rejecting job %s: %s", job_id, decision.reason)
        try:
            await client.report_job_result(
                agent_id=agent_id,
                job_id=job_id,
                status="rejected",
                result={},
                error=decision.reason,
            )
        except SansaClientError:
            logger.exception("failed to report rejection for job %s", job_id)
        return

    try:
        if job_type == "upload_file":
            await _execute_upload_file(
                client=client, agent_id=agent_id, policy=policy, params=params
            )
        elif job_type == "rescan":
            # A rescan is a no-op at this level; the sync loop performs
            # a rescan on every cycle anyway. Report success.
            pass
        else:
            raise ValueError(f"unhandled job type: {job_type!r}")

        await client.report_job_result(
            agent_id=agent_id, job_id=job_id, status="completed", result={}
        )
    except Exception as exc:
        logger.exception("job %s failed", job_id)
        try:
            await client.report_job_result(
                agent_id=agent_id,
                job_id=job_id,
                status="failed",
                result={},
                error=str(exc)[:500],
            )
        except SansaClientError:
            logger.exception("failed to report failure for job %s", job_id)


async def _execute_upload_file(
    *,
    client: SansaClient,
    agent_id: str,
    policy: Policy,
    params: dict[str, Any],
) -> None:
    path_str = params["path"]
    expected_hash = params.get("expected_hash")

    target = (policy.watch_root / path_str).resolve()
    content = target.read_bytes()
    if len(content) > policy.max_upload_bytes:
        raise JobExecutionError(
            f"file exceeds size cap ({len(content)} > {policy.max_upload_bytes})"
        )

    actual_hash = hashlib.sha256(content).hexdigest()
    if expected_hash and actual_hash != expected_hash:
        raise JobExecutionError(
            f"file changed before upload: expected {expected_hash}, got {actual_hash}"
        )

    await client.upload_content(
        agent_id=agent_id, content_hash=actual_hash, content=content
    )
    logger.info(
        "uploaded %s (%d bytes; sha256 %s)",
        path_str,
        len(content),
        actual_hash[:12],
    )
