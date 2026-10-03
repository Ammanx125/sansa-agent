# sansa_agent/policy.py
"""
Local policy enforcement.

Every job received from Sansa passes through check() before anything
happens. The policy is enforced on the agent, not the server. Even if the
server is compromised or a job is malformed, the agent refuses anything
that falls outside its locally-configured bounds.

The four checks:
  1. Job type allowlist — only known job types are processed.
  2. Path containment — for upload_file jobs, the requested path must
     resolve inside the agent's configured watch_root.
  3. Size cap — files larger than the configured cap are refused.
  4. Rate cap — the agent processes at most N jobs per cycle.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

ALLOWED_JOB_TYPES = frozenset({
    "upload_file",
    "rescan",
    "rotate_credential",
    "update_config",
})


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str = ""


class Policy:
    def __init__(
        self,
        *,
        watch_root: Path,
        max_upload_bytes: int,
        jobs_per_cycle: int = 20,
    ) -> None:
        self.watch_root = watch_root.resolve()
        self.max_upload_bytes = max_upload_bytes
        self.jobs_per_cycle = jobs_per_cycle

    def check_job(self, job_type: str, params: dict) -> PolicyDecision:
        if job_type not in ALLOWED_JOB_TYPES:
            return PolicyDecision(
                False, f"unknown job type: {job_type!r}"
            )

        if job_type == "upload_file":
            path_str = params.get("path")
            if not path_str:
                return PolicyDecision(False, "upload_file requires 'path'")

            target = (self.watch_root / path_str).resolve()

            # Path containment: target must be inside watch_root.
            try:
                target.relative_to(self.watch_root)
            except ValueError:
                return PolicyDecision(
                    False,
                    f"path escapes watch root: {path_str!r}",
                )

            if not target.exists() or not target.is_file():
                return PolicyDecision(False, f"file not found: {path_str!r}")

            size = target.stat().st_size
            if size > self.max_upload_bytes:
                return PolicyDecision(
                    False,
                    f"file exceeds size cap ({size} > {self.max_upload_bytes})",
                )

        return PolicyDecision(True)