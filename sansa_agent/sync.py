# sansa_agent/sync.py
"""
The agent's main loop.

Every N seconds:
  1. Scan the watch root.
  2. Compute deltas against the last known state.
  3. If there are deltas, POST them to Sansa.
  4. On success, update local state.
  5. Every M cycles, POST a heartbeat.

The watch_root is scanned on every cycle (rglob). For very large folders
this is expensive — the `watchdog` library is a future enhancement for
detecting changes without rescanning. For v1, a periodic full scan is
simple and correct.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

from sansa_agent import config as cfg
from sansa_agent.client import SansaClient, SansaClientError
from sansa_agent.state import compute_deltas, scan, to_sync_payload

logger = logging.getLogger("sansa_agent")


HEARTBEAT_EVERY_N_CYCLES = 5


async def run_sync_loop(
    *,
    interval_seconds: int = 60,
    stop_event: asyncio.Event,
) -> None:
    data = cfg.require_fields(
        "server_url", "agent_id", "credential", "source_id", "watch_root"
    )
    watch_root = Path(data["watch_root"]).expanduser().resolve()
    if not watch_root.exists():
        raise SystemExit(f"watch_root does not exist: {watch_root}")

    client = SansaClient(
        base_url=data["server_url"],
        credential=data["credential"],
    )

    cycle = 0
    logger.info("agent started; watching %s every %ds", watch_root, interval_seconds)

    while not stop_event.is_set():
        try:
            current = scan(watch_root)
            previous = data.get("files", {})
            deltas = compute_deltas(current=current, previous=previous)

            if deltas:
                payload = to_sync_payload(deltas)
                result = await client.sync(
                    agent_id=data["agent_id"], files=payload
                )
                logger.info("synced %d file(s): %s", len(deltas), result["counts"])
                data["files"] = {
                    path: {
                        "hash": info["hash"],
                        "size": info["size"],
                        "mtime": info["mtime"],
                        "ctime": info["ctime"],
                    }
                    for path, info in current.items()
                }
                cfg.save(data)

            cycle += 1
            if cycle % HEARTBEAT_EVERY_N_CYCLES == 0:
                await client.heartbeat(
                    agent_id=data["agent_id"],
                    agent_metadata={
                        "last_cycle_at": datetime.now(UTC).isoformat(),
                        "watch_root": str(watch_root),
                        "file_count": len(current),
                    },
                )

        except SansaClientError as exc:
            logger.warning("sync failed, will retry: %s", exc)
        except Exception:
            logger.exception("unexpected error in sync loop")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass

    logger.info("agent stopped")


async def main_async(*, interval_seconds: int = 60) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows: not supported on ProactorEventLoop
            pass

    await run_sync_loop(
        interval_seconds=interval_seconds, stop_event=stop_event
    )