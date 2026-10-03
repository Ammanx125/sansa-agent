# sansa_agent/cli.py
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from sansa_agent import config as cfg
from sansa_agent.client import SansaClient
from sansa_agent.sync import main_async as run_loop
from sansa_agent.watcher import main_async as run_demo_watcher


def cmd_enroll(args: argparse.Namespace) -> int:
    """
    One-time enrollment. Takes a server URL, an enrollment token, a watch
    root, and (optionally) the source_id. Exchanges the token for a
    credential and writes config.json.
    """
    watch_root = Path(args.watch_root).expanduser().resolve()
    if not watch_root.exists() or not watch_root.is_dir():
        print(f"watch root does not exist or is not a directory: {watch_root}",
              file=sys.stderr)
        return 1

    async def _enroll():
        client = SansaClient(base_url=args.server_url)
        result = await client.register(
            enrollment_token=args.enrollment_token,
            agent_metadata={
                "hostname": Path.home().name,
                "watch_root": str(watch_root),
            },
        )
        return result

    result = asyncio.run(_enroll())

    cfg.save({
        "server_url": args.server_url,
        "agent_id": str(result["agent_id"]),
        "credential": result["credential"],
        "source_id": str(result["source_id"]),
        "watch_root": str(watch_root),
        "files": {},
    })
    print(f"enrolled. agent_id={result['agent_id']}")
    print(f"config written to {cfg.config_path()}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    try:
        asyncio.run(run_loop(interval_seconds=args.interval))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_run_demo_watcher(args: argparse.Namespace) -> int:
    webhook_url = os.environ.get("SANSA_DEMO_WEBHOOK_URL")
    webhook_secret = os.environ.get("SANSA_DEMO_WEBHOOK_SECRET")
    if not webhook_url or not webhook_secret:
        print(
            "Set SANSA_DEMO_WEBHOOK_URL and SANSA_DEMO_WEBHOOK_SECRET.",
            file=sys.stderr,
        )
        return 1

    try:
        asyncio.run(
            run_demo_watcher(
                mode=args.mode,
                source_file=Path(args.source_file) if args.source_file else None,
                webhook_url=webhook_url,
                webhook_secret=webhook_secret,
                interval_seconds=args.interval,
            )
        )
    except KeyboardInterrupt:
        pass
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    data = cfg.load()
    if not data:
        print("agent is not configured.")
        return 1
    print(f"server_url: {data.get('server_url')}")
    print(f"agent_id:   {data.get('agent_id')}")
    print(f"source_id:  {data.get('source_id')}")
    print(f"watch_root: {data.get('watch_root')}")
    print(f"files:      {len(data.get('files', {}))}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="sansa-agent")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_enroll = sub.add_parser("enroll", help="enroll with a Sansa server")
    p_enroll.add_argument("--server-url", required=True)
    p_enroll.add_argument("--enrollment-token", required=True)
    p_enroll.add_argument("--watch-root", required=True)
    p_enroll.set_defaults(func=cmd_enroll)

    p_run = sub.add_parser("run", help="run the sync loop")
    p_run.add_argument("--interval", type=int, default=60)
    p_run.set_defaults(func=cmd_run)

    p_demo = sub.add_parser(
        "run-demo-watcher",
        help="generate vehicle events or forward CSV rows to a signed webhook",
    )
    p_demo.add_argument(
        "--mode", choices=("generated", "csv"), default="generated"
    )
    p_demo.add_argument("--source-file")
    p_demo.add_argument("--interval", type=int, default=30)
    p_demo.set_defaults(func=cmd_run_demo_watcher)

    p_status = sub.add_parser("status", help="show config and file count")
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())