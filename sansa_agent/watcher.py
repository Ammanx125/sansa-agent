"""Poll a demo CSV and forward new or changed rows to a signed Sansa webhook."""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import logging
import random
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from sansa_agent.config import config_dir

logger = logging.getLogger("sansa_agent.watcher")


class _WebhookResponse(Protocol):
    status_code: int

    def raise_for_status(self) -> None: ...


class _WebhookClient(Protocol):
    async def post(
        self, url: str, *, content: bytes, headers: dict[str, str]
    ) -> _WebhookResponse: ...


def _read_rows(source_file: Path) -> list[dict[str, str]]:
    with source_file.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames or any(
            not name or not name.strip() for name in reader.fieldnames
        ):
            raise ValueError(f"CSV must have non-empty column headers: {source_file}")
        if len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError(f"CSV column headers must be unique: {source_file}")

        rows: list[dict[str, str]] = []
        for row_number, row in enumerate(reader, start=1):
            if None in row:
                raise ValueError(
                    f"CSV row {row_number} has more values than its header"
                )
            if all(value is None or not value.strip() for value in row.values()):
                continue
            rows.append({key: value or "" for key, value in row.items()})
        return rows


def _event_id(
    *, source_file: Path, occurrence: int, row: dict[str, str]
) -> str:
    identity = json.dumps(
        {
            "source": str(source_file),
            "occurrence": occurrence,
            "row": row,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def _load_seen_ids(state_file: Path) -> set[str]:
    if not state_file.exists():
        return set()
    state = json.loads(state_file.read_text(encoding="utf-8"))
    if not isinstance(state, list) or any(not isinstance(item, str) for item in state):
        raise ValueError(f"invalid demo watcher state file: {state_file}")
    return set(state)


def _save_seen_ids(state_file: Path, seen_ids: set[str]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = state_file.with_suffix(f"{state_file.suffix}.tmp")
    temporary_file.write_text(
        json.dumps(sorted(seen_ids), indent=2), encoding="utf-8"
    )
    temporary_file.replace(state_file)


def _state_file_for(source_file: Path) -> Path:
    source_key = hashlib.sha256(str(source_file).encode("utf-8")).hexdigest()[:16]
    return config_dir() / f"demo-watcher-{source_key}.json"


def _generated_state_file_for(webhook_url: str) -> Path:
    webhook_key = hashlib.sha256(webhook_url.encode("utf-8")).hexdigest()[:16]
    return config_dir() / f"demo-vehicle-watcher-{webhook_key}.json"


def _save_generated_state(
    state_file: Path, state: dict[str, int | list[dict[str, Any]]]
) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = state_file.with_suffix(f"{state_file.suffix}.tmp")
    temporary_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temporary_file.replace(state_file)


def _load_generated_state(state_file: Path) -> dict[str, int | list[dict[str, Any]]]:
    if not state_file.exists():
        return {"sequence": 1, "pending": []}

    state = json.loads(state_file.read_text(encoding="utf-8"))
    if (
        not isinstance(state, dict)
        or not isinstance(state.get("sequence"), int)
        or state["sequence"] < 1
        or not isinstance(state.get("pending"), list)
        or any(not isinstance(event, dict) for event in state["pending"])
    ):
        raise ValueError(f"invalid generated watcher state file: {state_file}")
    return state


def _generate_vehicle_events(sequence: int) -> list[dict[str, Any]]:
    rng = random.Random(sequence)
    timestamp = datetime.now(UTC).isoformat()
    vehicle_id = f"BUS-{sequence % 12 + 1:03d}"
    trip_id = f"TRIP-{sequence:06d}"
    route = f"ADD-HWZ-{sequence % 5 + 1:02d}"
    passengers = rng.randint(18, 54)
    distance_km = round(rng.uniform(34.0, 245.0), 1)
    fuel_litres = round(distance_km * rng.uniform(0.22, 0.34), 1)
    purchased_litres = round(rng.uniform(55.0, 110.0), 1)
    fuel_cost = round(purchased_litres * rng.uniform(138.0, 158.0), 2)

    return [
        {
            "event_id": f"demo-{trip_id}-trip",
            "event_type": "vehicle_trip",
            "date": timestamp[:10],
            "timestamp": timestamp,
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "route": route,
            "quantity": passengers,
            "distance_km": distance_km,
            "fuel_litres": fuel_litres,
        },
        {
            "event_id": f"demo-{trip_id}-fuel",
            "event_type": "fuel_purchase",
            "order_date": timestamp[:10],
            "timestamp": timestamp,
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "purchase_order": f"FUEL-PO-{sequence:06d}",
            "product": "Diesel fuel",
            "quantity": purchased_litres,
            "unit_price": round(fuel_cost / purchased_litres, 2),
            "amount": fuel_cost,
            "currency": "ETB",
            "supplier": f"Addis Fuel {sequence % 3 + 1}",
        },
    ]


async def publish_generated_events(
    *,
    client: _WebhookClient,
    webhook_url: str,
    webhook_secret: str,
    state_file: Path,
    state: dict[str, int | list[dict[str, Any]]],
) -> int:
    """Persist and publish one trip/fuel batch, retaining unsent events on failure."""
    pending = state["pending"]
    assert isinstance(pending, list)
    if not pending:
        sequence = state["sequence"]
        assert isinstance(sequence, int)
        state["pending"] = _generate_vehicle_events(sequence)
        _save_generated_state(state_file, state)
        pending = state["pending"]
        assert isinstance(pending, list)

    published = 0
    while pending:
        event = pending[0]
        body = json.dumps(
            event, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        signature = hmac.new(
            webhook_secret.encode("utf-8"), body, hashlib.sha256
        ).hexdigest()
        response = await client.post(
            webhook_url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Sansa-Signature": signature,
            },
        )
        if response.status_code != 202:
            response.raise_for_status()
            raise RuntimeError(
                f"webhook returned unexpected status {response.status_code}; expected 202"
            )

        pending.pop(0)
        published += 1
        if not pending:
            sequence = state["sequence"]
            assert isinstance(sequence, int)
            state["sequence"] = sequence + 1
        _save_generated_state(state_file, state)

    return published


async def publish_new_rows(
    *,
    client: _WebhookClient,
    source_file: Path,
    webhook_url: str,
    webhook_secret: str,
    state_file: Path,
    seen_ids: set[str],
) -> int:
    """Publish each not-yet-delivered CSV row as one signed webhook request."""
    rows = await asyncio.to_thread(_read_rows, source_file)
    published = 0
    occurrences: dict[str, int] = {}

    for row in rows:
        canonical_row = json.dumps(row, sort_keys=True, separators=(",", ":"))
        occurrence = occurrences.get(canonical_row, 0)
        occurrences[canonical_row] = occurrence + 1
        event_id = _event_id(
            source_file=source_file, occurrence=occurrence, row=row
        )
        if event_id in seen_ids:
            continue

        body = json.dumps(
            row, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        signature = hmac.new(
            webhook_secret.encode("utf-8"), body, hashlib.sha256
        ).hexdigest()
        response = await client.post(
            webhook_url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Sansa-Signature": signature,
            },
        )
        if response.status_code != 202:
            response.raise_for_status()
            raise RuntimeError(
                f"webhook returned unexpected status {response.status_code}; expected 202"
            )

        seen_ids.add(event_id)
        await asyncio.to_thread(_save_seen_ids, state_file, seen_ids)
        published += 1

    return published


async def run_demo_watcher(
    *,
    source_file: Path,
    webhook_url: str,
    webhook_secret: str,
    interval_seconds: int,
    stop_event: asyncio.Event,
) -> None:
    source_file = source_file.expanduser().resolve()
    if not source_file.is_file():
        raise SystemExit(f"demo source file does not exist: {source_file}")
    if interval_seconds < 1:
        raise SystemExit("interval_seconds must be at least 1")
    parsed_url = urlsplit(webhook_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise SystemExit("webhook URL must be an absolute HTTP or HTTPS URL")
    if not webhook_secret:
        raise SystemExit("webhook secret must not be empty")

    state_file = _state_file_for(source_file)
    seen_ids = await asyncio.to_thread(_load_seen_ids, state_file)
    logger.info("demo watcher started for %s", source_file)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        while not stop_event.is_set():
            try:
                published = await publish_new_rows(
                    client=client,
                    source_file=source_file,
                    webhook_url=webhook_url,
                    webhook_secret=webhook_secret,
                    state_file=state_file,
                    seen_ids=seen_ids,
                )
                if published:
                    logger.info("published %d new demo record(s)", published)
            except httpx.HTTPStatusError as exc:
                response_status = exc.response.status_code
                if response_status == 429 or response_status >= 500:
                    logger.error(
                        "webhook returned HTTP %d; pending records will be retried",
                        response_status,
                    )
                else:
                    raise RuntimeError(
                        f"webhook rejected delivery with HTTP {response_status}"
                    ) from None
            except httpx.RequestError as exc:
                logger.error(
                    "webhook delivery failed (%s); pending records will be retried",
                    type(exc).__name__,
                )

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=interval_seconds
                )
            except TimeoutError:
                pass

    logger.info("demo watcher stopped")


async def run_generated_demo_watcher(
    *,
    webhook_url: str,
    webhook_secret: str,
    interval_seconds: int,
    stop_event: asyncio.Event,
) -> None:
    if interval_seconds < 1:
        raise SystemExit("interval_seconds must be at least 1")
    parsed_url = urlsplit(webhook_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise SystemExit("webhook URL must be an absolute HTTP or HTTPS URL")
    if not webhook_secret:
        raise SystemExit("webhook secret must not be empty")

    state_file = _generated_state_file_for(webhook_url)
    state = await asyncio.to_thread(_load_generated_state, state_file)
    logger.info("generated vehicle demo watcher started")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        while not stop_event.is_set():
            try:
                published = await publish_generated_events(
                    client=client,
                    webhook_url=webhook_url,
                    webhook_secret=webhook_secret,
                    state_file=state_file,
                    state=state,
                )
                if published:
                    logger.info(
                        "published %d vehicle demo event(s)", published
                    )
            except httpx.HTTPStatusError as exc:
                response_status = exc.response.status_code
                if response_status == 429 or response_status >= 500:
                    logger.error(
                        "webhook returned HTTP %d; pending events will be retried",
                        response_status,
                    )
                else:
                    raise RuntimeError(
                        f"webhook rejected delivery with HTTP {response_status}"
                    ) from None
            except httpx.RequestError as exc:
                logger.error(
                    "webhook delivery failed (%s); pending events will be retried",
                    type(exc).__name__,
                )

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=interval_seconds
                )
            except TimeoutError:
                pass

    logger.info("generated vehicle demo watcher stopped")


async def main_async(
    *,
    mode: str,
    source_file: Path | None,
    webhook_url: str,
    webhook_secret: str,
    interval_seconds: int,
) -> None:
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
            pass

    if mode == "generated":
        await run_generated_demo_watcher(
            webhook_url=webhook_url,
            webhook_secret=webhook_secret,
            interval_seconds=interval_seconds,
            stop_event=stop_event,
        )
    elif mode == "csv":
        if source_file is None:
            raise SystemExit("--source-file is required when --mode csv")
        await run_demo_watcher(
            source_file=source_file,
            webhook_url=webhook_url,
            webhook_secret=webhook_secret,
            interval_seconds=interval_seconds,
            stop_event=stop_event,
        )
    else:
        raise SystemExit(f"unsupported demo watcher mode: {mode}")
