import asyncio
import hashlib
import hmac
import json
from pathlib import Path

from sansa_agent.watcher import (
    _load_generated_state,
    _load_seen_ids,
    publish_generated_events,
    publish_new_rows,
)


class _Response:
    status_code = 202


class _WebhookClient:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    async def post(self, url: str, *, content: bytes, headers: dict) -> _Response:
        self.requests.append(
            {"url": url, "content": content, "headers": headers}
        )
        return _Response()


def test_publishes_csv_rows_as_signed_webhook_events_and_remembers_them(
    tmp_path: Path,
):
    source_file = tmp_path / "orders.csv"
    source_file.write_text(
        "supplier,quantity\nNorthwind,12\nContoso,8\n",
        encoding="utf-8",
    )
    state_file = tmp_path / "seen.json"
    client = _WebhookClient()
    secret = "test-signing-secret"
    url = "http://localhost/api/v1/webhooks/test-token"
    seen_ids: set[str] = set()

    published = asyncio.run(
        publish_new_rows(
            client=client,
            source_file=source_file,
            webhook_url=url,
            webhook_secret=secret,
            state_file=state_file,
            seen_ids=seen_ids,
        )
    )

    assert published == 2
    assert len(client.requests) == 2
    first = client.requests[0]
    expected_body = b'{"supplier":"Northwind","quantity":"12"}'
    assert first["url"] == url
    assert first["content"] == expected_body
    assert first["headers"]["Content-Type"] == "application/json"
    assert first["headers"]["X-Sansa-Signature"] == hmac.new(
        secret.encode(), expected_body, hashlib.sha256
    ).hexdigest()

    source_file.write_text(
        "supplier,quantity\nFabrikam,4\nNorthwind,12\nContoso,8\n",
        encoding="utf-8",
    )
    seen_ids = _load_seen_ids(state_file)
    published = asyncio.run(
        publish_new_rows(
            client=client,
            source_file=source_file,
            webhook_url=url,
            webhook_secret=secret,
            state_file=state_file,
            seen_ids=seen_ids,
        )
    )

    assert published == 1
    assert json.loads(client.requests[-1]["content"]) == {
        "supplier": "Fabrikam",
        "quantity": "4",
    }
    assert len(_load_seen_ids(state_file)) == 3


def test_generates_and_signs_vehicle_trip_and_fuel_events(tmp_path: Path):
    client = _WebhookClient()
    state_file = tmp_path / "generated.json"
    state = _load_generated_state(state_file)
    secret = "vehicle-demo-secret"
    url = "http://localhost/api/v1/webhooks/test-token"

    published = asyncio.run(
        publish_generated_events(
            client=client,
            webhook_url=url,
            webhook_secret=secret,
            state_file=state_file,
            state=state,
        )
    )

    assert published == 2
    trip, fuel = [json.loads(request["content"]) for request in client.requests]
    assert trip["event_type"] == "vehicle_trip"
    assert fuel["event_type"] == "fuel_purchase"
    assert trip["vehicle_id"] == fuel["vehicle_id"]
    assert trip["trip_id"] == fuel["trip_id"]
    assert trip["date"]
    assert trip["quantity"] > 0
    assert trip["distance_km"] > 0
    assert fuel["product"] == "Diesel fuel"
    assert fuel["supplier"]
    assert fuel["purchase_order"]
    assert fuel["order_date"]
    assert fuel["quantity"] > 0
    assert fuel["unit_price"] > 0
    assert fuel["amount"] > 0
    assert fuel["currency"] == "ETB"
    for request in client.requests:
        assert request["headers"]["X-Sansa-Signature"] == hmac.new(
            secret.encode(),
            request["content"],
            hashlib.sha256,
        ).hexdigest()

    saved_state = _load_generated_state(state_file)
    assert saved_state == {"sequence": 2, "pending": []}
