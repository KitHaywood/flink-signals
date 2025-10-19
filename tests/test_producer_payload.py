from datetime import datetime, timezone

from producer.run import prepare_payload


def test_prepare_payload_converts_fields():
    raw_message = {
        "product_id": "BTC-USD",
        "price": 41000.1234,
        "best_bid": 40999.9,
        "best_ask": 41000.5,
        "volume_24h": 123.45,
        "sequence": 42,
        "side": "buy",
        "event_time": "2024-06-01T12:00:00Z",
        "source": "coinbase",
    }

    payload = prepare_payload(raw_message)

    assert payload is not None
    assert payload["product_id"] == "BTC-USD"
    assert payload["price"] == raw_message["price"]
    assert payload["sequence"] == raw_message["sequence"]
    assert payload["event_time"] == "2024-06-01T12:00:00+00:00"


def test_prepare_payload_rejects_bad_data():
    malformed = {"product_id": "BTC-USD", "event_time": "invalid"}
    assert prepare_payload(malformed) is None
