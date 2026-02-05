import uuid
from datetime import datetime, timezone
from scripts.ingest.insert_product_event import insert_product_event

event = {
    "event_id": str(uuid.uuid4()),
    "event_ts": datetime.now(timezone.utc),
    "event_name": "user_signed_up",
    "event_version": 1,
    "source_system": "product_app",
    "environment": "prod",

    "customer_id": "CUST-00001",
    "user_id": "CUST-00001-U01",
    "session_id": None,

    "feature_name": None,
    "feature_action": None,

    "channel": "organic",
    "device": "web",
    "country": "US",

    "raw_payload": None,
}

print("Inserted rows:", insert_product_event(event))
print("Inserted rows again (should be 0):", insert_product_event(event))  # duplicate
