from typing import Dict, Any
from psycopg2.extras import Json
from .db import get_conn

SQL = """
INSERT INTO bronze.product_events (
  event_id, event_ts, event_name, event_version, source_system, environment,
  customer_id, user_id, session_id, feature_name, feature_action,
  channel, device, country, raw_payload
) VALUES (
  %(event_id)s, %(event_ts)s, %(event_name)s, %(event_version)s, %(source_system)s, %(environment)s,
  %(customer_id)s, %(user_id)s, %(session_id)s, %(feature_name)s, %(feature_action)s,
  %(channel)s, %(device)s, %(country)s, %(raw_payload)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

def insert_product_event(event: Dict[str, Any]) -> int:
    event = dict(event)
    if event.get("raw_payload") is not None:
        event["raw_payload"] = Json(event["raw_payload"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, event)
            return cur.rowcount
