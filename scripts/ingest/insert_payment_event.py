from typing import Dict, Any
from psycopg2.extras import Json
from .db import get_conn

SQL = """
INSERT INTO bronze.payment_events (
  event_id, event_ts, event_name, event_version, source_system, environment,
  customer_id, user_id, invoice_id, payment_id, attempt_number,
  amount_usd, status, failure_reason, raw_payload
) VALUES (
  %(event_id)s, %(event_ts)s, %(event_name)s, %(event_version)s, %(source_system)s, %(environment)s,
  %(customer_id)s, %(user_id)s, %(invoice_id)s, %(payment_id)s, %(attempt_number)s,
  %(amount_usd)s, %(status)s, %(failure_reason)s, %(raw_payload)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

def insert_payment_event(event: Dict[str, Any]) -> int:
    event = dict(event)
    if event.get("raw_payload") is not None:
        event["raw_payload"] = Json(event["raw_payload"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, event)
            return cur.rowcount
