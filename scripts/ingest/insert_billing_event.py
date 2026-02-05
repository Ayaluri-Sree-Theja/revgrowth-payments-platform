from typing import Dict, Any
from psycopg2.extras import Json
from .db import get_conn

SQL = """
INSERT INTO bronze.billing_events (
  event_id, event_ts, event_name, event_version, source_system, environment,
  customer_id, user_id, subscription_id, invoice_id,
  plan_id, old_plan_id, new_plan_id, amount_usd, raw_payload
) VALUES (
  %(event_id)s, %(event_ts)s, %(event_name)s, %(event_version)s, %(source_system)s, %(environment)s,
  %(customer_id)s, %(user_id)s, %(subscription_id)s, %(invoice_id)s,
  %(plan_id)s, %(old_plan_id)s, %(new_plan_id)s, %(amount_usd)s, %(raw_payload)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

def insert_billing_event(event: Dict[str, Any]) -> int:
    event = dict(event)  # shallow copy
    if event.get("raw_payload") is not None:
        event["raw_payload"] = Json(event["raw_payload"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, event)
            return cur.rowcount
