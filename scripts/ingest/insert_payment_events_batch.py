from typing import Dict, Any, Iterable, List
import math

from psycopg2.extras import Json, execute_batch
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

def _clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v

def _clean_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _clean_value(v) for k, v in d.items()}

def _prep(e: Dict[str, Any]) -> Dict[str, Any]:
    e = dict(e)
    rp = e.get("raw_payload")
    if rp is not None:
        e["raw_payload"] = Json(_clean_dict(rp))
    return e

def insert_payment_events_batch(events: Iterable[Dict[str, Any]], page_size: int = 5000) -> int:
    events_list: List[Dict[str, Any]] = [_prep(e) for e in events]
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, SQL, events_list, page_size=page_size)
    return len(events_list)
