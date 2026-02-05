from typing import Dict, Any, Iterable, List
import math

from psycopg2.extras import Json, execute_batch
from .db import get_conn

SQL = """
INSERT INTO bronze.product_events (
  event_id, event_ts, event_name, event_version, source_system, environment,
  customer_id, user_id, session_id,
  feature_name, feature_action,
  channel, device, country,
  raw_payload
) VALUES (
  %(event_id)s, %(event_ts)s, %(event_name)s, %(event_version)s, %(source_system)s, %(environment)s,
  %(customer_id)s, %(user_id)s, %(session_id)s,
  %(feature_name)s, %(feature_action)s,
  %(channel)s, %(device)s, %(country)s,
  %(raw_payload)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v

def _prep(e: Dict[str, Any]) -> Dict[str, Any]:
    e = dict(e)
    rp = e.get("raw_payload")
    if rp is not None:
        e["raw_payload"] = Json({k: _clean(v) for k, v in rp.items()})
    return e

def insert_product_events_batch(events: Iterable[Dict[str, Any]], page_size: int = 5000) -> int:
    rows: List[Dict[str, Any]] = [_prep(e) for e in events]
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, SQL, rows, page_size=page_size)
    return len(rows)
