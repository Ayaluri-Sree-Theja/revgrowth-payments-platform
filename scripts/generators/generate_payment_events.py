import uuid
import random
from datetime import datetime, timezone, timedelta
import pandas as pd

from scripts.ingest.insert_payment_events_batch import insert_payment_events_batch

BASE_DIR = "scripts/sdv/outputs"
INVPAY_CSV = f"{BASE_DIR}/base_invoices_payments.csv"

EVENT_VERSION = 1
SOURCE_SYSTEM = "payment_gateway"
ENVIRONMENT = "prod"

BATCH_SIZE = 5000

def make_event_id() -> str:
    return str(uuid.uuid4())

def to_ts(date_str: str) -> datetime:
    # invoice_date is YYYY-MM-DD; convert to UTC timestamp
    return datetime.fromisoformat(str(date_str)).replace(tzinfo=timezone.utc)

def main():
    print("Loading base_invoices_payments...")
    inv = pd.read_csv(INVPAY_CSV)

    events = []
    total_invoices = len(inv)

    print(f"Generating payment events for invoices: {total_invoices:,}")

    for i, r in inv.iterrows():
        invoice_id = r["invoice_id"]
        customer_id = r["customer_id"]
        amount = float(r["amount_usd"])
        attempts = int(r.get("attempts", 1))
        final_status = str(r.get("final_status", "succeeded")).lower()
        failure_reason = None if pd.isna(r.get("failure_reason")) else r.get("failure_reason")

        # base event timestamp: invoice date; attempts spread across a few days
        base_ts = to_ts(r["invoice_date"])

        payment_id = f"PAY-{uuid.uuid4().hex[:12]}"

        # Attempt events
        for a in range(1, attempts + 1):
            attempt_ts = base_ts + timedelta(hours=2 * a)  # simple spacing

            # Attempt-level status:
            # - if final succeeded: earlier attempts may fail if attempts > 1
            # - if final failed: all attempts fail
            if final_status == "failed":
                attempt_status = "failed"
                attempt_reason = failure_reason
            else:
                if attempts > 1 and a < attempts:
                    attempt_status = "failed"
                    attempt_reason = failure_reason or random.choice(
                        ["insufficient_funds", "card_declined", "network_error"]
                    )
                else:
                    attempt_status = "succeeded"
                    attempt_reason = None

            payload = {
                "invoice_id": invoice_id,
                "payment_id": payment_id,
                "attempt_number": a,
                "amount_usd": amount,
                "attempt_status": attempt_status,
                "failure_reason": attempt_reason,
            }

            events.append({
                "event_id": make_event_id(),
                "event_ts": attempt_ts,
                "event_name": "payment_attempted",
                "event_version": EVENT_VERSION,
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,

                "customer_id": customer_id,
                "user_id": None,

                "invoice_id": invoice_id,
                "payment_id": payment_id,
                "attempt_number": a,

                "amount_usd": amount,
                "status": attempt_status,
                "failure_reason": attempt_reason,
                "raw_payload": payload,
            })

        # Terminal event (succeeded or failed) at last attempt time
        terminal_ts = base_ts + timedelta(hours=2 * attempts)
        terminal_name = "payment_succeeded" if final_status == "succeeded" else "payment_failed"

        terminal_payload = {
            "invoice_id": invoice_id,
            "payment_id": payment_id,
            "attempts": attempts,
            "final_status": final_status,
            "failure_reason": failure_reason if final_status == "failed" else None,
            "amount_usd": amount,
        }

        events.append({
            "event_id": make_event_id(),
            "event_ts": terminal_ts,
            "event_name": terminal_name,
            "event_version": EVENT_VERSION,
            "source_system": SOURCE_SYSTEM,
            "environment": ENVIRONMENT,

            "customer_id": customer_id,
            "user_id": None,

            "invoice_id": invoice_id,
            "payment_id": payment_id,
            "attempt_number": attempts,

            "amount_usd": amount,
            "status": final_status,
            "failure_reason": failure_reason if final_status == "failed" else None,
            "raw_payload": terminal_payload,
        })

        # light progress every 25k invoices
        if (i + 1) % 25000 == 0:
            print(f"...processed {i+1:,}/{total_invoices:,} invoices")

    print(f"Inserting payment events (batch): {len(events):,}")
    insert_payment_events_batch(events, page_size=BATCH_SIZE)
    print("✅ payment events batch done")

    print("✅ Finished. Verify counts in Postgres.")

if __name__ == "__main__":
    main()
