import uuid
from datetime import datetime, timezone
import pandas as pd

from scripts.ingest.insert_billing_events_batch import insert_billing_events_batch

BASE_DIR = "scripts/sdv/outputs"
CUSTOMERS_CSV = f"{BASE_DIR}/base_customers.csv"
SUBS_CSV = f"{BASE_DIR}/base_subscriptions.csv"
INVPAY_CSV = f"{BASE_DIR}/base_invoices_payments.csv"

EVENT_VERSION = 1
SOURCE_SYSTEM = "billing_system"
ENVIRONMENT = "prod"

# Toggle these as needed
RUN_SUBSCRIPTIONS = True
RUN_INVOICES = True

# Batch sizes (tune if needed)
SUB_BATCH_SIZE = 2000
INV_BATCH_SIZE = 5000


def make_event_id() -> str:
    return str(uuid.uuid4())


def generate_subscription_created_events(subs_df: pd.DataFrame, customers_df: pd.DataFrame):
    """
    Emits one subscription_created per subscription.
    """
    cust_seg = customers_df.set_index("customer_id")[["plan_id", "channel", "country"]].to_dict(orient="index")

    events = []
    for _, s in subs_df.iterrows():
        cid = s["customer_id"]
        seg = cust_seg.get(cid, {})

        event_ts = datetime.fromisoformat(str(s["start_date"])).replace(tzinfo=timezone.utc)

        payload = {
            "subscription_id": s["subscription_id"],
            "customer_id": cid,
            "plan_id": s["plan_id"],
            "start_date": str(s["start_date"]),
            "channel": seg.get("channel"),
            "country": seg.get("country"),
        }

        events.append({
            "event_id": make_event_id(),
            "event_ts": event_ts,
            "event_name": "subscription_created",
            "event_version": EVENT_VERSION,
            "source_system": SOURCE_SYSTEM,
            "environment": ENVIRONMENT,

            "customer_id": cid,
            "user_id": None,

            "subscription_id": s["subscription_id"],
            "invoice_id": None,

            "plan_id": s["plan_id"],
            "old_plan_id": None,
            "new_plan_id": None,

            "amount_usd": None,
            "raw_payload": payload,
        })

    return events


def generate_invoice_created_events(invpay_df: pd.DataFrame):
    """
    Emits one invoice_created per invoice row.
    """
    events = []
    for _, r in invpay_df.iterrows():
        event_ts = datetime.fromisoformat(str(r["invoice_date"])).replace(tzinfo=timezone.utc)

        payload = {
            "invoice_id": r["invoice_id"],
            "subscription_id": r["subscription_id"],
            "customer_id": r["customer_id"],
            "invoice_date": str(r["invoice_date"]),
            "amount_usd": float(r["amount_usd"]),
            "plan_id": r.get("plan_id"),
            "channel": r.get("channel"),
            "country": r.get("country"),
            "attempts": int(r.get("attempts", 1)),
            "final_status": None if pd.isna(r.get("final_status")) else r.get("final_status"),
            "failure_reason": None if pd.isna(r.get("failure_reason")) else r.get("failure_reason"),
            "refund_flag": int(r.get("refund_flag", 0)),
            "chargeback_flag": int(r.get("chargeback_flag", 0)),
        }

        events.append({
            "event_id": make_event_id(),
            "event_ts": event_ts,
            "event_name": "invoice_created",
            "event_version": EVENT_VERSION,
            "source_system": SOURCE_SYSTEM,
            "environment": ENVIRONMENT,

            "customer_id": r["customer_id"],
            "user_id": None,

            "subscription_id": r["subscription_id"],
            "invoice_id": r["invoice_id"],

            "plan_id": r.get("plan_id"),
            "old_plan_id": None,
            "new_plan_id": None,

            "amount_usd": float(r["amount_usd"]),
            "raw_payload": payload,
        })

    return events


def main():
    print("Loading base tables...")
    customers = pd.read_csv(CUSTOMERS_CSV)
    subs = pd.read_csv(SUBS_CSV)
    invpay = pd.read_csv(INVPAY_CSV)

    if RUN_SUBSCRIPTIONS:
        print("Generating subscription_created events...")
        sub_events = generate_subscription_created_events(subs, customers)
        print(f"Inserting subscription_created (batch): {len(sub_events):,}")
        insert_billing_events_batch(sub_events, page_size=SUB_BATCH_SIZE)
        print("✅ subscription_created batch done")

    if RUN_INVOICES:
        print("Generating invoice_created events...")
        inv_events = generate_invoice_created_events(invpay)
        print(f"Inserting invoice_created (batch): {len(inv_events):,}")
        insert_billing_events_batch(inv_events, page_size=INV_BATCH_SIZE)
        print("✅ invoice_created batch done")

    print("✅ Billing generator finished. Verify counts in Postgres.")


if __name__ == "__main__":
    main()
