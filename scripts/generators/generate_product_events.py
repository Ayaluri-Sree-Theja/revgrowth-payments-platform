import uuid
import random
from datetime import datetime, timezone, timedelta
import pandas as pd

from scripts.ingest.insert_product_events_batch import insert_product_events_batch

BASE_DIR = "scripts/sdv/outputs"
CUSTOMERS_CSV = f"{BASE_DIR}/base_customers.csv"
USERS_CSV = f"{BASE_DIR}/base_users.csv"

EVENT_VERSION = 1
SOURCE_SYSTEM = "product_app"
ENVIRONMENT = "prod"

BATCH_SIZE = 5000

FEATURES = ["dashboard", "reports", "exports", "integrations", "settings"]

def make_event_id():
    return str(uuid.uuid4())

def to_ts(date_str: str, offset_days: int = 0):
    return datetime.fromisoformat(str(date_str)).replace(
        tzinfo=timezone.utc
    ) + timedelta(days=offset_days)

def main():
    print("Loading base tables...")
    customers = pd.read_csv(CUSTOMERS_CSV)
    users = pd.read_csv(USERS_CSV)

    cust_map = customers.set_index("customer_id").to_dict(orient="index")

    events = []

    print(f"Generating product events for users: {len(users):,}")

    for i, u in users.iterrows():
        cid = u["customer_id"]
        user_id = u["user_id"]
        cust = cust_map[cid]

        engagement = cust["engagement_score"]
        churn_p = cust["churn_propensity"]

        signup_ts = to_ts(cust["signup_date"])

        # Sessions per user depends on engagement
        session_count = max(2, int(random.gauss(engagement / 6, 2)))

        for s in range(session_count):
            session_id = f"SES-{uuid.uuid4().hex[:10]}"
            session_ts = signup_ts + timedelta(days=random.randint(0, 300))

            # session_started
            events.append({
                "event_id": make_event_id(),
                "event_ts": session_ts,
                "event_name": "session_started",
                "event_version": EVENT_VERSION,
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,

                "customer_id": cid,
                "user_id": user_id,
                "session_id": session_id,

                "feature_name": None,
                "feature_action": None,

                "channel": cust["channel"],
                "device": cust["device_preference"],
                "country": cust["country"],

                "raw_payload": {
                    "engagement_score": engagement
                }
            })

            # feature_used events inside session
            feature_uses = random.randint(1, max(1, int(engagement / 20)))
            for _ in range(feature_uses):
                events.append({
                    "event_id": make_event_id(),
                    "event_ts": session_ts + timedelta(minutes=random.randint(1, 45)),
                    "event_name": "feature_used",
                    "event_version": EVENT_VERSION,
                    "source_system": SOURCE_SYSTEM,
                    "environment": ENVIRONMENT,

                    "customer_id": cid,
                    "user_id": user_id,
                    "session_id": session_id,

                    "feature_name": random.choice(FEATURES),
                    "feature_action": "click",

                    "channel": cust["channel"],
                    "device": cust["device_preference"],
                    "country": cust["country"],

                    "raw_payload": {
                        "engagement_score": engagement
                    }
                })

        # Cancel intent (only if churn propensity high)
        if churn_p > 0.55 and random.random() < churn_p:
            events.append({
                "event_id": make_event_id(),
                "event_ts": signup_ts + timedelta(days=random.randint(60, 330)),
                "event_name": "cancel_intent",
                "event_version": EVENT_VERSION,
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,

                "customer_id": cid,
                "user_id": user_id,
                "session_id": None,

                "feature_name": "billing",
                "feature_action": "view_cancel",

                "channel": cust["channel"],
                "device": cust["device_preference"],
                "country": cust["country"],

                "raw_payload": {
                    "churn_propensity": churn_p
                }
            })

        if (i + 1) % 25000 == 0:
            print(f"...processed {i+1:,}/{len(users):,} users")

    print(f"Inserting product events (batch): {len(events):,}")
    insert_product_events_batch(events, page_size=BATCH_SIZE)
    print("✅ product events batch done")

if __name__ == "__main__":
    main()
