import random
import uuid
from datetime import datetime, timedelta, timezone
import pandas as pd

from .config import PLAN_MIX, CHANNEL_MIX, COUNTRY_MIX, PLAN_PRICE

def weighted_choice(d: dict):
    keys = list(d.keys())
    weights = list(d.values())
    return random.choices(keys, weights=weights, k=1)[0]

def month_start(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)

def main():
    random.seed(42)
    now = datetime.now(timezone.utc)
    start = month_start(now - timedelta(days=365))

    n_customers_seed = 3000  # small seed, enough for SDV to learn patterns
    rows_customers = []
    rows_users = []
    rows_subs = []
    rows_invpay = []

    for i in range(n_customers_seed):
        customer_id = f"CUST-{i+1:06d}"
        country = weighted_choice(COUNTRY_MIX)
        channel = weighted_choice(CHANNEL_MIX)
        plan_id = weighted_choice(PLAN_MIX)

        # Correlated attributes
        team_size = (
            random.randint(1, 5) if plan_id == "BASIC"
            else random.randint(3, 20) if plan_id == "PRO"
            else random.randint(10, 80)
        )

        signup_date = start + timedelta(days=random.randint(0, 330))
        engagement_score = max(0, min(100,
            int(random.gauss(55, 18) + (10 if plan_id != "BASIC" else 0) + (5 if channel == "organic" else -3))
        ))

        # Churn propensity proxy (lower engagement => higher churn)
        churn_propensity = max(0.02, min(0.65,
            (0.45 - engagement_score / 200) + (0.06 if channel == "paid" else 0.0) + (0.05 if plan_id == "BASIC" else -0.03)
        ))

        rows_customers.append({
            "customer_id": customer_id,
            "country": country,
            "channel": channel,
            "signup_date": signup_date.date().isoformat(),
            "plan_id": plan_id,
            "team_size": team_size,
            "industry": random.choice(["fintech", "ecommerce", "health", "education", "media", "b2b_saas"]),
            "device_preference": random.choice(["web", "ios", "android"]),
            "engagement_score": engagement_score,
            "churn_propensity": round(churn_propensity, 4)
        })

        # Users per customer correlated with team_size
        users_count = max(1, int(random.gauss(team_size * 0.7, 2)))
        users_count = min(users_count, team_size + 5)

        for u in range(users_count):
            user_id = f"{customer_id}-U{u+1:02d}"
            rows_users.append({
                "user_id": user_id,
                "customer_id": customer_id,
                "user_role": random.choice(["admin", "analyst", "member"]),
                "created_date": (signup_date + timedelta(days=random.randint(0, 15))).date().isoformat()
            })

        subscription_id = f"SUB-{uuid.uuid4().hex[:12]}"
        sub_start = signup_date + timedelta(days=random.randint(0, 7))

        rows_subs.append({
            "subscription_id": subscription_id,
            "customer_id": customer_id,
            "plan_id": plan_id,
            "start_date": sub_start.date().isoformat(),
            "status": "active"
        })

        # Create 3–12 invoices depending on how early they signed up
        months_active = max(3, int((now - sub_start).days / 30))
        months_active = min(months_active, 12)

        for m in range(months_active):
            inv_date = month_start(sub_start) + timedelta(days=30*m)
            amount = PLAN_PRICE[plan_id]

            # Payment attempt realism:
            # higher churn_propensity => more failures
            base_fail = churn_propensity * 0.35
            # country modifier
            if country in ["IN"]:
                base_fail += 0.03
            if country in ["US", "CA"]:
                base_fail -= 0.01
            base_fail = max(0.02, min(0.25, base_fail))

            attempts = 1
            final_status = "succeeded"
            failure_reason = None

            if random.random() < base_fail:
                # first attempt fails
                attempts = random.randint(1, 4)
                final_status = "failed" if random.random() < 0.35 else "succeeded"
                failure_reason = random.choice(["insufficient_funds", "card_declined", "expired_card", "network_error", "unknown"])

            refund_flag = 1 if (final_status == "succeeded" and random.random() < 0.012) else 0
            chargeback_flag = 1 if (final_status == "succeeded" and random.random() < 0.0025) else 0

            rows_invpay.append({
                "invoice_id": f"INV-{uuid.uuid4().hex[:12]}",
                "subscription_id": subscription_id,
                "customer_id": customer_id,
                "invoice_date": inv_date.date().isoformat(),
                "amount_usd": float(amount),
                "attempts": int(attempts),
                "final_status": final_status,
                "failure_reason": failure_reason,
                "refund_flag": int(refund_flag),
                "chargeback_flag": int(chargeback_flag),
            })

    seed_dir = "scripts/sdv/outputs"
    pd.DataFrame(rows_customers).to_csv(f"{seed_dir}/seed_customers.csv", index=False)
    pd.DataFrame(rows_users).to_csv(f"{seed_dir}/seed_users.csv", index=False)
    pd.DataFrame(rows_subs).to_csv(f"{seed_dir}/seed_subscriptions.csv", index=False)
    pd.DataFrame(rows_invpay).to_csv(f"{seed_dir}/seed_invoices_payments.csv", index=False)

    print("Seed data created in scripts/sdv/outputs/")

if __name__ == "__main__":
    main()
