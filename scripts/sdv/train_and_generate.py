import os
import uuid
import random
from datetime import datetime, timedelta, timezone
import pandas as pd

from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

from .config import (
    TARGET_CUSTOMERS, TARGET_USERS, MONTHS_HISTORY,
    PLAN_PRICE
)

OUT_DIR = "scripts/sdv/outputs"
SEED_CUSTOMERS = f"{OUT_DIR}/seed_customers.csv"
SEED_USERS = f"{OUT_DIR}/seed_users.csv"
SEED_SUBS = f"{OUT_DIR}/seed_subscriptions.csv"
SEED_INVPAY = f"{OUT_DIR}/seed_invoices_payments.csv"

def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)

def month_start(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)

def parse_date(s: str) -> datetime:
    # seed files store ISO dates (YYYY-MM-DD)
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def train_customer_synth(seed_customers: pd.DataFrame) -> GaussianCopulaSynthesizer:
    # Train SDV to learn realistic correlations across customer attributes
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed_customers)

    synth = GaussianCopulaSynthesizer(metadata)
    synth.fit(seed_customers)
    return synth

def train_invoice_synth(seed_joined: pd.DataFrame) -> GaussianCopulaSynthesizer:
    # Learn invoice/payment outcome patterns (attempts/status/failure/refund/chargeback)
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed_joined)

    synth = GaussianCopulaSynthesizer(metadata)
    synth.fit(seed_joined)
    return synth

def build_base_customers(customer_synth: GaussianCopulaSynthesizer) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    start = month_start(now - timedelta(days=30 * MONTHS_HISTORY))

    df = customer_synth.sample(TARGET_CUSTOMERS)

    # Enforce clean ids + coherent time window
    df = df.copy()
    df["customer_id"] = [f"CUST-{i+1:06d}" for i in range(TARGET_CUSTOMERS)]

    # signup_date: clamp into last MONTHS_HISTORY months
    # SDV may generate strings; normalize to date strings
    signup_dates = []
    for s in df["signup_date"].astype(str).tolist():
        try:
            d = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except Exception:
            d = start + timedelta(days=random.randint(0, 30 * MONTHS_HISTORY - 15))
        if d < start:
            d = start + timedelta(days=random.randint(0, 10))
        if d > now:
            d = now - timedelta(days=random.randint(0, 10))
        signup_dates.append(d.date().isoformat())
    df["signup_date"] = signup_dates

    # Clean numeric fields
    df["team_size"] = df["team_size"].fillna(3).astype(float).round().astype(int).clip(1, 200)
    df["engagement_score"] = df["engagement_score"].fillna(50).astype(float).round().astype(int).clip(0, 100)
    df["churn_propensity"] = df["churn_propensity"].fillna(0.25).astype(float).apply(lambda x: round(clamp(x, 0.02, 0.75), 4))

    # Ensure plan_id is one of our known plans
    df["plan_id"] = df["plan_id"].astype(str).str.upper().replace({"BASIC ": "BASIC", "PRO ": "PRO", "TEAM ": "TEAM"})
    df.loc[~df["plan_id"].isin(["BASIC", "PRO", "TEAM"]), "plan_id"] = "BASIC"

    # Keep only relevant columns (seed may have extra)
    keep = [
        "customer_id", "country", "channel", "signup_date", "plan_id",
        "team_size", "industry", "device_preference", "engagement_score", "churn_propensity"
    ]
    df = df[keep].copy()
    return df

def build_base_subscriptions(base_customers: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, c in base_customers.iterrows():
        signup_dt = parse_date(c["signup_date"])
        start_dt = signup_dt + timedelta(days=random.randint(0, 7))
        subscription_id = f"SUB-{uuid.uuid4().hex[:12]}"
        rows.append({
            "subscription_id": subscription_id,
            "customer_id": c["customer_id"],
            "plan_id": c["plan_id"],
            "start_date": start_dt.date().isoformat(),
            "status": "active"
        })
    return pd.DataFrame(rows)

def build_base_users(base_customers: pd.DataFrame, seed_users: pd.DataFrame) -> pd.DataFrame:
    # Learn user role proportions from seed (simple but realistic)
    role_probs = seed_users["user_role"].value_counts(normalize=True).to_dict()
    roles = list(role_probs.keys())
    weights = list(role_probs.values())

    rows = []
    total_users = 0

    # Users per customer correlated with team_size (and capped)
    for _, c in base_customers.iterrows():
        team_size = int(c["team_size"])
        # realistic: not every seat is a user; plus some variability
        users_count = max(1, int(random.gauss(team_size * 0.65, 2)))
        users_count = min(users_count, team_size + 5)
        # keep near target_users by scaling down later if we overshoot
        for u in range(users_count):
            user_id = f"{c['customer_id']}-U{u+1:03d}"
            signup_dt = parse_date(c["signup_date"])
            created_dt = signup_dt + timedelta(days=random.randint(0, 15))
            rows.append({
                "user_id": user_id,
                "customer_id": c["customer_id"],
                "user_role": random.choices(roles, weights=weights, k=1)[0],
                "created_date": created_dt.date().isoformat()
            })
            total_users += 1

    df = pd.DataFrame(rows)

    # If we overshoot hard, sample down to target while keeping per-customer distribution roughly
    if total_users > TARGET_USERS:
        df = df.sample(n=TARGET_USERS, random_state=42).sort_values(["customer_id", "user_id"]).reset_index(drop=True)

    return df

def build_base_invoices_payments(
    base_customers: pd.DataFrame,
    base_subscriptions: pd.DataFrame,
    invoice_synth: GaussianCopulaSynthesizer
) -> pd.DataFrame:
    """
    Fast path:
    1) Build invoice skeleton for all subscriptions across MONTHS_HISTORY.
    2) Sample SDV outcomes in one batch.
    3) Merge outcomes + enforce constraints.
    """
    now = datetime.now(timezone.utc)
    start_window = month_start(now - timedelta(days=30 * MONTHS_HISTORY))

    # Map customer segment fields
    cust_map = base_customers.set_index("customer_id").to_dict(orient="index")

    # 1) Build invoice skeleton
    skeleton_rows = []
    for _, s in base_subscriptions.iterrows():
        cid = s["customer_id"]
        seg = cust_map[cid]
        sub_start = parse_date(s["start_date"])
        first_month = month_start(sub_start)

        for m in range(MONTHS_HISTORY):
            inv_dt = month_start(first_month + timedelta(days=30 * m))
            if inv_dt < start_window or inv_dt > now:
                continue

            plan = seg["plan_id"]
            amount = float(PLAN_PRICE.get(plan, 29))

            skeleton_rows.append({
                "invoice_id": f"INV-{uuid.uuid4().hex[:12]}",
                "subscription_id": s["subscription_id"],
                "customer_id": cid,
                "invoice_date": inv_dt.date().isoformat(),
                "plan_id": plan,
                "country": seg["country"],
                "channel": seg["channel"],
                "amount_usd": amount,
            })

    skeleton = pd.DataFrame(skeleton_rows)
    n = len(skeleton)
    print(f"Invoice skeleton rows: {n:,}")

    # 2) Sample SDV outcomes in one batch
    # We only need these columns from the SDV seed model:
    outcomes = invoice_synth.sample(n)[["attempts", "final_status", "failure_reason", "refund_flag", "chargeback_flag"]].copy()

    # 3) Merge and enforce constraints
    df = pd.concat([skeleton.reset_index(drop=True), outcomes.reset_index(drop=True)], axis=1)

    # Clean attempts
    df["attempts"] = df["attempts"].fillna(1).astype(float).round().astype(int).clip(1, 4)

    # Clean status
    df["final_status"] = df["final_status"].astype(str).str.lower()
    df.loc[~df["final_status"].isin(["succeeded", "failed"]), "final_status"] = "succeeded"

    # Clean failure_reason
    df["failure_reason"] = df["failure_reason"].where(df["final_status"].eq("failed"), None)
    df["failure_reason"] = df["failure_reason"].fillna(
        random.choice(["insufficient_funds", "card_declined", "expired_card", "network_error", "unknown"])
    )

    # Refund/chargeback only if succeeded
    df["refund_flag"] = df["refund_flag"].fillna(0).astype(int)
    df["chargeback_flag"] = df["chargeback_flag"].fillna(0).astype(int)
    df.loc[df["final_status"].eq("failed"), ["refund_flag", "chargeback_flag"]] = 0

    # If succeeded, failure_reason should be null
    df.loc[df["final_status"].eq("succeeded"), "failure_reason"] = None

    return df

def main():
    random.seed(42)
    ensure_out_dir()

    seed_customers = pd.read_csv(SEED_CUSTOMERS)
    seed_users = pd.read_csv(SEED_USERS)
    seed_invpay = pd.read_csv(SEED_INVPAY)

    # Join invoice seed with customer seed to teach SDV segment patterns
    # (even if we later override segments, it helps SDV learn realistic combinations)
    seed_customers_min = seed_customers[["customer_id", "country", "channel", "plan_id"]].copy()
    seed_joined = seed_invpay.merge(seed_customers_min, on="customer_id", how="left")

    print("Training SDV customer synthesizer...")
    customer_synth = train_customer_synth(seed_customers)

    print("Training SDV invoice/payment outcome synthesizer...")
    invoice_synth = train_invoice_synth(seed_joined)

    print("Generating base_customers...")
    base_customers = build_base_customers(customer_synth)

    print("Generating base_subscriptions...")
    base_subs = build_base_subscriptions(base_customers)

    print("Generating base_users...")
    base_users = build_base_users(base_customers, seed_users)

    print("Generating base_invoices_payments (this may take a bit)...")
    base_invpay = build_base_invoices_payments(base_customers, base_subs, invoice_synth)

    # Write outputs
    base_customers.to_csv(f"{OUT_DIR}/base_customers.csv", index=False)
    base_users.to_csv(f"{OUT_DIR}/base_users.csv", index=False)
    base_subs.to_csv(f"{OUT_DIR}/base_subscriptions.csv", index=False)
    base_invpay.to_csv(f"{OUT_DIR}/base_invoices_payments.csv", index=False)

    print("✅ Base tables generated:")
    print(f"- {OUT_DIR}/base_customers.csv  ({len(base_customers):,} rows)")
    print(f"- {OUT_DIR}/base_users.csv      ({len(base_users):,} rows)")
    print(f"- {OUT_DIR}/base_subscriptions.csv ({len(base_subs):,} rows)")
    print(f"- {OUT_DIR}/base_invoices_payments.csv ({len(base_invpay):,} rows)")

if __name__ == "__main__":
    main()
