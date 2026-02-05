import pandas as pd

OUT_DIR = "scripts/sdv/outputs"

def main():
    c = pd.read_csv(f"{OUT_DIR}/base_customers.csv")
    u = pd.read_csv(f"{OUT_DIR}/base_users.csv")
    s = pd.read_csv(f"{OUT_DIR}/base_subscriptions.csv")
    p = pd.read_csv(f"{OUT_DIR}/base_invoices_payments.csv")

    print("Rows:")
    print("customers:", len(c))
    print("users:", len(u))
    print("subscriptions:", len(s))
    print("invoices/payments:", len(p))

    print("\nPlan mix (customers):")
    print(c["plan_id"].value_counts(normalize=True).round(3))

    print("\nChannel mix (customers):")
    print(c["channel"].value_counts(normalize=True).round(3))

    print("\nFailure rate (invoice final_status):")
    print((p["final_status"].eq("failed").mean()).round(3))

    print("\nTop failure reasons:")
    print(p.loc[p["final_status"].eq("failed"), "failure_reason"].value_counts().head(10))

    print("\nAvg amount by plan:")
    print(p.groupby("plan_id")["amount_usd"].mean().round(2))

if __name__ == "__main__":
    main()
