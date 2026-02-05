# Central place to control SDV generation scale + time horizon

TARGET_CUSTOMERS = 25_000
TARGET_USERS = 130_000  # approx; derived from customers * avg_users_per_customer

MONTHS_HISTORY = 12

# Distributions (tweak later if needed)
PLAN_MIX = {"BASIC": 0.55, "PRO": 0.35, "TEAM": 0.10}
CHANNEL_MIX = {"organic": 0.45, "paid": 0.35, "referral": 0.12, "partner": 0.08}
COUNTRY_MIX = {"US": 0.55, "IN": 0.20, "CA": 0.10, "UK": 0.10, "AU": 0.05}

# Plan pricing (USD/month)
PLAN_PRICE = {"BASIC": 29, "PRO": 99, "TEAM": 249}

# Payment failure base rates (will be modulated by country/channel/plan)
BASE_FAILURE_RATE = 0.08  # overall approx attempt failure rate
REFUND_RATE = 0.012
CHARGEBACK_RATE = 0.0025

# Retry behavior
MAX_ATTEMPTS = 4
RETRY_SUCCESS_PROB = 0.55  # conditional probability of later success after a failure
