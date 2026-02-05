with src as (
  select
    event_id,
    event_ts,
    event_name,
    event_version,
    source_system,
    environment,
    customer_id,
    subscription_id,
    invoice_id,
    plan_id,
    amount_usd,
    raw_payload
  from {{ source('bronze', 'billing_events') }}
)

select
  event_id,
  event_ts,
  event_name,
  event_version,
  source_system,
  environment,
  customer_id,
  subscription_id,
  invoice_id,
  upper(plan_id) as plan_id,
  amount_usd::numeric(12,2) as amount_usd,
  raw_payload
from src
where customer_id is not null
