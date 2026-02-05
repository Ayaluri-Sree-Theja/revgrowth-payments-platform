with src as (
  select
    event_id,
    event_ts,
    event_name,
    event_version,
    source_system,
    environment,
    customer_id,
    invoice_id,
    payment_id,
    attempt_number,
    amount_usd,
    status,
    failure_reason,
    raw_payload
  from {{ source('bronze', 'payment_events') }}
)

select
  event_id,
  event_ts,
  event_name,
  event_version,
  source_system,
  environment,
  customer_id,
  invoice_id,
  payment_id,
  attempt_number::int as attempt_number,
  amount_usd::numeric(12,2) as amount_usd,
  lower(status) as status,
  nullif(failure_reason, '') as failure_reason,
  raw_payload
from src
where invoice_id is not null
