with src as (
  select
    event_id,
    event_ts,
    event_name,
    event_version,
    source_system,
    environment,
    customer_id,
    user_id,
    session_id,
    feature_name,
    feature_action,
    channel,
    device,
    country,
    raw_payload
  from {{ source('bronze', 'product_events') }}
)

select
  event_id,
  event_ts,
  event_name,
  event_version,
  source_system,
  environment,
  customer_id,
  user_id,
  session_id,
  feature_name,
  feature_action,
  lower(channel) as channel,
  lower(device) as device,
  upper(country) as country,
  raw_payload
from src
where customer_id is not null
