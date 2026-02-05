with sessions as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    count(*) as sessions
  from {{ ref('stg_product_events') }}
  where event_name = 'session_started'
  group by 1,2
),
features as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    count(*) as feature_actions
  from {{ ref('stg_product_events') }}
  where event_name = 'feature_used'
  group by 1,2
),
cancel_intent as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    max(1) as has_cancel_intent
  from {{ ref('stg_product_events') }}
  where event_name = 'cancel_intent'
  group by 1,2
)

select
  coalesce(s.month, f.month) as month,
  coalesce(s.customer_id, f.customer_id) as customer_id,
  coalesce(s.sessions, 0) as sessions,
  coalesce(f.feature_actions, 0) as feature_actions,
  coalesce(c.has_cancel_intent, 0) as has_cancel_intent
from sessions s
full outer join features f
  on s.month = f.month and s.customer_id = f.customer_id
left join cancel_intent c
  on coalesce(s.month, f.month) = c.month
 and coalesce(s.customer_id, f.customer_id) = c.customer_id
order by 1,2
