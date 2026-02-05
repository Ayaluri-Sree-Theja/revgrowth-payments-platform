with failed as (
  select
    customer_id,
    date_trunc('month', event_ts)::date as month,
    count(*) as failed_invoices
  from {{ ref('stg_payment_events') }}
  where event_name = 'payment_failed'
  group by 1,2
),

cancel as (
  select
    customer_id,
    date_trunc('month', event_ts)::date as month,
    count(*) as cancel_intents
  from {{ ref('stg_product_events') }}
  where event_name = 'cancel_intent'
  group by 1,2
)

select
  coalesce(f.month, c.month) as month,
  coalesce(f.customer_id, c.customer_id) as customer_id,
  coalesce(f.failed_invoices, 0) as failed_invoices,
  coalesce(c.cancel_intents, 0) as cancel_intents,
  case
    when coalesce(f.failed_invoices, 0) > 0 and coalesce(c.cancel_intents, 0) > 0 then 1
    else 0
  end as failure_and_cancel_same_month
from failed f
full outer join cancel c
  on f.customer_id = c.customer_id and f.month = c.month
order by 1,2
