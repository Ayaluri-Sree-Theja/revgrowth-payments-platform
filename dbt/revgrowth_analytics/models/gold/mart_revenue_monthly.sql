with invoices as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    plan_id,
    amount_usd
  from {{ ref('stg_billing_events') }}
  where event_name = 'invoice_created'
)

select
  month,
  plan_id,
  count(distinct customer_id) as paying_customers,
  sum(amount_usd) as revenue_usd,
  round(avg(amount_usd), 2) as arpu_usd
from invoices
group by 1,2
order by 1,2
