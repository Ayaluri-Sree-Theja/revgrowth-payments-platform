{{ config(materialized='table') }}

with terminal as (
  select
    date_trunc('month', event_ts)::date as month,
    invoice_id,
    max(case when event_name = 'payment_succeeded' then 1 else 0 end) as succeeded,
    max(case when event_name = 'payment_failed' then 1 else 0 end) as failed
  from {{ ref('stg_payment_events') }}
  where event_name in ('payment_succeeded','payment_failed')
    and invoice_id is not null
  group by 1,2
),
attempts as (
  select
    invoice_id,
    max(attempt_number) as max_attempts
  from {{ ref('stg_payment_events') }}
  where event_name = 'payment_attempted'
    and invoice_id is not null
  group by 1
)

select
  t.month,
  count(*) as invoices,
  sum(t.succeeded) as succeeded_invoices,
  sum(t.failed) as failed_invoices,
  (sum(t.failed)::numeric / nullif(count(*),0)) as failure_rate_pct,
  round(avg(a.max_attempts)::numeric, 2) as avg_attempts
from terminal t
left join attempts a using(invoice_id)
group by 1
order by 1
