with revenue as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    sum(amount_usd) as revenue_usd
  from {{ ref('stg_billing_events') }}
  where event_name = 'invoice_created'
  group by 1,2
),

pay_term as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    count(*) filter (where event_name = 'payment_succeeded') as succeeded_invoices,
    count(*) filter (where event_name = 'payment_failed') as failed_invoices
  from {{ ref('stg_payment_events') }}
  where event_name in ('payment_succeeded','payment_failed')
  group by 1,2
),

attempts as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    avg(attempt_number)::numeric(10,2) as avg_attempt_number
  from {{ ref('stg_payment_events') }}
  where event_name = 'payment_attempted'
  group by 1,2
),

eng as (
  select
    date_trunc('month', event_ts)::date as month,
    customer_id,
    count(*) filter (where event_name='session_started') as sessions,
    count(*) filter (where event_name='feature_used') as feature_actions,
    max(case when event_name='cancel_intent' then 1 else 0 end) as has_cancel_intent
  from {{ ref('stg_product_events') }}
  group by 1,2
)

select
  coalesce(r.month, e.month, p.month, a.month) as month,
  coalesce(r.customer_id, e.customer_id, p.customer_id, a.customer_id) as customer_id,

  coalesce(r.revenue_usd, 0) as revenue_usd,

  coalesce(p.succeeded_invoices, 0) as succeeded_invoices,
  coalesce(p.failed_invoices, 0) as failed_invoices,
  round(
    (coalesce(p.failed_invoices,0)::numeric / nullif(coalesce(p.succeeded_invoices,0) + coalesce(p.failed_invoices,0),0)) * 100
  ,2) as payment_failure_rate_pct,

  coalesce(a.avg_attempt_number, 1.0) as avg_attempt_number,

  coalesce(e.sessions, 0) as sessions,
  coalesce(e.feature_actions, 0) as feature_actions,
  coalesce(e.has_cancel_intent, 0) as has_cancel_intent

from revenue r
full outer join eng e
  on r.month = e.month and r.customer_id = e.customer_id
full outer join pay_term p
  on coalesce(r.month, e.month) = p.month
 and coalesce(r.customer_id, e.customer_id) = p.customer_id
full outer join attempts a
  on coalesce(r.month, e.month, p.month) = a.month
 and coalesce(r.customer_id, e.customer_id, p.customer_id) = a.customer_id
order by 1,2
