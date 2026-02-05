{{ config(materialized='table') }}

with invoice_created as (

    -- Source of truth for plan attribution
    select
        invoice_id,
        date_trunc('month', event_ts)::date as month,
        upper(plan_id) as plan_id
    from {{ ref('stg_billing_events') }}
    where event_name = 'invoice_created'
      and invoice_id is not null
      and plan_id is not null

),

terminal as (

    -- Terminal outcome per invoice (succeeded/failed)
    select
        invoice_id,
        max(case when event_name = 'payment_succeeded' then 1 else 0 end) as succeeded,
        max(case when event_name = 'payment_failed' then 1 else 0 end) as failed
    from {{ ref('stg_payment_events') }}
    where event_name in ('payment_succeeded', 'payment_failed')
      and invoice_id is not null
    group by 1

),

attempts as (

    -- Attempts per invoice (use attempt_number like your overall reliability mart)
    select
        invoice_id,
        max(attempt_number) as max_attempts
    from {{ ref('stg_payment_events') }}
    where event_name = 'payment_attempted'
      and invoice_id is not null
    group by 1

),

facts as (

    select
        i.month,
        i.plan_id,
        i.invoice_id,
        t.succeeded,
        t.failed,
        a.max_attempts
    from invoice_created i
    join terminal t
      on i.invoice_id = t.invoice_id      -- only invoices that reached a terminal outcome
    left join attempts a
      on i.invoice_id = a.invoice_id

)

select
    month,
    plan_id,
    count(*) as invoices,
    sum(succeeded) as succeeded_invoices,
    sum(failed) as failed_invoices,

    -- IMPORTANT: decimal in [0,1], not multiplied by 100
    (sum(failed)::numeric / nullif(count(*), 0)) as failure_rate_pct,

    round(avg(max_attempts)::numeric, 2) as avg_attempts
from facts
group by 1,2
order by 1,2
