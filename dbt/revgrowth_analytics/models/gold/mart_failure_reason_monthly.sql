{{ config(materialized='table') }}

with terminal_invoices as (

    -- Total terminal invoices per month (succeeded OR failed)
    select
        date_trunc('month', event_ts)::date as month_start,
        count(distinct invoice_id) as total_invoices
    from {{ ref('stg_payment_events') }}
    where event_name in ('payment_succeeded', 'payment_failed')
      and invoice_id is not null
    group by 1

),

failed_by_reason as (

    -- Failed invoices per month per reason
    select
        date_trunc('month', event_ts)::date as month_start,
        coalesce(nullif(trim(lower(raw_payload ->> 'failure_reason')), ''), 'unknown') as failure_reason,
        count(distinct invoice_id) as failed_invoices
    from {{ ref('stg_payment_events') }}
    where event_name = 'payment_failed'
      and invoice_id is not null
    group by 1, 2

),

final as (

    select
        f.month_start,
        f.failure_reason,
        f.failed_invoices,
        t.total_invoices,
        (f.failed_invoices::numeric / nullif(t.total_invoices, 0)) as failure_rate_pct
    from failed_by_reason f
    join terminal_invoices t
      on f.month_start = t.month_start

)

select *
from final
order by month_start, failure_reason
