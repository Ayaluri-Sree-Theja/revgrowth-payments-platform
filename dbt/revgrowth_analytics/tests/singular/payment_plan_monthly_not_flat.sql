select plan_id
from analytics.mart_payment_reliability_plan_monthly
group by 1
having count(distinct failure_rate_pct) <= 1
