select plan_id
from analytics.mart_revenue_monthly
group by 1
having count(distinct revenue_usd) <= 1
