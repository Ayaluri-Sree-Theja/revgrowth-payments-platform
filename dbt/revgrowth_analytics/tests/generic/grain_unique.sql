{% test grain_unique(model, columns) %}

select
  {{ columns | join(" || '|' || ") }} as grain_key,
  count(*) as cnt
from {{ model }}
group by 1
having count(*) > 1

{% endtest %}
