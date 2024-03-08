{% test future_date_not_permitted(model, column_name) %}
select {{ column_name }}
from {{ model }}
where TRY_CAST({{ column_name }} AS DATE) > today()
{% endtest %}