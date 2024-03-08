{% test id_begins_with_tt(model, column_name) %}
select {{ column_name }}
from {{ model }}
where not starts_with({{ column_name }}, 'tt')
{% endtest %}