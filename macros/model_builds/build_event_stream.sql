{% macro build_event_stream(model_list) %}
{{ adapter.dispatch('build_event_stream', 'thesis_dbt')(model_list)}}
{% endmacro %}

{% macro default__build_event_stream(model_list) %}
{% for model in model_list %}
select *
from {{ ref(model) }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
{% endmacro %}
