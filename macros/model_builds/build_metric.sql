{% macro build_metric(event_stream) %}
{{ adapter.dispatch('build_metric', 'thesis_dbt')(event_stream)}}
{% endmacro %}

{% macro default__build_metric(event_stream) %}
{%- set is_metric = config.require('is_metric') -%}
{%- set event_name = config.require('event_name') -%}
{%- set attribute = config.require('attribute') -%}
{%- set aggfunc = config.require('aggfunc') -%}
{%- set after_timestamp = config.require('after_timestamp') -%}
{%- set before_timestamp = config.require('before_timestamp') -%}


{{"-- depends_on: "~ref(event_stream)}}
select 1

{%- endmacro -%}