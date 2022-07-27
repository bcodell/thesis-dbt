{%- macro parse_attribute(attribute_name, condition) -%}
    {{ adapter.dispatch('parse_attribute', 'thesis_dbt')(attribute_name, condition) }}
{%- endmacro -%}


{%- macro default__parse_attribute(attribute_name, condition) -%}
{%- if condition is none -%}
    {%- set condition_str = '' -%}
{%- else -%}
    {%- set condition_str = ' '~condition -%}
{%- endif -%}
{%- if attribute_name in ['event_id', 'event_at', var('customer_id')] -%}
{{attribute_name}}{{condition_str}}
{%- else -%}
json_extract_path_text(attributes, '{{attribute_name}}'){{condition_str}}
{%- endif -%}
{%- endmacro -%}