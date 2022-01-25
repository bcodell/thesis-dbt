{%- macro parse_attribute(attribute_name) -%}
    {{ adapter.dispatch('parse_attribute', 'thesis_dbt')(attribute_name) }}
{%- endmacro -%}


{%- macro default__parse_attribute(attribute_name) -%}
{%- if attribute_name in ['event_id', 'event_at', var('customer_id')] -%}
{{attribute_name}}
{%- else -%}
json_extract_path_text(attributes, '{{attribute_name}}')
{%- endif -%}
{%- endmacro -%}