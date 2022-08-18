{%- macro parse_attribute(metric_node) -%}
    {{ adapter.dispatch('parse_attribute', 'thesis_dbt')(metric_node) }}
{%- endmacro -%}

{%- macro default__parse_attribute(metric_node) -%}
{%- set attribute_name = metric_node.config.get('attribute') -%}
{%- set condition = metric_node.config.get('condition', none) -%}
{%- if condition is none -%}
    {%- set condition_str = '' -%}
{%- else -%}
    {%- set condition_str = ' '~condition -%}
{%- endif -%}
{%- set backup_value = metric_node.config.get('backup_value', none) -%}
{%- if backup_value is none -%}
    {%- set coalesce_prefix = '' -%}
    {%- set coalesce_suffix = '' -%}
{%- else -%}
    {%- set coalesce_prefix = 'coalesce(' -%}
    {%- set coalesce_suffix = ', '~backup_value~')' -%}
{%- endif -%}
{%- if attribute_name in ['event_id', 'event_at', var('customer_id')] -%}
{{coalesce_prefix}}{{attribute_name}}{{condition_str}}{{coalesce_suffix}}
{%- else -%}
{{coalesce_prefix}}nullif(json_extract_path_text(attributes, '{{attribute_name}}'), ''){{condition_str}}{{coalesce_suffix}}
{%- endif -%}
{%- endmacro -%}

{%- macro snowflake__parse_attribute(metric_node) -%}
{%- set attribute_name = metric_node.config.get('attribute') -%}
{%- set condition = metric_node.config.get('condition', none) -%}
{%- if condition is none -%}
    {%- set condition_str = '' -%}
{%- else -%}
    {%- set condition_str = ' '~condition -%}
{%- endif -%}
{%- set backup_value = metric_node.config.get('backup_value', none) -%}
{%- if backup_value is none -%}
    {%- set coalesce_prefix = '' -%}
    {%- set coalesce_suffix = '' -%}
{%- else -%}
    {%- set coalesce_prefix = 'coalesce(' -%}
    {%- set coalesce_suffix = ', '~backup_value~')' -%}
{%- endif -%}
{%- if attribute_name in ['event_id', 'event_at', var('customer_id')] -%}
{{coalesce_prefix}}{{attribute_name}}{{condition_str}}{{coalesce_suffix}}
{%- else -%}
{{coalesce_prefix}}nullif(to_varchar(get_path(attributes, '{{attribute_name}}')), ''){{condition_str}}{{coalesce_suffix}}
{%- endif -%}
{%- endmacro -%}