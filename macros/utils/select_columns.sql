{%- macro select_columns(column_list, indent_spaces=8) -%}
    {{ adapter.dispatch('select_columns', 'thesis_dbt')(column_list, indent_spaces=8) }}
{%- endmacro -%}


{%- macro default__select_columns(column_list, indent_spaces=8) -%}
{% for column in column_list -%}
{%- if not loop.first -%}{% for s in range(indent_spaces) %} {% endfor %}{%- endif -%}, {{column}}
{% endfor %}
{%- endmacro -%}
