{%- macro aggfunc_sumbool(column) -%}
    {{ adapter.dispatch('aggfunc_sumbool', 'thesis_dbt')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_sumbool(column) -%}
sum({{column}}::int)
{%- endmacro -%}

{%- macro bigquery__aggfunc_sumbool(column) -%}
sum(cast({{column}} as int64))
{%- endmacro -%}
