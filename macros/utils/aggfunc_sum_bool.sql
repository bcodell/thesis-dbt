{%- macro aggfunc_sum_bool(column) -%}
    {{ adapter.dispatch('aggfunc_sum_bool', 'thesis_dbt')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_sum_bool(column) -%}
sum({{column}}::int)
{%- endmacro -%}

{%- macro bigquery__aggfunc_sum_bool(column) -%}
sum(cast({{column}} as int64))
{%- endmacro -%}
