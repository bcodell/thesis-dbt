{%- macro aggfunc_listagg(column) -%}
    {{ adapter.dispatch('aggfunc_listagg', 'thesis_dbt')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_listagg(column) -%}
listagg({{column}}, ', ')
{%- endmacro -%}

{%- macro snowflake__aggfunc_listagg(column) -%}
listagg({{column}}, ', ')
{%- endmacro -%}

{%- macro bigquery__aggfunc_listagg(column) -%}
string_agg({{column}}, ', ')
{%- endmacro -%}

{%- macro redshift__aggfunc_listagg(column) -%}
list_agg({{column}}, ', ')
{%- endmacro -%}

{%- macro postgres__aggfunc_listagg(column) -%}
string_agg({{column}}, ', ')
{%- endmacro -%}
