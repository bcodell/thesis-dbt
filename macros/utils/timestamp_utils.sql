{# Copied from dbt-utils v0.8.0 with some minor formatting changes #}

{%- macro type_timestamp() -%}
  {{ return(adapter.dispatch('type_timestamp', 'thesis_dbt')()) }}
{%- endmacro -%}

{%- macro default__type_timestamp() -%}
    timestamp
{%- endmacro -%}

{%- macro snowflake__type_timestamp() -%}
    timestamp_ntz
{%- endmacro -%}



{%- macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', 'thesis_dbt')()) }}
{%- endmacro -%}

{%- macro default__current_timestamp() -%}
    current_timestamp::{{thesis_dbt.type_timestamp()}}
{%- endmacro -%}

{%- macro redshift__current_timestamp() -%}
    getdate()
{%- endmacro -%}

{%- macro bigquery__current_timestamp() -%}
    current_timestamp
{%- endmacro -%}



{%- macro early_timestamp() -%}
  {{ return(adapter.dispatch('early_timestamp', 'thesis_dbt')()) }}
{%- endmacro -%}

{%- macro default__early_timestamp() -%}
    cast('100-01-01' as {{thesis_dbt.type_timestamp()}})
{%- endmacro -%}
