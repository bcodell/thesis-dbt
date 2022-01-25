{%- macro compile_aggfunc(column_name, aggfunc, event_name, table_alias) -%}
    {{ adapter.dispatch('compile_aggfunc', 'thesis_dbt')(column_name, aggfunc, event_name, table_alias) }}
{%- endmacro -%}


{%- macro default__compile_aggfunc(column_name, aggfunc, event_name, table_alias) -%}
{%- if aggfunc in ['first_value', 'last_value'] -%}
{%- set aggfunc_map = {'first_value': 'min', 'last_value': 'max'} -%}
{%- set agg = aggfunc_map[aggfunc] -%}
{%- set event_col = event_name~'_event_at' -%}
ltrim(
            {{agg}}(
                cast({{table_alias}}.{{event_col}} as varchar)
                || cast({{table_alias}}.{{column_name}} as varchar)
            ),
            {{agg}}(cast({{table_alias}}.{{event_col}} as varchar))
        )
{%- else -%}
{{aggfunc}}({{table_alias}}.{{column_name}})
{%- endif -%}

{%- endmacro -%}
