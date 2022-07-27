{%- macro compile_aggfunc(column_name, aggfunc, event_name, table_alias) -%}
    {{ adapter.dispatch('compile_aggfunc', 'thesis_dbt')(column_name, aggfunc, event_name, table_alias) }}
{%- endmacro -%}


{%- macro default__compile_aggfunc(column_name, aggfunc, event_name, table_alias) -%}
{%- set alias_col = table_alias~'.'~column_name -%}
{%- if aggfunc in ['first_value', 'last_value'] -%}
{%- set aggfunc_map = {'first_value': 'min', 'last_value': 'max'} -%}
{%- set agg = aggfunc_map[aggfunc] -%}
{%- set event_col = event_name~'_event_at' -%}
ltrim(
            {{agg}}(
                cast({{table_alias}}.{{event_col}} as varchar)
                || cast({{alias_col}} as varchar)
            ),
            {{agg}}(cast({{table_alias}}.{{event_col}} as varchar))
        )
{%- elif aggfunc == 'notnull' -%}
max({{alias_col}} is not null)
{%- else -%}
{{aggfunc}}({{alias_col}})
{%- endif -%}

{%- endmacro -%}