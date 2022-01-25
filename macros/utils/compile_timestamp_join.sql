{%- macro compile_timestamp_join(primary_event, secondary_event, secondary_alias, relative, timestamp) -%}
    {{ adapter.dispatch('compile_timestamp_join', 'thesis_dbt')(primary_event, secondary_event, secondary_alias, relative, timestamp) }}
{%- endmacro -%}


{%- macro default__compile_timestamp_join(primary_event, secondary_event, secondary_alias, relative, timestamp) -%}
{%- if timestamp is not none -%}
{%- if relative == 'after' -%}
{%- if timestamp == 'previous' -%}
{{secondary_alias}}.{{secondary_event}}_event_at >= coalesce(enriched.previous_{{primary_event}}_event_at, {{thesis_dbt.early_timestamp()}})
{%- elif timestamp == 'current' -%}
{{secondary_alias}}.{{secondary_event}}_event_at > coalesce(enriched.{{primary_event}}_event_at, {{thesis_dbt.early_timestamp()}})
{%- endif -%}
{%- elif relative == 'before' -%}
{%- if timestamp == 'next' -%}
{{secondary_alias}}.{{secondary_event}}_event_at <= coalesce(enriched.next_{{primary_event}}_event_at, {{thesis_dbt.current_timestamp()}})
{%- elif timestamp == 'current' -%}
{{secondary_alias}}.{{secondary_event}}_event_at < coalesce(enriched.{{primary_event}}_event_at, {{thesis_dbt.current_timestamp()}})
{%- endif -%}
{%- endif -%}
{%- endif -%}

{%- endmacro -%}
