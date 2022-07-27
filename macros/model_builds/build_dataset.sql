{% macro build_dataset(event_stream, primary_event, primary_event_attributes, metrics) %}
{{ adapter.dispatch('build_dataset', 'thesis_dbt')(event_stream, primary_event, primary_event_attributes, metrics) }}
{% endmacro %}

{% macro default__build_dataset(event_stream, primary_event, primary_event_attributes, metrics) %}
{% set standard_columns = [
    'event_id',
    var('customer_id'),
    'event_at'
]
%}

{%- set sql_graph = {
    'primary_event': primary_event,
    'primary_event_cte': 'primary__'~primary_event,
    'primary_event_attributes': [],
    'join_requirements': [],
    'secondary_events': {}
} -%}

{%- if primary_event_attributes is not none -%}
{%- for pe in primary_event_attributes -%}
{%- do sql_graph['primary_event_attributes'].append(
    {
        'attribute_name': pe,
        'parsed_attribute': thesis_dbt.parse_attribute(pe),
        'column_name': primary_event~'_'~pe
    }
)
-%}
{%- endfor -%}
{%- endif -%}

{% if execute %}
{% for metric in metrics %}
    {% for node in graph.nodes.values() | selectattr("name", "equalto", metric) %}
        {%- if node.name == metric -%}
            {%- set secondary_event = node.config.event_name -%}
            {%- set secondary_event_cte = 'secondary__'~secondary_event -%}
            {%- if secondary_event not in sql_graph['secondary_events'].keys() -%}
                {%- do sql_graph['secondary_events'].update({
                    secondary_event: {
                        'event_name': secondary_event,
                        'cte': secondary_event_cte,
                        'joins': {}
                    }
                }) -%}
            {%- endif -%}

            {%- set after_ts = node.config.after_timestamp -%}
            {%- set before_ts = node.config.before_timestamp -%}
            {%- do sql_graph['join_requirements'].append(after_ts) -%}
            {%- do sql_graph['join_requirements'].append(before_ts) -%}
            {%- set join_key = (after_ts, before_ts) -%}
            {%- set table_alias = secondary_event~'_'~after_ts~'_'~before_ts -%}
            {%- if join_key not in sql_graph['secondary_events'][secondary_event]['joins'].keys() -%}
                {%- do sql_graph['secondary_events'][secondary_event]['joins'].update({
                    join_key: {
                        'after_ts': after_ts,
                        'before_ts': before_ts,
                        'table_alias': table_alias,
                        'metrics': []
                    }
                }) -%}
            {%- endif -%}
            {%- set metric_name = node.name -%}
            {%- set aggfunc = node.config.aggfunc -%}
            {%- set attribute_name = node.config.attribute -%}
            {%- set condition = node.config.condition -%}
            {%- set parsed_attribute = thesis_dbt.parse_attribute(attribute_name, condition) -%}
            {%- set aggregation = thesis_dbt.compile_aggfunc(
                column_name=metric_name,
                aggfunc=aggfunc,
                event_name=secondary_event,
                table_alias=table_alias
            ) -%}
            {%- do sql_graph['secondary_events'][secondary_event]['joins'][join_key]['metrics'].append({
                'metric_name': metric_name,
                'aggfunc': aggfunc,
                'attribute_name': attribute_name,
                'parsed_attribute': parsed_attribute,
                'aggregation': aggregation
            }) -%}
        {%- endif -%}
    {% endfor -%}
{% endfor -%}
{% endif %}

{%- set primary_columns = [] %}
{%- set enriched_columns = [] %}
{%- set primary_event_name = sql_graph['primary_event'] %}


{%- for metric in metrics %}
{%- set metric_dependency = "-- depends_on: "~ref(metric) -%}
{%- endfor %}


with {{sql_graph['primary_event_cte']}} as (
    select
        {%- for sc in standard_columns -%}
        {% set primary_sc = sql_graph['primary_event']~'_'~sc %}
        {% if not loop.first %}, {% endif %}{{sc}} as {{primary_sc}}
        {%- do primary_columns.append(primary_sc) -%}
        {% endfor %}
        {%- for attr in sql_graph['primary_event_attributes'] %}
        , {{attr['parsed_attribute']}} as {{attr['column_name']}}
        {%- do primary_columns.append(attr['column_name']) -%}
        {%- endfor %}
    from {{ ref(event_stream) }}
    where event_name = '{{primary_event}}'
)
, enriched as (
    select
        {%- for col in primary_columns %}
        {% if not loop.first %}, {% endif %}t1.{{col}}
        {%- do enriched_columns.append(col) -%}
        {% endfor %}
        {% if 'previous' in sql_graph['join_requirements'] %}
        {%- set col_name = 'previous_'~primary_event~'_event_at' -%}
        , max(t2.{{primary_event}}_event_at) as {{col_name}}
        {%- do enriched_columns.append(col_name) -%}
        {%- endif %}
        {%- if 'next' in sql_graph['join_requirements'] %}
        {%- set col_name = 'next_'~primary_event~'_event_at' -%}
        , min(t3.{{primary_event}}_event_at) as {{col_name}}
        {%- do enriched_columns.append(col_name) -%}
        {%- endif %}
    from {{sql_graph['primary_event_cte']}} t1
    {% if 'previous' in sql_graph['join_requirements'] -%}
    left join {{sql_graph['primary_event_cte']}} t2
        on t1.{{primary_event}}_{{ var('customer_id') }} = t2.{{primary_event}}_{{ var('customer_id') }}
        and t1.{{primary_event}}_event_at > t2.{{primary_event}}_event_at
    {%- endif %}
    {%- if 'next' in sql_graph['join_requirements'] -%}
    left join {{sql_graph['primary_event_cte']}} t3
        on t1.{{primary_event}}_{{ var('customer_id') }} = t3.{{primary_event}}_{{ var('customer_id') }}
        and t1.{{primary_event}}_event_at < t3.{{primary_event}}_event_at
    {%- endif %}
    group by
        {%- for col in primary_columns %}
        {% if not loop.first -%}, {% endif -%}t1.{{col}}
        {%- endfor %}
)
{% for secondary_event in sql_graph['secondary_events'].keys() -%}
{%- set se = sql_graph['secondary_events'][secondary_event] -%}
, {{se['cte']}} as (
    select
        {% for sc in standard_columns -%}
        {%- if not loop.first -%}, {% endif -%}{{sc}} as {{secondary_event}}_{{sc}}
        {% endfor %}
        {%- for j in se['joins'].keys() -%}
        {%- set join_reqs = se['joins'][j] -%}
        {%- for sm in join_reqs['metrics'] -%}
        , {{sm['parsed_attribute']}} as {{sm['metric_name']}}
        {%- endfor -%}
        {% endfor %}
    from {{ ref(event_stream) }}
    where event_name = '{{secondary_event}}'
)
{% endfor -%}
, joined as (
    select
        {%- for ec in enriched_columns %}
        {% if not loop.first -%}, {% endif -%}enriched.{{ec}}
        {%- endfor %}
        {%- for secondary_event in sql_graph['secondary_events'].keys() -%}
        {%- set se = sql_graph['secondary_events'][secondary_event] -%}
        {%- for j in se['joins'].keys() -%}
        {%- set join_reqs = se['joins'][j] -%}
        {%- for sm in join_reqs['metrics'] %}
        , {{sm['aggregation']}} as {{sm['metric_name']}}
        {%- endfor -%}
        {%- endfor -%}
        {% endfor %}
    from enriched
    {% for secondary_event in sql_graph['secondary_events'].keys() -%}
    {%- set se = sql_graph['secondary_events'][secondary_event] -%}
    {% for j in se['joins'].keys() %}
    {%- set join_reqs = se['joins'][j] -%}
    {%- set alias = join_reqs['table_alias'] -%}
    left join {{se['cte']}} {{alias}}
        on enriched.{{sql_graph['primary_event']}}_{{ var('customer_id') }} = {{alias}}.{{secondary_event}}_{{ var('customer_id') }}
        {%- if join_reqs['after_ts'] is not none %}
        and {{thesis_dbt.compile_timestamp_join(
            primary_event=sql_graph['primary_event'],
            secondary_event=secondary_event,
            secondary_alias=alias,
            relative='after',
            timestamp=join_reqs['after_ts']
        )}}
        {%- endif %}
        {%- if join_reqs['before_ts'] is not none %}
        and {{thesis_dbt.compile_timestamp_join(
            primary_event=sql_graph['primary_event'],
            secondary_event=secondary_event,
            secondary_alias=alias,
            relative='before',
            timestamp=join_reqs['before_ts']
        )}}
        {%- endif %}
    {% endfor %}
    {%- endfor -%}
    group by
        {%- for ec in enriched_columns %}
        {% if not loop.first -%}, {% endif -%}enriched.{{ec}}
        {%- endfor %}
)
select *
from joined


{% endmacro %}
