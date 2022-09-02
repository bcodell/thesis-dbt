{#
build_event: Compiles a final select statement in a standardized format so that all event models can be unioned together in the final event stream model. For each event transformation model, a final CTE should be compiled containing the primary customer id used in the event stream, a timestamp column representing when the timestamp occurred, and all of the static attributes associated with the event, then this macro should be applied after that CTE. No aggregations or transformations beyond basic selecting and column aliasing will occur in this macro.
    - cte: string; the name of the last CTE in the query containing all relevant columns to compile the event model
    - event_at_column: string; default is "event_at". the alias of the column the timestamp at which the event occurred. gets re-aliased to `event_at` within this macro. default is "event_at".
    - attributes: list; default is none. a list of column names corresponding to static attributes associated with the event.
#}

{% macro build_event(cte, attributes=none, event_at_column="event_at") %}
{{ adapter.dispatch('build_event', 'thesis_dbt')(cte, attributes, event_at_column) }}
{% endmacro %}


{% macro default__build_event(cte, attributes=none, event_at_column="event_at", event_stream=none) %}
{%- set customer_id = var('customer_id', var('thesis_dbt')[event_stream]['customer_id']) -%}

select
    {{ thesis_dbt.surrogate_key([customer_id, event_at_column, "'"~this.name~"'"]) }} as event_id
    , {{ customer_id }} as {{ customer_id }}
    , '{{ this.name }}' as event_name
    , {{event_at_column}} as event_at
    , {{ thesis_dbt.attributes_to_json(attributes) }} as attributes
from {{cte}}

{% endmacro %}
