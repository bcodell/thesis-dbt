{%- macro attributes_to_json(attributes_list) -%}
    {{ adapter.dispatch('attributes_to_json', 'thesis_dbt')(attributes_list) }}
{%- endmacro -%}


{%- macro default__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    json_build_object(
        {% for attr in attributes_list -%}
        '{{attr}}', {{attr}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro postgres__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    json_build_object(
        {% for attr in attributes_list -%}
        '{{attr}}', {{attr}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro redshift__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    '{' ||
        {% for attr in attributes_list -%}
        {% if not loop.first -%}', '{%- endif -%}'"{{attr}}": "' || decode(cast({{attr}} as {{thesis_dbt.type_string()}}), null, '', cast({{attr}} as {{thesis_dbt.type_string()}})){% if not loop.last %} ||{% endif %}
        {% endfor -%}
    || '}'
    {%- else -%}
    cast(null as varchar)
    {%- endif -%}
{%- endmacro -%}


{%- macro bigquery__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    to_json(struct(
        {% for attr in attributes_list -%}
        {{attr}} as {{attr}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    ))
    {%- else -%}
    null
    {%- endif -%}
{%- endmacro -%}


{%- macro snowflake__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    object_construct(
        {% for attr in attributes_list -%}
        '{{attr}}', {{attr}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    null::object
    {%- endif -%}
{%- endmacro -%}
