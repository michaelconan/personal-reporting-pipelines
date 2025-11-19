-- Adapter-aware macro to load the live or mock source based on target name 
-- Usage: {{ make_source('my_source', 'my_relation', 'dev') }}
--
{% macro make_source(source, relation, target_name) -%}
    {% set source_name = source ~ '__' ~ relation %}
    {% if target_name == 'dev' %}
        {{ ref(source_name) }}
    {% else %}
        {{ source(source, relation) }}
    {% endif %}
{%- endmacro %}