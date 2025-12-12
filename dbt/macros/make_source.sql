-- Adapter-aware macro to load the live or mock source based on target name 
-- Usage: {{ make_source('my_source', 'my_relation') }}
--
{% macro make_source(source_name, relation_name) -%}
    {% set ref_name = source_name ~ '__' ~ relation_name %}
    {% if target.name == 'dev' %}
        {{ ref(ref_name) }}
    {% else %}
        {{ source(source_name, relation_name) }}
    {% endif %}
{%- endmacro %}