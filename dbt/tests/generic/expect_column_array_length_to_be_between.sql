{% test expect_column_array_length_to_be_between(model, column_name, min_value, max_value) %}
    
    {% set array_length_sql %}
        {% if target.type == 'bigquery' %}
            array_length(json_extract_array({{ column_name }}))
        {% elif target.type == 'duckdb' %}
            json_array_length({{ column_name }})
        {% else %}
            {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
        {% endif %}
    {% endset %}
    
    select
        *
    from {{ model }}
    where {{ array_length_sql }} < {{ min_value }}
        or {{ array_length_sql }} > {{ max_value }}
        or {{ array_length_sql }} is null

{% endtest %}
