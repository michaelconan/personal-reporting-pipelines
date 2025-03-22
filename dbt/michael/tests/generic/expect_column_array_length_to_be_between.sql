{% test expect_column_array_length_to_be_between(model, column_name, min_value, max_value) %}
    SELECT *
    FROM {{ model }}
    WHERE array_length({{ column_name }}) < {{ min_value }}
       OR array_length({{ column_name }}) > {{ max_value }}
{% endtest %}