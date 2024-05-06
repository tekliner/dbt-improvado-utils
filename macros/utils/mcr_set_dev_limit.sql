{% macro mcr_set_dev_limit(number_of_rows=100) %}
    {% if target.schema.startswith('dev_') %}
        {{ return("limit " ~ number_of_rows) }}
    {% endif %}
{% endmacro %}
