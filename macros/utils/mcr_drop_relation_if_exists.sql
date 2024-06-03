{%- macro mcr_drop_relation_if_exists(relation) -%}
    {%- set relation_name = relation.identifier -%}
    {%- set relation_type = run_query('select engine from system.tables where name = ' ~ "'" ~ relation_name ~ "'") -%}

    {% if relation_type %}
        {%- do run_query('drop ' ~ relation_type[0][0] ~ ' if exists ' ~ relation_name) -%}
    {% endif %}
{%- endmacro -%}
