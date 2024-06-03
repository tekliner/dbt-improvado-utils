{%- macro mcr_drop_relation_if_exists(relation) -%}
    {%- set relation_name = relation.identifier -%}
    {%- set relation_type_query %}
        select
            if(engine = 'Dictionary', engine, 'Table')
        from
            system.tables
        where
            name = '{{ relation_name }}'
    {%- endset -%}
    {% set relation_type = run_query(relation_type_query) %}

    {% if relation_type %}
        {%- do run_query('drop ' ~ relation_type[0][0] ~ ' if exists ' ~ relation_name) -%}
    {% endif %}
{%- endmacro -%}
