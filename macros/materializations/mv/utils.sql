{% macro create_temporary_empty_table(relation, sql) %}
    {% set wrapped_sql %}
        WITH
            __schema_template as (
                {{ sql }}
            )
        SELECT *
        FROM __schema_template
        WHERE 1=0
    {% endset %}

    {% call statement('tmp_relation_table') -%}
        {{ clickhouse__create_table_as(False, relation, wrapped_sql)  }}
    {%- endcall %}

    {{ return(relation) }}
{% endmacro %}


{% macro materialize_table(_this, sql) %}
    {%- set existing_relation = load_cached_relation(_this) -%}
    {%- set target_relation = _this.incorporate(type='table') -%}
    {%- set backup_relation = none -%}
    {%- set preexisting_backup_relation = none -%}
    {%- set preexisting_intermediate_relation = none -%}

    {% if existing_relation is not none %}
        {%- set backup_relation_type = existing_relation.type -%}
        {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
        {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
        {% if not existing_relation.can_exchange %}
            {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
            {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
        {% endif %}
    {% endif %}

    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}

    {% if backup_relation is none %}
        {{ log('Creating new relation ' + target_relation.name )}}
        -- There is not existing relation, so we can just create
        {% call statement('main') -%}
            {{ create_table_as(False, target_relation, sql) }}
        {%- endcall %}
    {% elif existing_relation.can_exchange %}
        -- We can do an atomic exchange, so no need for an intermediate
        {% call statement('main') -%}
            {{ create_table_as(False, backup_relation, sql) }}
        {%- endcall %}
        {% do exchange_tables_atomic(backup_relation, existing_relation) %}
    {% else %}
        -- We have to use an intermediate and rename accordingly
        {% call statement('main') -%}
            {{ create_table_as(False, intermediate_relation, sql) }}
        {%- endcall %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% endif %}

    {{ drop_relation_if_exists(backup_relation) }}

    {{ return({'relations': [target_relation]}) }}
{% endmacro %}


{% macro materialize_matview(_this, mv_target_relation, sql) %}
    {%- set existing_relation = load_cached_relation(_this) -%}
    {%- set target_relation = _this.incorporate(type='table') -%}
    {%- set backup_relation = none -%}
    {%- set preexisting_backup_relation = none -%}
    {%- set preexisting_intermediate_relation = none -%}

    {% if existing_relation is not none %}
        {%- set backup_relation_type = existing_relation.type -%}
        {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
        {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
        {% if not existing_relation.can_exchange %}
            {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
            {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
        {% endif %}
    {% endif %}

    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}

    {% if backup_relation is none %}
        {{ log('Creating new relation ' + target_relation.name )}}
        -- There is not existing relation, so we can just create
        {% call statement('main') -%}
            {{ dbt_improvado_utils.create_materialized_view_as(target_relation, mv_target_relation, sql) }}
        {%- endcall %}
    {% elif existing_relation.can_exchange %}
        -- We can do an atomic exchange, so no need for an intermediate
        {% call statement('main') -%}
            {{ dbt_improvado_utils.create_materialized_view_as(backup_relation, mv_target_relation, sql) }}
        {%- endcall %}
        {% do exchange_tables_atomic(backup_relation, existing_relation) %}
    {% else %}
        -- We have to use an intermediate and rename accordingly
        {% call statement('main') -%}
            {{ dbt_improvado_utils.create_materialized_view_as(intermediate_relation, mv_target_relation, sql) }}
        {%- endcall %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% endif %}

    {{ drop_relation_if_exists(backup_relation) }}

    {{ return({'relations': [target_relation]}) }}
{% endmacro %} 
