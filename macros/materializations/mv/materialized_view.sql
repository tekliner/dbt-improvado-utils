{% macro create_materialized_view_as(relation, target, sql) -%}
    CREATE MATERIALIZED VIEW {{ relation }} TO {{ target }} AS (
        {{ sql }}
    )
{% endmacro %}



{% materialization custom_materialized_view, adapter='clickhouse' -%}
    -- set 'mv_allowed_env' to 'prod_dev' on model config to allow development materialized views
    {% set mv_allowed_env = config.get('mv_allowed_env', default='prod') %}
    {% set mv_allowed = False %}
    {% set is_prod_schema = dbt_improvado_utils.mcr_is_prod_schema() %}
    {% set target_table_exists, target_table = get_or_create_relation(database=this.database, schema=this.schema, identifier=this.identifier, type='table') -%}
    {% set existing_target_table = load_cached_relation(target_table) %}

    {% set prefix ='_mv_' %}
    {% set mv_identifier = prefix ~ this.identifier %}
    {% set target_matview = this.incorporate(path={"identifier": mv_identifier}) %}
    {% set existing_matview = load_cached_relation(target_matview) %}

    {% set tmp_relation = make_intermediate_relation(target_table) %}
    {% do drop_relation_if_exists(tmp_relation) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% do dbt_improvado_utils.create_temporary_empty_table(tmp_relation, sql) %}
    {{ to_drop.append(tmp_relation) }}

    {% if is_prod_schema and mv_allowed_env == 'prod' or mv_allowed_env == 'prod_dev' %}
        {% set mv_allowed = True %}
    {% endif %}

    {% if existing_matview is not none %}
        -- check schema consistency if matview exists
        {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_matview) %}
        {% if schema_changes_dict['schema_changed'] or existing_target_table is none %}
            {% set full_rebuild = True %}
        {% endif %}
        {% do log("MV exists, schema_changed=" ~ schema_changes_dict['schema_changed'] ~ ", mv_allowed=" ~ mv_allowed ~ ", existing_target_table=" ~ existing_target_table, True) %}

    {% else %}
        -- matview doesnt exist, full build checks
        {% if existing_target_table is not none %}
            -- target exists, check schema consistency
            {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_target_table) %}
            {% if schema_changes_dict['schema_changed'] %}
                -- target inconsistent, full rebuild
                {% set full_rebuild = True %}
            {% endif %}
            -- target consistent, create matview only
            {% set create_matview = True %}
            {% do log("Target exists, schema_changed=" ~ schema_changes_dict['schema_changed'] ~ ", mv_allowed=" ~ mv_allowed, True) %}

        {% else %}
            -- target doesnt exist, full rebuild
            {% set full_rebuild = True %}
            {% do log("Target doesn't exist, full build", True) %}
        {% endif %}

    {% endif %}

    {% do drop_relation_if_exists(tmp_relation) %}

    {% if full_rebuild %}
        {% do dbt_improvado_utils.materialize_table(target_table, sql) %}
    {% endif %}

    {% if full_rebuild or create_matview and mv_allowed %}
        {% if mv_allowed %}
            {% do dbt_improvado_utils.materialize_matview(target_matview, target_table, sql) %}
        {% else %}
            {% do log("MV creation skipped", True) %}
        {% endif %}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        -- cleanup
        {% set should_revoke = should_revoke(existing_target_table, full_refresh_mode=True) %}
        {% do apply_grants(target_table, grant_config, should_revoke=should_revoke) %}

        {% do persist_docs(target_table, model) %}

        -- `COMMIT` happens here
        {% do adapter.commit() %}

    {% else %}
        -- for dry run 
        {{ store_result('main', 'SKIP') }}
    {% endif %}

    {% for rel in to_drop %}
        {% do adapter.drop_relation(rel) %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_table, target_matview]}) }}

{%- endmaterialization %}
