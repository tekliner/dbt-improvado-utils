{% macro regex_replace_schema(raw_sql, base_relation, target_relation) %}
    {% set re = modules.re %}

    {% set relation_pattern = base_relation.identifier %}
    {% set match = re.search(relation_pattern, raw_sql) %}
    {% if not match %}
        {% do exceptions.raise_compiler_error(raw_schema ~ ' table not found in raw sql for replace') %}
    {% endif %}

-- replacing model name in "create" statement and `schema`.`model_name` in "selest as" statement to schema.model_name
    {% set header_replacement = raw_sql.replace(match.group(), target_relation.schema + '.' + target_relation.identifier) %}
    {% set body_replacement = re.sub('(?:`)(\w+)(?:`\.`)(\w+)(?:`)', '\\1' + '.' + '\\2', header_replacement) %}

    {% do return(body_replacement) %}
{% endmacro %}


{% macro show_create_table(relation) %}
    {% call statement('show_create_table', fetch_result=True) %}
        SHOW CREATE TABLE {{ relation.schema }}.{{ relation.identifier }}
    {% endcall %}

    {% do return(load_result('show_create_table').table.columns['statement'][0]) %}
{% endmacro %}

-- previous name 'create_custom', now called 'create_custom'
{% materialization create_custom, adapter='clickhouse' %}
    {% set target_relation = this.incorporate(type='table') %}
    {% set existing_relation = load_cached_relation(target_relation) %}

    {% set intermediate_relation = make_intermediate_relation(target_relation) %}

    {% set backup_relation_type = 'table' if existing_relation is none else existing_relation.type %}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) %}

    {% set existing_intermediate_relation = load_cached_relation(intermediate_relation) %}
    {% if existing_intermediate_relation %}
        {% do dbt_improvado_utils.mcr_drop_relation_if_exists(existing_intermediate_relation) %}
    {% else %}
        {% do dbt_improvado_utils.mcr_drop_relation_if_exists(intermediate_relation) %}
    {% endif %}

    {% set existing_backup_relation = load_cached_relation(backup_relation) %}
    {% if existing_backup_relation %}
        {% do dbt_improvado_utils.mcr_drop_relation_if_exists(existing_backup_relation) %}
    {% else %}
        {% do dbt_improvado_utils.mcr_drop_relation_if_exists(backup_relation) %}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% set ddl_changed = False %}

    {% if existing_relation is none %}
        -- No existing dict, simply create a new one
        {% call statement('main') %}
            {{ sql }}
        {% endcall %}

    {% else %}
        {% set re = modules.re %}
        -- Strip whitespace to compare
        
        {% set old_sql = dbt_improvado_utils.show_create_table(existing_relation) %}
        {% set new_sql = dbt_improvado_utils.regex_replace_schema(sql, target_relation, target_relation) %}
        
        -- Remove 'CREATE relation IF' and 'AS SELECT' clause,comments since DDL doesn't store them
        {% set new_sql_create_clause_removed = re.sub('(?i)if not exists', '', new_sql) %}
        {% set new_sql_as_select_from_removed = re.sub('(?is)(?:empty as|as select) .*', '', new_sql_create_clause_removed) %}
        {% set new_sql_comments_removed = re.sub('(?im)((--|{#)+.+(#}|$))', '', new_sql_as_select_from_removed) %}

        --Removing credentials
        {% set user_creds_pattern = "(?i)user\s?'([^']+)'"%}
        {% set password_creds_pattern = "(?i)password\s?'([^']+)'"%}

        {% set old_sql_half_creds_clean = re.sub(user_creds_pattern, "'DB_USER'", old_sql) %}
        {% set old_sql_clean = re.sub(password_creds_pattern, "'DB_PASSWORD'", old_sql_half_creds_clean) %}
        
        {% set new_sql_half_creds_clean = re.sub(user_creds_pattern, "'DB_USER'", new_sql_comments_removed) %}
        {% set new_sql_clean = re.sub(password_creds_pattern, "'DB_PASSWORD'", new_sql_half_creds_clean) %}

         -- Strip whitespace to compare
        
        {% set old_sql_stripped = re.sub('\s+', ' ', old_sql_clean.strip()) %}
        {% set new_sql_stripped = re.sub('\s+', ' ', new_sql_clean.strip()) %}

        {% if old_sql_stripped != new_sql_stripped  %}
            {% do log("DDL changed for " ~ existing_relation, True) %}
            {% do log("OLD: " ~ old_sql_stripped, True) %}
            {% do log("NEW: " ~ new_sql_stripped, True) %}

            {% set ddl_changed = True %}
        {% endif %}

        {% if ddl_changed %}
            {% if existing_relation.can_exchange %}
                -- We can do an atomic exchange, so no need for an intermediate
                {% set build_sql = dbt_improvado_utils.regex_replace_schema(sql, target_relation, backup_relation) %}
                {% call statement('main') %}
                    {{ build_sql }}
                {% endcall %}
                {% do exchange_tables_atomic(backup_relation, existing_relation) %}

            {% else %}
                -- We have to use an intermediate and rename accordingly
                {% set build_sql = dbt_improvado_utils.regex_replace_schema(sql, target_relation, intermediate_relation) %}
                {% call statement('main') %}
                    {{ build_sql }}
                {% endcall %}

                -- We need to drop the backup relation before we can use its name and to avoid name collisions
                {{ adapter.drop_relation(backup_relation) }}
                {{ adapter.rename_relation(existing_relation, backup_relation) }}
                {{ adapter.rename_relation(intermediate_relation, target_relation) }}

                {{ to_drop.append(intermediate_relation) }}
            {% endif %}

            {{ to_drop.append(backup_relation) }}
        {% else %}
            {% set do_nothing = True %}
        {% endif %}

    {% endif %}

    {% for rel in to_drop %}
        {% set rel_with_type = load_cached_relation(rel) %}
        {% if rel_with_type %}
            adapter.drop_relation(rel_with_type)
        {% endif %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do persist_docs(target_relation, model) %}
    -- `COMMIT` happens here
    {% do adapter.commit() %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if do_nothing %}
        -- to avoid error when nothing gets done
        {% do log("ddl unchanged, skipping " ~ target_relation, True) %}
        {{ store_result('main', 'SKIP') }}
    {% endif %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
