{#
    Resolves whether to use ref() or source() for a given table depending on the environment.

    In prod: always uses source().
    In dev on improvado_models_staging:
        1. Checks if the parent exists in any internal_analytics_dbt_staging_* schema → uses fully-qualified name.
        2. Else checks if the parent exists in the same schema → ref().
        3. Else falls back to source().
    In dev on other schemas (e.g. personal staging):
        1. Checks if the parent exists in the same schema → ref().
        2. Else falls back to source().

    Existence checks probe the parent's database object name. For models with an `alias` config
    (e.g. alias = '_' ~ this.identifier) pass it via `identifier` — otherwise the probes miss the
    object, the ref() branch is unreachable and the macro always falls back to source(). BI-9961.

    Args:
        src_yml    (str): source name as defined in sources.yml (used for source() fallback)
        table_name (str): model/table name to resolve
        identifier (str, optional): parent's database object name when it differs from table_name
                                    (the model's `alias`)
#}
{%- macro mcr_source_or_ref(src_yml, table_name, identifier=none) -%}
    {%- set parent_identifier = identifier if identifier else table_name -%}
    {%- set is_same_schema_parent_exists = adapter.get_relation(database=none, schema=target.schema, identifier=parent_identifier) -%}

    {%- if not dbt_improvado_utils.mcr_is_prod_schema() -%}
        {%- if target.schema == 'improvado_models_staging' -%}
            {# parent model may be on internal_analytics_dbt_staging_* when child is on improvado_models_staging #}
            {%- set different_schema_parent_query -%}
                select
                    format('`{}`.`{}`', database, name)
                from
                    system.tables
                where
                    database like 'internal_analytics_dbt_staging_%'
                    and name = '{{ parent_identifier }}'
            {%- endset -%}

            {%- set _result = run_query(different_schema_parent_query) -%}
            {%- set is_different_schema_parent_exists = _result[0][0] if _result and _result.rows | length > 0 else none -%}

            {%- if is_different_schema_parent_exists -%}
                {{ is_different_schema_parent_exists }}

            {%- elif is_same_schema_parent_exists -%}
                {{ ref(table_name) }}

            {%- else -%}
                {{ source(src_yml, table_name) }}

            {%- endif -%}

        {%- elif is_same_schema_parent_exists -%}
            {{ ref(table_name) }}
        {%- else -%}
            {{ source(src_yml, table_name) }}
        {%- endif -%}
    {%- else -%}
        {{ source(src_yml, table_name) }}
    {%- endif -%}
{%- endmacro -%}
