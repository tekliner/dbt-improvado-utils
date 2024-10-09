{%- macro mcr_source_or_ref(src_yml, table_name) -%}
    {%- set is_parent_on_staging_exists = adapter.get_relation(database=none, schema=target.schema, identifier=table_name) -%}
    {%- set ref_relation = ref(table_name) -%}
    {%- set source_relation = source(src_yml, table_name) -%}

    {%- if target.schema.startswith('internal_analytics_dbt_staging') and is_parent_on_staging_exists -%}
        {{ ref_relation }}
    {%- else -%}
        {{ source_relation }}
    {%- endif -%}
{%- endmacro -%}
