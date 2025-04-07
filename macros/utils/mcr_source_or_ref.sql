{%- macro mcr_source_or_ref(src_yml, table_name) -%}
    {%- set is_parent_on_staging_exists = adapter.get_relation(database=none, schema=target.schema, identifier=table_name) -%}

    {%- if 'staging' in target.schema and is_parent_on_staging_exists -%}
        {{ ref(table_name) }}
    {%- else -%}
        {{ source(src_yml, table_name) }}
    {%- endif -%}
{%- endmacro -%}
