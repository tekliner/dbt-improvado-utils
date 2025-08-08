{%- macro mcr_source_or_ref(src_yml, table_name) -%}
    {%- set is_parent_node_exists = adapter.get_relation(database=none, schema=target.schema, identifier=table_name) -%}

    {%- if not dbt_improvado_utils.mcr_is_prod_schema() and is_parent_node_exists -%}
        {{ ref(table_name) }}
    {%- else -%}
        {{ source(src_yml, table_name) }}
    {%- endif -%}
{%- endmacro -%}
