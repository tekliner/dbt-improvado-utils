{%- macro mcr_dynamic_limit(rows_limit=1000) -%}

    {%- if not dbt_improvado_utils.mcr_is_prod_schema() and not var('ignore_dev_limits') -%}
        {{ return('limit ' ~ rows_limit) }}
    {%- endif -%}

{%- endmacro -%}
