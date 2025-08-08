{%- macro mcr_dynamic_limit(rows_limit=1000) -%}

    {%- if 'staging' in target.schema and not var('ignore_dev_limits') -%}
        {{ return('limit ' ~ rows_limit) }}
    {%- endif -%}

{%- endmacro -%}
