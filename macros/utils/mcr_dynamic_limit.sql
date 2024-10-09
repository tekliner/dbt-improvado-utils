{%- macro mcr_dynamic_limit(rows_limit=1000) -%}
    {%- if target.schema not in ('default', 'internal_analytics', 'internal_analytics_src') -%}
        {{ return('limit ' ~ rows_limit) }}
    {%- endif -%}
{%- endmacro -%}
