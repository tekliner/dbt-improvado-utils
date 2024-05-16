{%- macro mcr_dynamic_ref(relation, rows_limit=1000) -%}
    {%- if target.schema not in ('default', 'internal_analytics', 'internal_analytics_src') and model.config.materialized not in ('view','seed') -%}
        {{ return(ref(relation) ~ ' limit ' ~ rows_limit) }}
    {%- else -%}
        {{ ref(relation) }}
    {%- endif -%}
{%- endmacro -%}
