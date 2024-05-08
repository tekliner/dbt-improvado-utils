{%- macro mcr_dynamic_ref(relation, rows_limit=1000) -%}
    {%- if target.schema != 'internal_analytics' and model.config.materialized not in ('view','seed') -%}
        {{ return(ref(relation) ~ ' limit ' ~ rows_limit) }}
    {%- else -%}
        {{ ref(relation) }}
    {%- endif -%}
{%- endmacro -%}
