{%- macro mcr_dynamic_ref(relation) -%}
    {% set query_result = run_query('select count() from ' ~ relation) %}

    {%- if execute -%}
        {%- set dynamic_limit = ((query_result[0][0] | float) * 0.1) | int -%}
    {%- endif -%}

    {%- if target.schema != 'internal_analytics' and model.config.materialized not in ('view','seed') -%}
        {{ return(ref(relation) ~ ' limit ' ~ dynamic_limit) }}
    {%- else -%}
        {{ ref(relation) }}
    {%- endif -%}
{%- endmacro -%}
