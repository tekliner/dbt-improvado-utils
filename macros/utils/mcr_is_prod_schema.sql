{%- macro mcr_is_prod_schema() -%}
    {# Checks if the current schema is a production schema #}

    {%- set production_schema = (
            'default',
            'internal_analytics',
            'internal_analytics_src',
            'improvado_models',
            'improvado_models_superset'
    ) -%}

    {{ return(target.schema in production_schema) }}
{%- endmacro -%}
