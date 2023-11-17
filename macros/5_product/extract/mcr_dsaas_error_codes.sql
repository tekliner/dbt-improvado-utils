{% macro mcr_dsaas_error_codes(error_message_text) %}

-- Query Error codes
    {% set extract_mapping_table_query %}
        SELECT *
        FROM 
        {% if target.schema == 'internal_analytics_src' %}
            {{ source('palantir_gsheets', 'src_gsheet_dsas_error_mapping') }}
        {% else %}
            {{ ref('stg_dsas_error_mapping') }}
        {% endif %}
    {% endset %}
    {% set mapping_table = run_query(extract_mapping_table_query) %}

    {% if execute %}
        case
        {% for r in mapping_table.rows if r["datasource_db_name"] %}
            when datasource_db_name = '{{ r["datasource_db_name"] }}'
            {%- if r["report_type"] -%}
                and report_type='{{ r["report_type"] }}'  
            {%- endif -%}
            {%- set error_message_regex = r["error_message_regex"] | replace("'", "''")  -%}
            and match({{error_message_text}},  '{{error_message_regex }}' ) then {{ r["error_code_2"] }}
        {% endfor %}
        end
    {% endif %}		

{% endmacro %}        