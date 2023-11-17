{% macro mcr_opp_stage_name_normalization(opp_stage_name) %}
    if ({{opp_stage_name}} in ['Closed Won','Close Won Current customer'],
        'Hayday (Closed/Won)', 
        {{opp_stage_name}})  
{% endmacro %}
