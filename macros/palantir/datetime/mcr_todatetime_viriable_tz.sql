-- readme
    -- we have input sting: '2022-02-22 02:22:20' and timezone:'NewYerk/America' 
        -- than we can get output: '2022-02-22 07:22:20'
    -- why?
        -- it works with DST (sammer/winter time)
        -- it allow to use variable for timezone. clickhouse functions does not allow to use virable

{% macro mcr_todatetime_viriable_tz(datetime_in_local_time_zone_str,local_time_zone) %}
{# readme #}
    {# clickhouse functions (i.e. toDateTime() ) works only with constant time_zone. #}
    {# this why we need to create custom function that change constant to virable time zone #}

-- system time zones list
    {% set time_zones %} 
        SELECT * 
        from  system.time_zones
        {#   where time_zone='Africa/Abidjan' -- remove mny rows if we need debug mode #}
    {% endset %}
    {% set time_zones_list = run_query(time_zones) %}

-- create if clause line for every time zone
    {% if execute %}
        multiIf( {% for system_time_zone in time_zones_list.rows %}{#
            #}{{ local_time_zone }}='{{system_time_zone["time_zone"]}}',{# -- if variable equels to constant
            #}toDateTime({{ datetime_in_local_time_zone_str }},'{{system_time_zone["time_zone"]}}'),{# -- than change variable to constant #}
            {% endfor %}
        NULL
        )
    {% endif %}		
{% endmacro %} 
