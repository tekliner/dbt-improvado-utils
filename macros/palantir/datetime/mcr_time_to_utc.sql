{% macro mcr_time_to_utc(time_in_local_time_zone_str,local_time_zone) %}{#

    #}{% set datetime_str %}{#
        #}toString(today()){#
            #}|| ' '{#
            #}|| {{ time_in_local_time_zone_str }}{#
    #}{% endset %}{#
    
    #}right({#
        #}toString({#
             #}{{ mcr_todatetime_viriable_tz( 
                datetime_str, 
                local_time_zone ) }}{#
        #}),{#
        #}8{# 8 symbols fot 12:34:56 + :: = 8 symbols of time string
    #}){# 

#}{% endmacro %} 
