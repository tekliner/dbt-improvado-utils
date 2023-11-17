{% macro mcr_datasource_name_to_dbname(datasource_name) %}
    multiIf(
             {{ datasource_name }}  in ('adwords_video', 'video_adwords'), 'youtube',
             {{ datasource_name }}= 'microsoft_dynamic_365', 'dynamic_365',
             {{ datasource_name }}= 'eventbrite', 'eventbrite_sales',
             {{ datasource_name }}= 'google_my_business', 'gmb',
             {{ datasource_name }}= 'google_dcs', 'dcs',
             {{ datasource_name }}= 'the_trade_desk_api', 'ttd_api',
             {{ datasource_name }}= 'teads', 'teads_api',
             {{ datasource_name }}= 'snapchat_ads', 'snapchat',
             {{ datasource_name }}= 'impact_radius', 'impact_radius_api',
             {{ datasource_name }}= 'google_sc', 'gsc',
             {{ datasource_name }}= 'google_adwords', 'adwords',
             {{ datasource_name }}= 'google_analytics', 'analytics',
             {{ datasource_name }}= 'oracle_eloqua_forms', 'oracle_forms',
             {{ datasource_name }}= 'google_dbm', 'dbm',
             {{ datasource_name }}= 'yandex_direct', 'yandex',
             {{ datasource_name }}= 'google_dcm', 'dcm',
             {{ datasource_name }}= 'google_dcm', 'dcm',
             {{ datasource_name }}= 'amazon_selling_partner', 'amazon_sp',
             {{ datasource_name }}
    )  
{% endmacro %}