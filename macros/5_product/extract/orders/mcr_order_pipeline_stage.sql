--Macros generate order pipeline stage based on dts_order_category
 -- More here https://improvado.quip.com/7ZyiA9TfvoQL
{% macro mcr_order_pipeline_stage() %}

    multiIf(
        dts_order_category in ['extract_using_dsas','extract_from_email_ch_job','extract_spread_sheet',
            'extract_from_s3','extract_from_email','extract_from_metatron'],                        'extract order',
        dts_order_category in ['load_order','load_order_v2'],                                           'load order',
        dts_order_category in ['download_dsas_job','watch_ftp_changes','dataprep'],                     'other job',
        'not categoried job'
    )                                                                           as order_pipeline_stage

{% endmacro %}
