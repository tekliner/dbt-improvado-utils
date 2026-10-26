{{
  config(
    materialized                = 'incremental_and_live',
    production_schema           = 'default',
    input_models                = 'incremental_and_live_test_input',
    input_timestamp_columns     = 'event_date',
    start_time                  = (modules.datetime.date.today() - modules.datetime.timedelta(days = 60)).isoformat(),
    output_session_end_column   = 'event_date',
    output_id_column            = 'event_id',
    time_unit_name              = 'day',
    interval_fluctuation        = 2,
    materialized_window         = 5,
    partition_by                = 'toYYYYMM(event_date)',
    order_by                    = 'event_date',
    life_section                = true,
    unfinished_section          = true,
    silence_mode                = true,
    enabled                     = var('enabled', false),
    )
}}

-- input window column is a Date
select * from {{ ref('incremental_and_live_test_input') }}
