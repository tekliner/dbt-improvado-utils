{{
  config(
    materialized                = 'microbatch',
    production_schema           = 'default',
    output_datetime_column      = 'event_datetime',
    materialization_start_date  = var('materialization_start_date'),
    time_unit_name              = 'hour',
    batch_size                  = var('batch_size', default=24),
    overwrite_size              = var('overwrite_size', default=12),
    partition_by                = 'toYYYYMM(event_datetime)',
    order_by                    = 'event_datetime',
    )
}}


with
    microbatch_test_input as (
        --microbatch: event_datetime, 12
        select * from {{ ref('microbatch_test_input') }}
    )

select * from microbatch_test_input
