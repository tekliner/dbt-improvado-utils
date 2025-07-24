{{
  config(
    enabled = var('enabled', false)
    )
}}


select
    now() - toIntervalHour(number)                          as event_datetime,

    toUInt32(
        cityHash64(number)
    )                                                       as event_id,

    concat(
        'value_',
        toString(number * cityHash64(number))
    )                                                       as event_data
from
    numbers(1000)
