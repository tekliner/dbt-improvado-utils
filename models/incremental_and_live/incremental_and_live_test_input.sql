{{
  config(
    materialized = 'table',
    enabled      = var('enabled', false)
    )
}}

-- 40 days of hourly events, carrying the same moment both as DateTime and as Date:
-- incremental_and_live models are filtered on either column type in production

select
    toStartOfHour(now()) - toIntervalHour(number)           as event_datetime,
    toDate(toStartOfHour(now()) - toIntervalHour(number))   as event_date,
    toUInt32(number)                                        as event_id
from
    numbers(24 * 40)
