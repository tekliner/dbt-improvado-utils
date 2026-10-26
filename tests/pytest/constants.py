# dbt settings
MICROBATCH_INPUT_MODEL = 'microbatch_test_input'
MICROBATCH_TEST_MODEL = 'microbatch_test'

INCREMENTAL_AND_LIVE_INPUT_MODEL = 'incremental_and_live_test_input'
# (model, input window column) — the same input filtered on a Date and on a DateTime column
INCREMENTAL_AND_LIVE_TEST_MODELS = [
    ('incremental_and_live_test_date', 'event_date'),
    ('incremental_and_live_test_datetime', 'event_datetime'),
]

# queries
QUERY_COUNT_ROWS = """
    select
        count() as rows_count
    from
        default.{table_name}
    """

QUERY_LAST_INSERT_SETTINGS = """
    select
        Settings as settings
    from
        system.query_log
    where
        type = 'QueryFinish'
        and query_kind = 'Insert'
        and position(query, '{table_name}__microbatch_tmp') > 0
    order by
        event_time_microseconds desc
    limit 1
    """

QUERY_TIMESTAMP = """
    select
        min({timestamp_column}) as min_timestamp,
        max({timestamp_column}) as max_timestamp
    from
        default.{table_name}
    """
