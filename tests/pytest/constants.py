# dbt settings
MICROBATCH_INPUT_MODEL = 'microbatch_test_input'
MICROBATCH_TEST_MODEL = 'microbatch_test'

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
