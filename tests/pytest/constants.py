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

QUERY_TIMESTAMP = """
    select
        min({timestamp_column}) as min_timestamp,
        max({timestamp_column}) as max_timestamp
    from
        default.{table_name}
    """
