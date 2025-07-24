from datetime import timedelta
from os import environ

import clickhouse_connect
import pytest
from dbt.tests.util import run_dbt

from tests.pytest.constants import (
    MICROBATCH_INPUT_MODEL,
    MICROBATCH_TEST_MODEL,
    QUERY_COUNT_ROWS,
    QUERY_TIMESTAMP,
)


class TestMicrobatch:
    @pytest.fixture(scope="class")
    def ch_client(self):
        """ClickHouse client setup fixture"""

        client = clickhouse_connect.get_client(
            host=environ['CLICKHOUSE_HOST'],
            port=environ['CLICKHOUSE_PORT'],
            user=environ['CLICKHOUSE_USER'],
            password=environ['CLICKHOUSE_PASSWORD'],
            database=environ['CLICKHOUSE_DATABASE'],
        )
        return client

    @pytest.fixture(scope="class")
    def setup_test_environment(self, ch_client):
        """Pretest setup fixture"""

        run_dbt(['run', '--select', f'+{MICROBATCH_INPUT_MODEL}'])

        con = ch_client
        timestamps = con.query_df(
            QUERY_TIMESTAMP.format(
                table_name=MICROBATCH_TEST_MODEL, timestamp_column='event_datetime'
            )
        )
        min_timestamp = timestamps['min_timestamp'][0]
        max_timestamp = timestamps['max_timestamp'][0]

        return {'min_timestamp': min_timestamp, 'max_timestamp': max_timestamp}

    def test_batching_1h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 1h batch size
        """

        con = ch_client
        max_timestamp = setup_test_environment['max_timestamp']
        offset_hours = 50

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f'''{{
                    "materialization_start_date": "{(max_timestamp - timedelta(hours=offset_hours)).strftime("%Y-%m-%d")}",
                    "batch_size": 1
                }}''',
                '--full-refresh',
            ]
        )

        actual_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL)
        )

        expected_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL)
            + f"where event_datetime >= toDate('{max_timestamp}' - interval {offset_hours} hour)"
        )

        assert expected_result['rows_count'][0] == actual_result['rows_count'][0]

    def test_batching_8h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 8h batch size
        """

        con = ch_client
        max_timestamp = setup_test_environment['max_timestamp']
        offset_hours = 100

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f'''{{
                    "materialization_start_date": "{(max_timestamp - timedelta(hours=offset_hours)).strftime("%Y-%m-%d")}",
                    "batch_size": 8
                }}''',
                '--full-refresh',
            ]
        )

        actual_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL)
        )

        expected_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL)
            + f"where event_datetime >= toDate('{max_timestamp}' - interval {offset_hours} hour)"
        )

        assert expected_result['rows_count'][0] == actual_result['rows_count'][0]

    def test_batching_24h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 24h batch size
        """

        con = ch_client
        min_timestamp = setup_test_environment['min_timestamp']

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f'{{"materialization_start_date": "{min_timestamp.strftime("%Y-%m-%d")}"}}',
                '--full-refresh',
            ]
        )

        actual_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL)
        )

        expected_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL)
        )

        assert expected_result['rows_count'][0] == actual_result['rows_count'][0]
