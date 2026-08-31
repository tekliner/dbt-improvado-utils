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

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_INPUT_MODEL}',
                '--vars',
                '{"enabled": true}',
            ]
        )

        con = ch_client
        timestamps = con.query_df(
            QUERY_TIMESTAMP.format(
                table_name=MICROBATCH_INPUT_MODEL, timestamp_column='event_datetime'
            )
        )
        min_timestamp = timestamps['min_timestamp'][0]
        max_timestamp = timestamps['max_timestamp'][0]

        return {'min_timestamp': min_timestamp, 'max_timestamp': max_timestamp}

    def execute_test(self, ch_client, test_params):
        con = ch_client
        dbt_vars = {
            'materialization_start_date': test_params["materialization_start_date"],
            'enabled': True,
        }
        if test_params.get('batch_size'):
            dbt_vars['batch_size'] = test_params["batch_size"]

        query_condition = test_params['query_condition'] if test_params.get('query_condition') else ''

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f'{dbt_vars}',
                '--full-refresh',
            ]
        )

        actual_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL)
        )

        expected_result = con.query_df(
            QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL)
            + query_condition
        )

        assert expected_result['rows_count'][0] == actual_result['rows_count'][0]

    # tests definition
    def test_batching_1h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 1h batch size
        """

        max_timestamp = setup_test_environment['max_timestamp']
        offset_hours = 50
        materialization_start_date = (
            max_timestamp - timedelta(hours=offset_hours)
        ).strftime("%Y-%m-%d")
        query_condition = f"where event_datetime >= toDate('{max_timestamp}' - interval {offset_hours} hour)"

        test_params = {
            'materialization_start_date': materialization_start_date,
            'batch_size': 1,
            'query_condition': query_condition,
        }

        self.execute_test(ch_client, test_params)

    def test_stale_tmp_relation_is_swept_and_not_double_counted(self, ch_client, setup_test_environment):
        """
        A crashed prior run can leave its invocation-scoped tmp table behind (each run now
        gets its own uniquely-named tmp table so overlapping runs can't collide on it). The
        next run must sweep that leftover rather than let it accumulate, and must not read
        rows from it into the target.
        """

        con = ch_client
        min_timestamp = setup_test_environment['min_timestamp']
        materialization_start_date = min_timestamp.strftime("%Y-%m-%d")

        self.execute_test(ch_client, {'materialization_start_date': materialization_start_date})

        stale_tmp_table = f'{MICROBATCH_TEST_MODEL}__microbatch_tmp_deadbeefdeadbeefdeadbeefdeadbeef'
        con.command(
            f'create table default.{stale_tmp_table} engine = MergeTree order by tuple() '
            f'as select * from default.{MICROBATCH_TEST_MODEL} limit 5'
        )

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f"{{'materialization_start_date': '{materialization_start_date}', 'enabled': True}}",
            ]
        )

        remaining_stale = con.query_df(
            f"select name from system.tables where database = 'default' and name = '{stale_tmp_table}'"
        )
        assert remaining_stale.empty, 'stale tmp table from a previous run was not swept'

        actual_result = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))
        expected_result = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))
        assert expected_result['rows_count'][0] == actual_result['rows_count'][0], (
            'row count changed after a run swept a stale tmp table - rows leaked in from it'
        )

    def test_batching_8h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 8h batch size
        """

        max_timestamp = setup_test_environment['max_timestamp']
        offset_hours = 100
        materialization_start_date = (
            max_timestamp - timedelta(hours=offset_hours)
        ).strftime("%Y-%m-%d")
        query_condition = f"where event_datetime >= toDate('{max_timestamp}' - interval {offset_hours} hour)"

        test_params = {
            'materialization_start_date': materialization_start_date,
            'batch_size': 8,
            'query_condition': query_condition,
        }

        self.execute_test(ch_client, test_params)

    def test_batching_24h(self, ch_client, setup_test_environment):
        """
        Microbatch test with 24h batch size
        """

        min_timestamp = setup_test_environment['min_timestamp']
        materialization_start_date = min_timestamp.strftime("%Y-%m-%d")

        test_params = {
            'materialization_start_date': materialization_start_date,
        }

        self.execute_test(ch_client, test_params)
