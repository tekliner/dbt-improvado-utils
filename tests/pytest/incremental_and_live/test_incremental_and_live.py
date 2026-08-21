from os import environ

import clickhouse_connect
import pytest
from dbt.tests.util import run_dbt

from tests.pytest.constants import (
    INCREMENTAL_AND_LIVE_INPUT_MODEL,
    INCREMENTAL_AND_LIVE_TEST_MODELS,
    QUERY_COUNT_ROWS,
)

QUERY_COUNT_DISTINCT_IDS = """
    select
        count(distinct event_id) as ids_count
    from
        default.{table_name}
    """

# the window filter incremental_and_live injects around the input relation
QUERY_INPUT_WINDOW_FILTERS = """
    select
        query
    from
        system.query_log
    where
        type = 'QueryFinish'
        and query_kind = 'Insert'
        and query like '%`{input_model}`%'
        and query like '%`{output_model}\\_%'
        and query like '%between%'
    """


class TestIncrementalAndLive:
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
    def input_rows_count(self, ch_client):
        """Pretest setup fixture: build the input model once"""

        run_dbt(
            [
                'run',
                '--select',
                INCREMENTAL_AND_LIVE_INPUT_MODEL,
                '--vars',
                '{"enabled": true}',
            ]
        )

        rows = ch_client.query_df(
            QUERY_COUNT_ROWS.format(table_name=INCREMENTAL_AND_LIVE_INPUT_MODEL)
        )
        return int(rows['rows_count'][0])

    # tests definition
    @pytest.mark.parametrize('model, input_column', INCREMENTAL_AND_LIVE_TEST_MODELS)
    def test_full_build_matches_input(self, ch_client, input_rows_count, model, input_column):
        """
        A full build over a Date / DateTime input column must succeed and
        reproduce every input row exactly once
        """

        run_dbt(
            [
                'run',
                '--select',
                model,
                '--vars',
                '{"enabled": true}',
                '--full-refresh',
            ]
        )

        rows = ch_client.query_df(QUERY_COUNT_ROWS.format(table_name=model))
        ids = ch_client.query_df(QUERY_COUNT_DISTINCT_IDS.format(table_name=model))

        assert int(rows['rows_count'][0]) == input_rows_count
        assert int(ids['ids_count'][0]) == input_rows_count

    @pytest.mark.parametrize('model, input_column', INCREMENTAL_AND_LIVE_TEST_MODELS)
    def test_input_window_filter_is_applied(self, ch_client, input_rows_count, model, input_column):
        """
        The interval inserts must read the input relation through a window on
        the input column (otherwise every insert scans the whole input)
        """

        ch_client.command('system flush logs')

        inserts = ch_client.query_df(
            QUERY_INPUT_WINDOW_FILTERS.format(
                input_model=INCREMENTAL_AND_LIVE_INPUT_MODEL,
                output_model=model,
            )
        )

        assert len(inserts) > 0
        assert all(
            f'where {input_column} between' in query
            for query in inserts['query']
        )
