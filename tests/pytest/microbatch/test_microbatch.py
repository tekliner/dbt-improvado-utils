from datetime import datetime, timedelta
from os import environ

import clickhouse_connect
import pytest
from dbt.tests.util import run_dbt

from tests.pytest.constants import (
    MICROBATCH_INPUT_MODEL,
    MICROBATCH_TEST_MODEL,
)


@pytest.fixture(scope="session")
def ch_client():
    client = clickhouse_connect.get_client(
        host=environ['CLICKHOUSE_HOST'],
        port=environ['CLICKHOUSE_PORT'],
        user=environ['CLICKHOUSE_USER'],
        password=environ['CLICKHOUSE_PASSWORD'],
        database=environ['CLICKHOUSE_DATABASE'],
    )
    return client


@pytest.fixture(scope="session")
def setup_test_environment(ch_client):
    con = ch_client
    return con


def test_batching_24h(setup_test_environment):
    con = setup_test_environment

    result_query = """
        select
            count() as rows_count
        from
            default.{table_name}
    """

    run_dbt(
        [
            'run',
            '--select',
            f'+{MICROBATCH_TEST_MODEL}',
            '--vars',
            f'{{"materialization_start_date": "{(datetime.now() - timedelta(hours=1000)).strftime("%Y-%m-%d")}" }}',
        ]
    )

    expected_df = con.query_df(result_query.format(table_name=MICROBATCH_INPUT_MODEL))

    assert expected_df['rows_count'][0] == 1000
