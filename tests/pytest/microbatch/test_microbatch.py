from datetime import timedelta
from os import environ

import clickhouse_connect
import pytest
from dbt.tests.util import run_dbt

from tests.pytest.constants import (
    MICROBATCH_INPUT_MODEL,
    MICROBATCH_TEST_MODEL,
    QUERY_COUNT_ROWS,
    QUERY_LAST_INSERT_SETTINGS,
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

        if test_params.get('batch_query_settings'):
            dbt_vars['batch_query_settings'] = test_params["batch_query_settings"]

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

    def test_batches_starting_after_now_are_skipped(self, ch_client, setup_test_environment):
        """
        parts_count has a floor of 2, so an incremental run whose window fits one batch
        (overwrite 12h < batch 24h here) used to issue a second INSERT whose window starts
        after now. It never matches a row but still runs the whole input query. Such batches
        must be skipped; the run must still issue its first batch and keep the row count.
        """

        con = ch_client
        min_timestamp = setup_test_environment['min_timestamp']
        materialization_start_date = min_timestamp.strftime("%Y-%m-%d")

        self.execute_test(ch_client, {'materialization_start_date': materialization_start_date})

        # counted as a delta, not by event_time: a now() taken from the client against the
        # server's event_time picked up the full-refresh INSERTs of the step above (10 instead
        # of 1) on some runs
        batch_inserts_query = f"""
            select count() as n
            from system.query_log
            where type = 'QueryFinish'
              and query_kind = 'Insert'
              and query like '%{MICROBATCH_TEST_MODEL}\\_\\_microbatch\\_tmp\\_%'
            """
        con.command('system flush logs')
        batch_inserts_before = con.query_df(batch_inserts_query)['n'][0]

        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f"{{'materialization_start_date': '{materialization_start_date}', 'enabled': True}}",
            ]
        )

        con.command('system flush logs')
        batch_inserts = con.query_df(batch_inserts_query)['n'][0] - batch_inserts_before
        assert batch_inserts == 1, (
            f'expected exactly one batch INSERT for a window that fits one batch, got {batch_inserts}'
        )

        actual_result = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))
        expected_result = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))
        assert expected_result['rows_count'][0] == actual_result['rows_count'][0]

    def test_max_datetime_reads_latest_partition_and_falls_back(self, ch_client, setup_test_environment):
        """
        The previous-run max datetime must come from the newest non-empty partition only, not
        from a whole-table max() (which turns into a full scan once a lightweight DELETE mask
        exists). When every row of the newest partition is deleted, the macro must fall back
        to the whole-table max() and the run must still refill the target.
        """

        con = ch_client
        min_timestamp = setup_test_environment['min_timestamp']
        materialization_start_date = min_timestamp.strftime("%Y-%m-%d")

        self.execute_test(ch_client, {'materialization_start_date': materialization_start_date})

        # the max() the macro issues against the target: one query pinned to a partition, or
        # the whole-table fallback (no _partition_id predicate)
        max_queries_query = f"""
            select
                countIf(query like '%\\_partition\\_id =%') as partition_pinned,
                countIf(query not like '%\\_partition\\_id =%') as whole_table
            from system.query_log
            where type = 'QueryFinish'
              and query_kind = 'Select'
              and query like '%toDateTime(max(event_datetime))%'
              and query like '%`default`.`{MICROBATCH_TEST_MODEL}`%'
              and query not like '%query\\_log%'
            """
        incremental_run = [
            'run',
            '--select',
            f'{MICROBATCH_TEST_MODEL}',
            '--vars',
            f"{{'materialization_start_date': '{materialization_start_date}', 'enabled': True}}",
        ]

        con.command('system flush logs')
        before = con.query_df(max_queries_query).iloc[0]
        run_dbt(incremental_run)
        con.command('system flush logs')
        after = con.query_df(max_queries_query).iloc[0]

        assert after['partition_pinned'] - before['partition_pinned'] == 1, 'expected one partition-pinned max() query'
        assert after['whole_table'] - before['whole_table'] == 0, 'whole-table max() must not run while the newest partition has rows'

        # delete every row of the newest partition with a lightweight DELETE: system.parts still
        # lists the partition with rows > 0, but the pinned max() sees nothing -> fallback
        latest_partition = con.query_df(
            f"""
            select max(partition_id) as partition_id
            from system.parts
            where active and rows > 0 and database = 'default' and table = '{MICROBATCH_TEST_MODEL}'
            """
        )['partition_id'][0]
        con.command(
            f"delete from default.{MICROBATCH_TEST_MODEL} where toYYYYMM(event_datetime) = {latest_partition}"
        )

        before = after
        run_dbt(incremental_run)
        con.command('system flush logs')
        after = con.query_df(max_queries_query).iloc[0]

        assert after['partition_pinned'] - before['partition_pinned'] == 1
        assert after['whole_table'] - before['whole_table'] == 1, 'expected the whole-table fallback after the newest partition was emptied'

        actual_rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))['rows_count'][0]
        expected_rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))['rows_count'][0]
        assert actual_rows == expected_rows, 'the emptied partition must be refilled'

        # the input is a view over now(), so its max moves between reads: check that the
        # target's max is back inside the partition that was emptied
        actual_max = con.query_df(
            QUERY_TIMESTAMP.format(table_name=MICROBATCH_TEST_MODEL, timestamp_column='event_datetime')
        )['max_timestamp'][0]
        assert int(actual_max.strftime('%Y%m')) == int(latest_partition), 'the emptied partition must hold rows again'

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

    def test_check_duplicate_parts_ignores_empty_parts(self, ch_client):
        """
        AI-1668. A DELETE mutation that removes every row of a part leaves a 0-row part behind.
        Since ClickHouse 26.3 such parts stay active for 30s-5min (dedicated cleanup thread),
        and all empty parts share one hash_of_all_files, so check_duplicate_parts used to treat
        them as duplicates and DROP PART them - racing the cleanup thread into a hard
        NO_SUCH_DATA_PART (code 232) that aborted the whole materialization.
        Empty parts hold no rows: the check must skip them and leave the table untouched.
        remove_empty_parts=0 pins the empties in place so the test is deterministic on any
        ClickHouse version.
        """

        con = ch_client
        table = 'check_duplicate_parts_empty_parts_test'
        con.command(f'drop table if exists default.{table}')
        con.command(
            f'create table default.{table} (p UInt8, marker UInt8, v UInt64) '
            f'engine = MergeTree partition by p order by v settings remove_empty_parts = 0'
        )
        # three separate inserts -> three parts in one partition; two of them will be emptied
        con.command(f'insert into default.{table} values (1, 1, 1)')
        con.command(f'insert into default.{table} values (1, 1, 2)')
        con.command(f'insert into default.{table} values (1, 0, 3)')
        con.command(
            f'alter table default.{table} delete where marker = 1 settings mutations_sync = 2'
        )

        parts_query = (
            "select name, rows, hash_of_all_files from system.parts "
            f"where database = 'default' and table = '{table}' and active order by name"
        )
        before = con.query_df(parts_query)
        assert len(before) == 3, f'expected 3 active parts, got {before}'
        empties = before[before['rows'] == 0]
        assert len(empties) == 2, f'expected 2 empty parts, got {before}'
        assert empties['hash_of_all_files'].nunique() == 1, 'empty parts should share one hash'

        run_dbt(
            [
                'run-operation',
                'check_duplicate_parts',
                '--args',
                f"{{relation: {{schema: default, identifier: {table}}}, silence_mode: true}}",
            ]
        )

        after = con.query_df(parts_query)
        assert list(after['name']) == list(before['name']), (
            f'check_duplicate_parts dropped parts it must ignore: before={before} after={after}'
        )
        rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=table))
        assert rows['rows_count'][0] == 1

    def test_check_duplicate_parts_report_mode_keeps_copies(self, ch_client):
        """
        BI-10065. fct_product_domain_events' target held six pairs of parts with identical
        hash_of_all_files. copy_partition handed both copies to tmp, check_duplicate_parts dropped
        one of each, and check_untouched_rows_match then refused the REPLACE on every run because
        tmp held fewer old rows than target. action='report' must log the pairs and leave every
        part in place so the history goes back unchanged; the default action='drop' must still
        keep exactly one copy. An unknown action is rejected before anything is touched.
        """

        con = ch_client
        table = 'check_duplicate_parts_report_mode_test'
        con.command(f'drop table if exists default.{table}')
        con.command(
            f'create table default.{table} (event_datetime DateTime, v UInt64) '
            f'engine = MergeTree partition by toYYYYMM(event_datetime) order by v'
        )
        # two inserts with identical content -> two parts with the same hash_of_all_files
        for _ in range(2):
            con.command(
                f"insert into default.{table} values ('2026-10-06 20:00:00', 1), ('2026-10-06 21:00:00', 2)"
            )
        con.command(f"insert into default.{table} values ('2026-10-06 22:00:00', 3)")

        parts_query = (
            "select name, rows, hash_of_all_files from system.parts "
            f"where database = 'default' and table = '{table}' and active order by name"
        )
        before = con.query_df(parts_query)
        assert len(before) == 3, f'expected 3 active parts, got {before}'
        assert before['hash_of_all_files'].nunique() == 2, f'expected one duplicated hash, got {before}'

        def args(action):
            return (
                f"{{relation: {{schema: default, identifier: {table}}}, "
                f"silence_mode: true, action: {action}}}"
            )

        run_dbt(['run-operation', 'check_duplicate_parts', '--args', args('bogus')], expect_pass=False)
        run_dbt(['run-operation', 'check_duplicate_parts', '--args', args('report')])

        after_report = con.query_df(parts_query)
        assert list(after_report['name']) == list(before['name']), (
            f'report mode must not drop parts: before={before} after={after_report}'
        )
        rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=table))
        assert rows['rows_count'][0] == 5

        run_dbt(['run-operation', 'check_duplicate_parts', '--args', args('drop')])

        after_drop = con.query_df(parts_query)
        assert len(after_drop) == 2, f'drop mode must keep one copy: {after_drop}'
        assert after_drop['hash_of_all_files'].nunique() == 2
        rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=table))
        assert rows['rows_count'][0] == 3

    def test_identical_parts_in_target_are_healed_before_copy(self, ch_client, setup_test_environment):
        """
        BI-10065. A server restart right after a REPLACE PARTITION can bring the replaced parts
        back as active next to their replacements: byte-identical twins, every row twice. The
        next incremental run used to copy both into tmp, drop one there, and then refuse the
        REPLACE because tmp held fewer old rows than target - on every run until someone
        dropped the twins by hand. Now the default 'drop' mode removes the extra copies in
        target before the copy: the run passes and the row count matches the input again.
        """

        con = ch_client
        min_timestamp = setup_test_environment['min_timestamp']
        materialization_start_date = min_timestamp.strftime("%Y-%m-%d")
        dbt_vars = f"{{'materialization_start_date': '{materialization_start_date}', 'enabled': True}}"

        self.execute_test(ch_client, {'materialization_start_date': materialization_start_date})

        # the partition the next run copies: the one holding max(event_datetime) - overwrite_size
        partition_id = con.query_df(
            f"select toString(toYYYYMM(max(event_datetime) - interval 12 hour)) as p "
            f"from default.{MICROBATCH_TEST_MODEL}"
        )['p'][0]

        # twin every part of that partition: attach it into a helper table and back again. Merges
        # are stopped until the run: a background merge folding a part and its twin together
        # leaves the rows doubled in one part, with no twins left to heal
        con.command(f'system stop merges default.{MICROBATCH_TEST_MODEL}')
        helper = f'{MICROBATCH_TEST_MODEL}__twin_helper'
        con.command(f'drop table if exists default.{helper}')
        con.command(f'create table default.{helper} as default.{MICROBATCH_TEST_MODEL}')
        con.command(
            f"alter table default.{helper} attach partition id '{partition_id}' "
            f"from default.{MICROBATCH_TEST_MODEL}"
        )
        con.command(
            f"alter table default.{MICROBATCH_TEST_MODEL} attach partition id '{partition_id}' "
            f"from default.{helper}"
        )
        con.command(f'drop table default.{helper}')

        pairs_query = (
            "select count() as n from (select hash_of_all_files from system.parts "
            f"where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active and rows > 0 "
            "group by partition_id, hash_of_all_files having count() > 1)"
        )
        assert con.query_df(pairs_query)['n'][0] > 0, 'the setup did not create identical parts'
        input_rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))['rows_count'][0]
        doubled = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))['rows_count'][0]
        assert doubled > input_rows, 'the setup did not double any rows'

        try:
            run_dbt(['run', '--select', f'{MICROBATCH_TEST_MODEL}', '--vars', dbt_vars])
        finally:
            con.command('system start merges')

        assert con.query_df(pairs_query)['n'][0] == 0, 'identical parts survived the run'
        actual = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))['rows_count'][0]
        assert actual == input_rows, f'row count after healing: {actual}, input: {input_rows}'

    def test_check_untouched_rows_match(self, ch_client):
        """
        Last guard before REPLACE PARTITION: rows older than start_time are only copied into the
        tmp relation, so target and tmp must hold the same number of them. The guard has to pass
        on a faithful copy and abort on a copy that picked up a second set of those rows, even
        when the parts no longer look alike - which is what check_duplicate_parts missed on
        2026-10-07. Rows at or after start_time are rewritten by the run and must not count.
        """

        con = ch_client
        target = 'check_untouched_rows_target_test'
        tmp = 'check_untouched_rows_tmp_test'
        for table in (target, tmp):
            con.command(f'drop table if exists default.{table}')
            con.command(
                f'create table default.{table} (event_datetime DateTime, v UInt64) '
                f'engine = MergeTree partition by toYYYYMM(event_datetime) order by v'
            )
        # two rows before start_time, one after it
        con.command(
            f"insert into default.{target} values "
            f"('2026-10-06 20:00:00', 1), ('2026-10-06 21:00:00', 2), ('2026-10-06 23:00:00', 3)"
        )
        con.command(f"alter table default.{tmp} attach partition id '202610' from default.{target}")
        # the run rewrote the tail with a different number of rows - must not matter
        con.command(f"insert into default.{tmp} values ('2026-10-06 23:30:00', 4)")

        args = (
            f"{{existing_relation: {{schema: default, identifier: {target}}}, "
            f"intermediate_relation: {{schema: default, identifier: {tmp}}}, "
            f"partition_id: '202610', output_datetime_column: event_datetime, "
            f"start_time: '2026-10-06 22:00:00', silence_mode: true}}"
        )
        run_dbt(['run-operation', 'check_untouched_rows_match', '--args', args])

        # a second set of the old rows lands in tmp as a separate, differently shaped part
        con.command(
            f"insert into default.{tmp} values ('2026-10-06 20:00:00', 1), ('2026-10-06 21:00:00', 2)"
        )
        run_dbt(
            ['run-operation', 'check_untouched_rows_match', '--args', args], expect_pass=False
        )

        rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=target))
        assert rows['rows_count'][0] == 3, 'the guard must leave target untouched'

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

    def test_batch_query_settings(self, ch_client, setup_test_environment):
        """
        batch_query_settings must reach ClickHouse on the batch INSERT without
        changing the result (BI-9988)
        """

        min_timestamp = setup_test_environment['min_timestamp']

        test_params = {
            'materialization_start_date': min_timestamp.strftime("%Y-%m-%d"),
            'batch_query_settings': {
                'max_threads': 4,
                'max_bytes_before_external_sort': 6000000000,
            },
        }

        self.execute_test(ch_client, test_params)

        con = ch_client
        con.command('system flush logs')
        settings = con.query_df(
            QUERY_LAST_INSERT_SETTINGS.format(table_name=MICROBATCH_TEST_MODEL)
        )['settings'][0]

        assert settings['max_threads'] == '4'
        assert settings['max_bytes_before_external_sort'] == '6000000000'

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

    # BI-10065: partition rows baseline -----------------------------------------------------------
    BASELINE_TABLE = '_microbatch_partition_rows'

    def _copied_partition(self, con):
        """The partition the next incremental run copies: the one holding max(event_datetime) - overwrite_size"""
        return con.query_df(
            f"select toString(toYYYYMM(max(event_datetime) - interval 12 hour)) as p "
            f"from default.{MICROBATCH_TEST_MODEL}"
        )['p'][0]

    def _run(self, materialization_start_date, expect_pass=True, **extra_vars):
        dbt_vars = {'materialization_start_date': materialization_start_date, 'enabled': True, **extra_vars}
        run_dbt(['run', '--select', f'{MICROBATCH_TEST_MODEL}', '--vars', f'{dbt_vars}'], expect_pass=expect_pass)

    def _rows(self, con):
        return con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_TEST_MODEL))['rows_count'][0]

    def test_empty_model_passes(self, ch_client, setup_test_environment):
        """
        A model with no rows at all (Tokyo: fct_les_raw_export_s3_uploads, 2026-10-27) leaves
        nothing to record: the empty partition list must not fail the run, neither after the
        exchange of a full refresh nor after an incremental run with an empty tmp.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        run_dbt(
            [
                'run',
                '--select',
                f'{MICROBATCH_TEST_MODEL}',
                '--vars',
                f"{{'materialization_start_date': '{start}', 'enabled': True, 'empty_input': True}}",
                '--full-refresh',
            ]
        )
        assert self._rows(con) == 0
        self._run(start, empty_input=True)
        assert self._rows(con) == 0

    def test_full_refresh_records_partition_rows(self, ch_client, setup_test_environment):
        """
        A full refresh records every partition of the new target with its row count, so the next
        run has a baseline for whichever partition it copies.
        """

        con = ch_client
        con.command(f'drop table if exists default.{self.BASELINE_TABLE}')
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})

        diff = con.query_df(
            f"select count() as n from ("
            f"  select partition_id, sum(rows) as rows from system.parts"
            f"  where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active group by partition_id"
            f") as p full outer join ("
            f"  select partition_id, argMax(rows, recorded_at) as rows from default.{self.BASELINE_TABLE}"
            f"  where table = '{MICROBATCH_TEST_MODEL}' group by partition_id"
            f") as b using partition_id where p.rows != b.rows or p.partition_id = '' or b.partition_id = ''"
            f" settings join_use_nulls = 0"
        )['n'][0]
        assert diff == 0, 'baseline must hold every partition with its row count'

    def test_partition_rows_baseline_catches_rows_doubled_inside_parts(self, ch_client, setup_test_environment):
        """
        BI-10065, 2026-10-27: after a ClickHouse restart fct_dataflow_run_events held an hour of rows
        twice inside one merged part. No identical parts to heal, and both row count guards passed,
        because tmp is a copy of target and inherits the extra rows. Here a second set of old rows is
        inserted as a part of its own: with 'fail' the run must stop and leave target as it was,
        with 'warn' it must go on.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})
        self._run(start, partition_rows_baseline_action='fail')  # a clean incremental run passes

        partition_id = self._copied_partition(con)
        old_rows_filter = (
            f"_partition_id = '{partition_id}' and event_datetime < "
            f"(select max(event_datetime) - interval 13 hour from default.{MICROBATCH_TEST_MODEL})"
        )
        con.command(
            f"insert into default.{MICROBATCH_TEST_MODEL} "
            f"select * from default.{MICROBATCH_TEST_MODEL} where {old_rows_filter} limit 50"
        )
        doubled = self._rows(con)
        input_rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))['rows_count'][0]
        assert doubled > input_rows, 'the setup did not double any old rows'

        self._run(start, expect_pass=False, partition_rows_baseline_action='fail')
        assert self._rows(con) == doubled, "'fail' must leave target as it was"

        self._run(start, partition_rows_baseline_action='warn')
        assert self._rows(con) == doubled, "'warn' goes on; the doubled old rows stay"

    def test_partition_rows_baseline_reset_after_manual_change(self, ch_client, setup_test_environment):
        """
        A deliberate manual change (here a DELETE of old rows) trips the check on the next run;
        microbatch_partition_rows_reset lets one run through and records the new count, after which
        the check passes again without the var.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})

        partition_id = self._copied_partition(con)
        before = self._rows(con)
        # the oldest rows of the partition: always below the rewritten window, whatever the input data
        con.command(
            f"alter table default.{MICROBATCH_TEST_MODEL} delete where _partition_id = '{partition_id}' "
            f"and event_datetime = (select min(event_datetime) from default.{MICROBATCH_TEST_MODEL} "
            f"where _partition_id = '{partition_id}') settings mutations_sync = 2"
        )
        assert self._rows(con) < before, 'the setup did not delete any old rows'

        self._run(start, expect_pass=False, partition_rows_baseline_action='fail')
        self._run(start, partition_rows_baseline_action='fail',
                  microbatch_partition_rows_reset=[MICROBATCH_TEST_MODEL])
        self._run(start, partition_rows_baseline_action='fail')

    def test_partition_rows_baseline_missing_passes(self, ch_client, setup_test_environment):
        """
        No baseline for the partition (first run after the upgrade, a new month): the run passes
        and leaves one behind.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})
        con.command(f'drop table if exists default.{self.BASELINE_TABLE}')

        self._run(start, partition_rows_baseline_action='fail')

        recorded = con.query_df(
            f"select count() as n from default.{self.BASELINE_TABLE} "
            f"where table = '{MICROBATCH_TEST_MODEL}' and partition_id = '{self._copied_partition(con)}'"
        )['n'][0]
        assert recorded > 0, 'the run must record the partition it replaced'

    def test_partition_rows_table_is_not_recreated_on_every_run(self, ch_client, setup_test_environment):
        """
        In a Replicated database every CREATE goes through the DDL queue, even one that finds the
        table. Once the baseline table exists, a run must not issue CREATE for it again.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})

        creates_query = f"""
            select count() as n
            from system.query_log
            where type = 'QueryFinish'
              and query_kind = 'Create'
              and query like '%{self.BASELINE_TABLE}%'
            """
        con.command('system flush logs')
        creates_before = con.query_df(creates_query)['n'][0]

        self._run(start)

        con.command('system flush logs')
        assert con.query_df(creates_query)['n'][0] == creates_before, 'the run issued CREATE for an existing table'

    # BI-10065: parts brought back by a restart ----------------------------------------------------
    def _parts_query(self, partition_id):
        return (
            "select name, min_block_number, max_block_number, rows from system.parts "
            f"where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active and rows > 0 "
            f"and partition_id = '{partition_id}' order by min_block_number"
        )

    def _bring_back_replaced_version(self, con, partition_id, current_version_change=None):
        """
        What a restart inside old_parts_lifetime after a REPLACE PARTITION leaves behind: the
        replaced version of the partition active again, below every block of the current one, and
        a baseline recorded for the current one. A restart cannot be staged here, so it is built
        the other way round: the parts the partition holds now become the old version, a copy of
        them (changed by current_version_change, if given) is attached above them as the current
        version, and the baseline is recorded for that copy, as the run that replaced it would have.
        Merges are stopped on target, so the two versions stay apart; the caller starts them again.
        """

        helper = f'{MICROBATCH_TEST_MODEL}__replaced_version_helper'
        con.command(f'system stop merges default.{MICROBATCH_TEST_MODEL}')
        con.command(f'drop table if exists default.{helper}')
        con.command(f'create table default.{helper} as default.{MICROBATCH_TEST_MODEL}')
        con.command(
            f"alter table default.{helper} attach partition id '{partition_id}' from default.{MICROBATCH_TEST_MODEL}"
        )
        con.command(f"optimize table default.{helper} partition id '{partition_id}' final")
        if current_version_change:
            con.command(current_version_change.format(table=f'default.{helper}'))

        floor = con.query_df(
            "select max(max_block_number) + 1 as b from system.parts "
            f"where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active"
        )['b'][0]
        con.command(
            f"alter table default.{MICROBATCH_TEST_MODEL} attach partition id '{partition_id}' from default.{helper}"
        )
        con.command(f'drop table default.{helper}')
        con.command(
            f"insert into default.{self.BASELINE_TABLE} (table, partition_id, rows, invocation_id, recorded_at, min_block) "
            f"select '{MICROBATCH_TEST_MODEL}', partition_id, sum(rows), 'test', now64(3), min(min_block_number) "
            f"from system.parts where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active "
            f"and partition_id = '{partition_id}' and min_block_number >= {floor} group by partition_id"
        )
        parts = con.query_df(self._parts_query(partition_id))
        assert (parts['max_block_number'] < floor).any() and (parts['min_block_number'] >= floor).any(), (
            f'the setup did not leave two versions apart: {parts}'
        )
        return floor

    def test_run_records_min_block_of_replaced_partitions(self, ch_client, setup_test_environment):
        """
        An incremental run records, for the partition it replaced, the lowest block number the
        REPLACE put in: every part of the partition starts at it or above.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})
        self._run(start)

        partition_id = self._copied_partition(con)
        recorded = con.query_df(
            f"select argMax(min_block, recorded_at) as b, argMax(rows, recorded_at) as r "
            f"from default.{self.BASELINE_TABLE} where table = '{MICROBATCH_TEST_MODEL}' "
            f"and partition_id = '{partition_id}'"
        )
        parts = con.query_df(self._parts_query(partition_id))
        assert recorded['b'][0] > 0, 'no min_block recorded'
        assert recorded['b'][0] == parts['min_block_number'].min(), f'{recorded} vs {parts}'
        assert recorded['r'][0] == parts['rows'].sum()

    def test_parts_brought_back_by_a_restart_are_healed(self, ch_client, setup_test_environment):
        """
        BI-10065, 2026-10-29: after a restart at 11:13 UTC the copied partition of
        fct_order_run_events held its replaced version next to the current one, partly in parts
        that were not byte-identical, so check_duplicate_parts could not heal them, and rows older
        than the overwrite window stayed doubled. Here the current version also lacks the newest
        hour of the old one, as after a real REPLACE. With 'heal' the old version is dropped, the
        run passes and the model holds the input exactly once; the same in a partition the run does
        not copy (yesterday's daily partition, the previous month at a month boundary).
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        input_rows = con.query_df(QUERY_COUNT_ROWS.format(table_name=MICROBATCH_INPUT_MODEL))['rows_count'][0]
        self.execute_test(ch_client, {'materialization_start_date': start})
        self._run(start)

        copied = self._copied_partition(con)
        older = con.query_df(
            f"select toString(min(toYYYYMM(event_datetime))) as p from default.{MICROBATCH_TEST_MODEL}"
        )['p'][0]
        assert older != copied, 'the input must span more than one partition'

        try:
            floor = self._bring_back_replaced_version(
                con, copied,
                current_version_change=(
                    "alter table {table} delete where event_datetime >= "
                    "(select max(event_datetime) - interval 1 hour from {table}) settings mutations_sync = 2"
                ),
            )
            self._bring_back_replaced_version(con, older)
            assert self._rows(con) > input_rows, 'the setup did not double any rows'

            self._run(start)
        finally:
            con.command('system start merges')

        assert self._rows(con) == input_rows, f'rows after healing: {self._rows(con)}, input: {input_rows}'
        older_parts = con.query_df(self._parts_query(older))
        assert (older_parts['min_block_number'] >= floor).all(), f'old parts survived: {older_parts}'

    def test_parts_merged_across_versions_stop_the_run(self, ch_client, setup_test_environment):
        """
        Once a merge has folded an old part and a current one together, no part can be dropped
        without losing current rows. 'heal' must leave target as it is and fail the run.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})
        self._run(start)

        partition_id = self._copied_partition(con)
        try:
            floor = self._bring_back_replaced_version(con, partition_id)
            con.command('system start merges')
            con.command(f"optimize table default.{MICROBATCH_TEST_MODEL} partition id '{partition_id}' final")
            parts = con.query_df(self._parts_query(partition_id))
            assert ((parts['min_block_number'] < floor) & (parts['max_block_number'] >= floor)).any(), (
                f'the setup did not merge the two versions: {parts}'
            )
            doubled = self._rows(con)

            self._run(start, expect_pass=False)
        finally:
            con.command('system start merges')

        assert self._rows(con) == doubled, "'heal' must not touch a partition it cannot heal"

    def test_partition_rows_baseline_checks_every_partition(self, ch_client, setup_test_environment):
        """
        Rows doubled in a partition the run does not copy were not looked at before; now every
        recorded partition is compared, and 'heal' stops the run (no old blocks to drop here).
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})

        older = con.query_df(
            f"select toString(min(toYYYYMM(event_datetime))) as p from default.{MICROBATCH_TEST_MODEL}"
        )['p'][0]
        assert older != self._copied_partition(con), 'the input must span more than one partition'
        con.command(
            f"insert into default.{MICROBATCH_TEST_MODEL} "
            f"select * from default.{MICROBATCH_TEST_MODEL} where _partition_id = '{older}' limit 50"
        )
        doubled = self._rows(con)

        self._run(start, expect_pass=False)
        assert self._rows(con) == doubled
        # the reset records every partition, so the next run passes without it
        self._run(start, microbatch_partition_rows_reset=[MICROBATCH_TEST_MODEL])
        self._run(start)

    def test_partition_rows_table_gets_min_block_column(self, ch_client, setup_test_environment):
        """
        A baseline table created before min_block existed gets the column on the next run, and its
        old rows (min_block 0) are left to the row count check: nothing is dropped on their account.
        """

        con = ch_client
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        self.execute_test(ch_client, {'materialization_start_date': start})
        con.command(f'drop table if exists default.{self.BASELINE_TABLE}')
        con.command(
            f'create table default.{self.BASELINE_TABLE} (table String, partition_id String, rows UInt64, '
            f'invocation_id String, recorded_at DateTime64(3)) '
            f'engine = ReplacingMergeTree(recorded_at) order by (table, partition_id)'
        )
        con.command(
            f"insert into default.{self.BASELINE_TABLE} "
            f"select '{MICROBATCH_TEST_MODEL}', partition_id, sum(rows), 'old', now64(3) from system.parts "
            f"where database = 'default' and table = '{MICROBATCH_TEST_MODEL}' and active group by partition_id"
        )
        rows = self._rows(con)

        self._run(start)

        columns = con.query_df(
            f"select name from system.columns where database = 'default' and table = '{self.BASELINE_TABLE}'"
        )['name'].tolist()
        assert 'min_block' in columns, columns
        assert self._rows(con) == rows

    def test_replicated_table_records_rows_per_partition(self, ch_client, setup_test_environment):
        """
        BI-10065, 2026-10-29: ReplicatedMergeTree numbers blocks per partition, not per table, so
        in improvado_models (a Replicated database) the current month can sit at lower block numbers
        than an older one. v1.3.50 took the REPLACE floor table-wide, found no part above it in the
        current month, recorded 0 rows, and the next run stopped on the mismatch. Here the model is
        built in a Replicated database and an older partition is pushed to higher block numbers
        (inserts that are deleted again); two incremental runs must pass and the baseline must hold
        the real row count.
        """

        con = ch_client
        db = 'microbatch_replicated_test'
        table = f'{db}.{MICROBATCH_TEST_MODEL}'
        start = setup_test_environment['min_timestamp'].strftime("%Y-%m-%d")
        dbt_vars = f"{{'materialization_start_date': '{start}', 'enabled': True, 'production_schema': '{db}', 'engine': 'ReplicatedMergeTree()'}}"
        con.command(f'drop database if exists {db} sync')
        con.command(f"create database {db} engine = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')")
        default_db = environ['CLICKHOUSE_DATABASE']
        environ['CLICKHOUSE_DATABASE'] = db
        try:
            run_dbt(['run', '--select', MICROBATCH_INPUT_MODEL, '--vars', dbt_vars])
            run_dbt(['run', '--select', MICROBATCH_TEST_MODEL, '--full-refresh', '--vars', dbt_vars])
            assert con.query_df(
                f"select engine from system.tables where database = '{db}' and name = '{MICROBATCH_TEST_MODEL}'"
            )['engine'][0] == 'ReplicatedMergeTree'

            partitions = con.query_df(
                f"select toString(min(toYYYYMM(event_datetime))) as older, "
                f"toString(toYYYYMM(max(event_datetime) - interval 12 hour)) as copied, "
                f"toString(min(event_datetime)) as older_time from {table}"
            )
            older, copied, older_time = partitions['older'][0], partitions['copied'][0], partitions['older_time'][0]
            assert older != copied, 'the input must span more than one partition'
            for i in range(60):
                con.command(
                    f"insert into {table} (event_datetime, event_id, event_data) "
                    f"values ('{older_time}', {i}, 'block_filler')"
                )
            con.command(f"alter table {table} delete where event_data = 'block_filler' settings mutations_sync = 2")
            blocks = con.query_df(
                "select partition_id, max(max_block_number) as b from system.parts "
                f"where database = '{db}' and table = '{MICROBATCH_TEST_MODEL}' and active group by partition_id"
            ).set_index('partition_id')['b']
            assert blocks[older] > blocks[copied], f'the setup did not push {older} above {copied}: {blocks}'

            run_dbt(['run', '--select', MICROBATCH_TEST_MODEL, '--vars', dbt_vars])
            run_dbt(['run', '--select', MICROBATCH_TEST_MODEL, '--vars', dbt_vars])

            recorded = con.query_df(
                f"select argMax(rows, recorded_at) as r from {db}.{self.BASELINE_TABLE} "
                f"where table = '{MICROBATCH_TEST_MODEL}' and partition_id = '{copied}'"
            )['r'][0]
            actual = con.query_df(
                f"select sum(rows) as r from system.parts where database = '{db}' "
                f"and table = '{MICROBATCH_TEST_MODEL}' and active and partition_id = '{copied}'"
            )['r'][0]
            assert recorded == actual and actual > 0, f'recorded {recorded}, actual {actual}'
        finally:
            environ['CLICKHOUSE_DATABASE'] = default_db
            con.command(f'drop database if exists {db} sync')
