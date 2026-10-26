{%- materialization microbatch -%}
-- base settings -------------------------------------------------------------------------------------------------------
    {%- set dt                                  = modules.datetime -%}
    {%- set re                                  = modules.re -%}
    {%- set diu                                 = dbt_improvado_utils -%}
    {%- set full_refresh                        = flags.FULL_REFRESH -%}
    {%- set target_schema                       = target.schema -%}
    {%- set production_schema                   = config.get('production_schema',
                                                    default=var('main_production_schema', 'internal_analytics')) -%}
    -- dependency management and tracking changed relations within dbt pipeline
    {%- set sections_arr                        = [] -%}
    -- if contract is enforced in yaml file
    {%- set is_contract_enforced                = config.get('contract').enforced -%}
    -- possible values: fail, full_refresh
    {%- set on_schema_change_action             = config.get('on_schema_change_action', default='fail') -%}

-- microbatch settings
    {%- set microbatch_settings_pattern         = '\s*--\s*microbatch:\s*(\w+),\s*(\d+)(?:,\s*(\d+))?\s*.*`.+`\.`(.+)`\s+(final)?' -%}
    {%- set microbatch_settings                 = re.findall(microbatch_settings_pattern, sql, flags=re.IGNORECASE) -%}

    {%- set input_models_list                   = [] -%}
    {%- set final_settings_list                 = [] -%}
    {%- set input_columns_list                  = [] -%}
    {%- set input_lookback_windows_list         = [] -%}
    {%- set input_lookforward_windows_list      = [] -%}

-- output model data
    {%- set output_datetime_column              = config.require('output_datetime_column') -%}

-- datetime settings
    {%- set materialization_start_date          = dt.datetime.strptime(
                                                    config.require('materialization_start_date'), '%Y-%m-%d') -%}
    {%- set _materialization_end_date_str       = config.get('materialization_end_date') -%}
    {%- set materialization_end_date            = dt.datetime.strptime(_materialization_end_date_str, '%Y-%m-%d')
                                                    if _materialization_end_date_str else none -%}
    {%- set time_unit_name                      = config.require('time_unit_name') -%}
    {%- set batch_size                          = config.require('batch_size') -%}
    {%- set overwrite_size                      = config.get('overwrite_size', default=0) -%}
    {%- set dev_days_offset                     = config.get('dev_days_offset', default=0) -%}
    {%- set fixed_now                           = dt.datetime.now().replace(microsecond=0) -%}

-- other settings
    {%- set partition_by                        = config.require('partition_by') -%}
    {%- set partition_by_format                 = re.findall('(?<=to)\w+(?=\()', partition_by)[0] | lower -%}

-- query settings for the heavy batch INSERT (BI-9988); the light DDL is already
-- covered by the adapter-level query_settings
    {%- set batch_query_settings                = config.get('batch_query_settings', default={}) -%}

-- log settings
    {%- set debug_mode                          = config.get('debug_mode', default=false) -%}
    {%- set silence_mode                        = not config.get('silence_mode', default=false) -%}

-- what to do with duplicate parts (same hash_of_all_files) found in target before the copy, in
-- tmp before the guard and in target after REPLACE: 'drop' keeps one copy (default),
-- 'report' only logs them (BI-10065)
    {%- set duplicate_parts_action              = config.get('duplicate_parts_action', default='drop') -%}

-- logic ---------------------------------------------------------------------------------------------------------------
    {%- if not microbatch_settings -%}
        {%- do exceptions.raise_compiler_error(
                    diu.mcr_log_colored(
                        'No microbatch settings found\n' ~
                        'Please add microbatch settings to the model input section', color='red')) -%}
    {%- endif -%}

-- getting microbatch settings
    {%- for setting in microbatch_settings -%}
        {%- do input_models_list.append(setting[3]) -%}
        {%- do final_settings_list.append(setting[4]) -%}
        {%- do input_columns_list.append(setting[0]) -%}
        {%- do input_lookback_windows_list.append(setting[1] | int) -%}
        {%- do input_lookforward_windows_list.append((setting[2] | int) if setting[2] else 0) -%}
    {%- endfor -%}

    {{- diu.mcr_log_colored(
            'Input models:\n\t' ~ input_models_list | join('\n\t') ~
            '\nFinal is set for:\n\t' ~
                zip(input_models_list, final_settings_list) | selectattr(1) | map(attribute=0) | join('\n\t')  ~
            '\nInput columns:\n\t' ~ input_columns_list | join('\n\t') ~
            '\nInput lookback windows:\n\t' ~ input_lookback_windows_list | join('\n\t') ~
            '\nInput lookforward windows:\n\t' ~ input_lookforward_windows_list | join('\n\t') ~
            '\nFixed now time:\n\t' ~ fixed_now, silence_mode) -}}

-- clearing sql from microbatch and final settings
    {%- set sql = re.sub('\s*--\s*microbatch:\s*\w+,\s*\d+(?:,\s*\d+)?', '', sql) -%}
    {%- set sql = re.sub('`(.+)`\.`(.+)`\s+final', '`\\1`.`\\2`', sql, flags=re.IGNORECASE) -%}

    {%- if target_schema == production_schema -%}
        {{- diu.mcr_log_colored('Starting to build to production schema: ' ~ target_schema, silence_mode) -}}

    {%- else -%}
        {%- set materialization_start_date =
                    dt.datetime.combine(dt.datetime.today() - dt.timedelta(days=dev_days_offset), dt.time.min) -%}

        {{- diu.mcr_log_colored(
                'Starting to build to dev schema:\n\t' ~ target_schema ~
                '\nDev schema materialization start date:\n\t' ~ materialization_start_date ~
                '\nOriginal materialization start date:\n\t' ~ config.get('materialization_start_date'), silence_mode) -}}
    {%- endif -%}

-- checking columns consistency(columns names and types comparison)
    {%- set is_schema_changed =
                diu.check_schema_changes(
                    database=none,
                    schema=target_schema,
                    identifier=this.identifier,
                    type='table',
                    sql=diu.get_table_structure(sql, is_contract_enforced),
                    debug_mode=debug_mode,
                    silence_mode=silence_mode) -%}

    {%- if is_schema_changed -%}
        {%- if on_schema_change_action == 'fail' and not full_refresh -%}
            {%- do exceptions.raise_compiler_error(
                    diu.mcr_log_colored(
                        'Schema change detected. Materialization will be stopped\n' ~
                        'Please revise the schema changes or set "on_schema_change_action" to "full_refresh"\n' ~
                        'If you want to force materialization - run with "full-refresh" flag', color='red')) -%}

        {%- elif on_schema_change_action == 'full_refresh' -%}
            {%- set full_refresh = true -%}
            {{- diu.mcr_log_colored(
                    'Full refresh will be done since "on_schema_change_action" is set to "full_refresh"', silence_mode, color='red') -}}

        {%- endif -%}
    {%- endif -%}

-- creating target relation
    {%- set target_relation_exists, target_relation =
                diu.get_or_create_dataset(
                    database=none,
                    schema=target_schema,
                    identifier=this.identifier,
                    type='table',
                    sql=diu.get_table_structure(sql, is_contract_enforced),
                    debug_mode=debug_mode,
                    silence_mode=silence_mode) -%}

    {%- do sections_arr.append(target_relation) -%}

-- previous run target_relation_max_datetime calculation
    {%- if target_relation_exists and not full_refresh -%}
    -- getting max datetime from target relation
        {%- set target_relation_max_datetime =
                    diu.get_max_datetime(target_relation, output_datetime_column).replace(tzinfo=None) -%}

        {%- set last_record_datetime =
                    [target_relation_max_datetime, materialization_start_date] | max -%}

        {%- set start_time = last_record_datetime - diu.get_unit_interval(value=overwrite_size, unit=time_unit_name) -%}

        {{- diu.mcr_log_colored(
                'Target relation exists' ~
                '\nLast record datetime:\n\t' ~ last_record_datetime ~
                '\nLast record datetime with overwrite size:\n\t' ~ start_time, debug_mode) -}}

    {%- else -%}
        {%- set start_time = materialization_start_date -%}
        {{- diu.mcr_log_colored(
                'Target relation doesn\'t exist' ~
                '\nDefault datetime:\n\t' ~ start_time, debug_mode) -}}

    {%- endif -%}

-- interval counts calculation
    {%- set interval_range =
                diu.get_unit_datediff(
                    startdate=start_time,
                    enddate=materialization_end_date or fixed_now,
                    unit=time_unit_name) -%}

    {{- diu.mcr_log_colored('Calculating interval parts', silence_mode) -}}
    {%- set parts_count = [(interval_range / batch_size) | round(0, 'ceil') | int, 2] | max -%}

    {#-
        parts_count never drops below 2, so a run whose window fits one batch (every 5-minute
        cronjob model: overwrite_size < batch_size) always gets a second batch that starts after
        now. Its HAVING window can only match rows timestamped in the future, and on Palantir
        Lisbon not one of ~90,000 such batches wrote a row in 30 days (2026-09-14..10-14, 32
        models, all users) -- yet the inputs still run their full query. On src_improvado_order_run_events,
        _legacy and _runs_billing the future batch costs 60-85% of the real one (the
        mrt_order_run_events_v2 joins are built regardless of the window): ~0.25 core around the
        clock across the five BI-10045 models. Batches starting after fixed_now are skipped; the
        first batch is always kept, because the tmp partition already had rows >= start_time
        deleted and must be refilled even if the target somehow holds future-dated rows.
        materialization_end_date is left alone: a backfill ends where it ends.
    -#}
    {%- set batch_indexes = [] -%}
    {%- for i in range(parts_count) -%}
        {%- set interval_start = start_time + diu.get_unit_interval(value=i * batch_size, unit=time_unit_name) -%}
        {%- if i == 0 or materialization_end_date or interval_start <= fixed_now -%}
            {%- do batch_indexes.append(i) -%}
        {%- endif -%}
    {%- endfor -%}

    {{- diu.mcr_log_colored(
            'Interval parts count:\n\t' ~ parts_count ~
            ((' (' ~ (parts_count - batch_indexes | length) ~ ' starting after ' ~ fixed_now ~ ' skipped)')
                if batch_indexes | length < parts_count else ''), silence_mode) -}}

-- temporary table creation
-- the tmp identifier is scoped to this dbt invocation: two overlapping runs of the same
-- model (e.g. a retried CronJob execution) must never write into the same physical tmp
-- table, since that race is invisible to check_duplicate_parts (each run's INSERT produces
-- its own MergeTree parts with a different hash, even when the resulting rows are identical)
    {%- set tmp_identifier = target_relation.identifier ~ '__microbatch_tmp_' ~ (invocation_id | replace('-', '')) -%}

    {%- do diu.cleanup_stale_microbatch_tmp_relations(
                schema=target_schema,
                base_identifier=target_relation.identifier,
                keep_identifier=tmp_identifier,
                silence_mode=silence_mode) -%}

    {%- set tmp_relation_exists, tmp_relation =
                get_or_create_relation(
                    database=none,
                    schema=target_schema,
                    identifier=tmp_identifier,
                    type='table') -%}

    {%- do drop_relation(tmp_relation) -%}
    {%- do create_table_as(false, tmp_relation.identifier, sql) -%}

    {%- set partition_id = diu.get_partition_id(start_time, partition_by_format) -%}

    {%- if not full_refresh -%}
    -- healing identical parts in target before they are copied (BI-10065). A server restart
    -- inside old_parts_lifetime after a REPLACE PARTITION brings the replaced parts back as
    -- active next to their replacements: byte-identical twins, the same rows twice. Copied into
    -- tmp they used to make one of two things happen: check_duplicate_parts(tmp) dropped one
    -- copy and check_untouched_rows_match refused every REPLACE from then on
    -- (fct_product_domain_events stood still 2026-10-19 09:04 -> 10-21 14:47 UTC), or, with
    -- duplicate_parts_action = 'report', the doubled rows were kept and flowed downstream.
    -- Dropping the extra copies in target first keeps one of each, so target and tmp agree and
    -- the guard has nothing to object to; with 'report' they are only logged, as before.
        {%- do diu.check_duplicate_parts(target_relation, silence_mode, action=duplicate_parts_action) -%}

        {{- diu.mcr_log_colored(
                'Copying partition "' ~ partition_id ~ '" from ' ~ target_relation ~ ' to ' ~ tmp_relation, silence_mode) -}}
        {%- do diu.copy_partition(target_relation, tmp_relation, partition_id) -%}

    -- guarding against a duplicated ATTACH (e.g. retried statement after a dropped connection)
    -- must run before any mutation touches tmp, otherwise the duplicate rows are indistinguishable
        {%- do diu.check_partition_row_count_match(target_relation, tmp_relation, partition_id, silence_mode) -%}

    -- deleting data to be overwritten from tmp relation
        {%- do diu.wait_for_merges(tmp_relation) -%}
        {%- do run_query(
            "alter table " ~ tmp_relation ~ " delete where " ~ output_datetime_column ~ " >= " ~ "toDateTime('" ~ start_time ~ "')"
            ~ " settings mutations_sync = 1") -%}

    {%- endif -%}

-- insert queries generation
    {%- for i in batch_indexes -%}
    -- inserting intervals list calculation
        {%- set where_conditions, having_conditions =
                    diu.get_intervals_list(
                        interval_offset=i * batch_size,
                        time_unit_name=time_unit_name,
                        start_time=start_time,
                        lookback_windows_list=input_lookback_windows_list,
                        lookforward_windows_list=input_lookforward_windows_list,
                        batch_size=batch_size) -%}

    -- inserting intervals list calculation
        {%- set insert_query =
                    diu.get_insert_query(
                        sql=sql,
                        target_relation=tmp_relation,
                        input_models_list=input_models_list,
                        final_settings_list=final_settings_list,
                        input_columns_list=input_columns_list,
                        where_conditions=where_conditions,
                        having_conditions=having_conditions,
                        output_column=output_datetime_column,
                        debug_mode=debug_mode,
                        query_settings=batch_query_settings) -%}

        {%- if execute -%}
            {%- if loop.first -%}
                {{- diu.mcr_log_colored('Inserting into: ' ~ tmp_relation, silence_mode) -}}
            {%- endif -%}

        {{- diu.mcr_log_colored(
            'Inserting batch: ' ~ (i + 1) ~ ' out of ' ~ parts_count ~
            '\nDate range: from ' ~  having_conditions[0] ~ ' to ' ~  having_conditions[1], silence_mode) -}}

        {{- diu.mcr_log_colored(insert_query[250:1100] ~ '\n\n...\n\n' ~ insert_query[-200:], debug_mode) -}}

        -- insert query execution
            {%- call statement('inserting_new_data_to_temporary_table') -%}
                {{- insert_query -}}
            {%- endcall -%}

        {%- endif -%}
    {%- endfor -%}

    {%- if full_refresh -%}
    -- exchanging tmp table and target table
        {{- diu.mcr_log_colored('Exchanging ' ~ tmp_relation ~ ' with ' ~ target_relation, silence_mode) -}}
        {{- diu.exchange_tables(tmp_relation, target_relation) -}}
    {%- else -%}
    -- checking tmp table for duplicate parts before replacing and dropping if needed.
    -- With duplicate_parts_action = 'report' the copies stay: a target that already holds
    -- duplicate parts (fct_product_domain_events, 2026-10-19) hands them to tmp through
    -- copy_partition, and dropping them here would fail check_untouched_rows_match on every
    -- run - the guard expects the old rows back exactly as they were (BI-10065)
        {%- do diu.check_duplicate_parts(tmp_relation, silence_mode, action=duplicate_parts_action) -%}

    -- last guard before the swap: rows older than start_time were only copied, never rewritten,
    -- so tmp must hold exactly as many of them as target does
        {%- do diu.check_untouched_rows_match(
                    target_relation, tmp_relation, partition_id, output_datetime_column, start_time, silence_mode) -%}

    -- replacing partitions
        {{- diu.mcr_log_colored('Replacing partitions from ' ~ tmp_relation ~ ' to ' ~ target_relation, silence_mode) -}}
        {{- diu.insert_overwrite_partitions(target_relation, tmp_relation) -}}

    -- checking target table for duplicate parts after replacing and dropping if needed
        {%- do diu.check_duplicate_parts(target_relation, silence_mode, action=duplicate_parts_action) -%}
    {%- endif -%}
-- dropping tmp table after replacing or exchanging
    {%- do drop_relation(tmp_relation) -%}

    {%- call noop_statement('main', 'Done') -%} {%- endcall -%}
    {%- do return ({'relations': sections_arr}) -%}
{%- endmaterialization -%}


{%- macro check_schema_changes(database, schema, identifier, type, sql, debug_mode, silence_mode) -%}
{#
    Compares history table columns names and types consistency with the query being executed
    Arguments:
        database(string):           The database name
        schema(string):             The schema name
        identifier(string):         The table name
        type(string):               The table type
        sql(string):                The query string
        debug_mode(bool):           Should the debug messages be printed
        silence_mode(bool):         Should the log messages be silenced
    Returns:
        True if the existing table is not consistent with the query being executed
#}
-- namespace to allow carrying a value from within a loop body to an outer scope
    {%- set diu = dbt_improvado_utils -%}

    {%- set is_schema_changed = namespace(value=false) -%}

-- get relation to check its existance
    {%- set relation =
            adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

    {%- if relation and execute -%}
        {{- diu.mcr_log_colored('Checking existing table columns names and types consistency', debug_mode) -}}

        {%- set check_relation_exists, check_relation =
                    diu.get_or_create_dataset(
                        database=database,
                        schema=schema,
                        identifier=identifier ~ '__consistent_tmp',
                        type=type,
                        sql=sql,
                        debug_mode=debug_mode,
                        silence_mode=silence_mode) -%}

    -- getting columns from comparable relations
        {%- set columns_old = adapter.get_columns_in_relation(relation) -%}
        {%- set columns_new = adapter.get_columns_in_relation(check_relation) -%}

    -- dropping tmp table for columns comparison
        {%- do drop_relation(check_relation) -%}

    -- columns number comparison
        {%- if (columns_old | length) != (columns_new | length) -%}
            {%- set is_schema_changed.value = true -%}
            {{- diu.mcr_log_colored(
                    'Number of columns doesn\'t match', silence_mode, color='red') -}}

        {%- else -%}
            {{- diu.mcr_log_colored(
                    'Number of columns match\nChecking for name and type consistency', silence_mode) -}}

            {%- for i in range(columns_new | length) -%}
                {%- set column_old = columns_old[i] -%}
                {%- set column_new = columns_new[i] -%}

                {%- if column_old.data_type != column_new.data_type or column_old.name != column_new.name -%}
                    {%- set is_schema_changed.value = true -%}
                    {{- diu.mcr_log_colored(
                            'Column name/type mismatch:' ~
                            '\n\told: ' ~ column_old.name ~ ' ' ~ column_old.data_type ~
                            '\n\tnew: ' ~ column_new.name ~ ' ' ~ column_new.data_type, silence_mode, color='red') -}}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}

        {{- diu.mcr_log_colored(
                'Checking for name and type consistency is done',
                silence_mode,
                color='red' if is_schema_changed.value else 'green') -}}
    {%- endif -%}

    {{- return(is_schema_changed.value) -}}
{%- endmacro -%}


{%- macro get_or_create_dataset(database, schema, identifier, type, sql, debug_mode, silence_mode) -%}
{#
    Gets relation if it exists
    Creates relation if it doesn't exist
    Arguments:
        database(string):    The database name
        schema(string):      The schema name
        identifier(string):  The table name
        type(string):        The table type
        sql(string):         The query string
        debug_mode(bool):    Should the debug messages be printed
        silence_mode(bool):  Should the log messages be silenced
    Returns:
        Array of two elements:
            relation_exists(bool):      If the relation exists
            Relation_obj(api.Relation): The relation object
#}
    {%- set diu = dbt_improvado_utils -%}

    {{- diu.mcr_log_colored('Checking if relation exists:\n\t' ~ identifier, debug_mode) -}}

    {%- set relation_exists, relation =
                get_or_create_relation(
                    database=database,
                    schema=schema,
                    identifier=identifier,
                    type=type) -%}

-- creating relation if it doesn't exist
    {%- if not relation_exists and execute -%}
        {{- diu.mcr_log_colored(
                'Creating non-existing relation:\n\t' ~ identifier, debug_mode) -}}

        {%- if type == 'table' -%}
            {# `temporary` was previously an undeclared Jinja variable here (rendered as Undefined) #}
            {%- do create_table_as(false, relation, sql) -%}
        {%- else -%}
            {%- do create_view_as(relation, sql) -%}
        {%- endif -%}

        {{- diu.mcr_log_colored(
                'Relation has been created:\n\t' ~ identifier, debug_mode) -}}
    {%- endif -%}

    {{- return ([relation_exists, relation]) -}}
{%- endmacro -%}


{%- macro get_max_datetime(relation, timestamp_column) -%}
{#
    Gets the maximum value of timestamp_column in relation
    Arguments:
        relation(api.Relation): The target relation
        timestamp_column(string): Column name
    Returns:
        The maximum value of timestamp_column in DateTime format

    Reads only the newest non-empty partition instead of the whole table. Every microbatch
    model partitions by toYYYYMM(output_datetime_column) (partition_by is required and parsed
    for that), so the maximum of that column lives in the partition with the greatest
    partition_id. A plain max() over the whole table is answered from _minmax_count_projection
    only while no part carries a lightweight DELETE mask; after the first DELETE the same
    max() reads every part on every run (BI-10046, Palantir Lisbon, 30 days:
    fct_dataflow_run_events 5.5k calls / 101 TB, src_improvado_dataflow_run_events 5.5k / 85 TB).
    Falls back to the whole-table max() when the newest partition has no live rows (all of
    them deleted) or system.parts lists no partition at all.
#}
    {%- if execute -%}
        {%- set latest_partition_query -%}
            select
                max(partition_id)
            from
                system.parts
            where
                active
                and rows > 0
                and database = '{{ relation.schema }}'
                and table = '{{ relation.identifier }}'
        {%- endset -%}
        {%- set latest_partition_id = run_query(latest_partition_query)[0][0] -%}

        {%- if latest_partition_id -%}
            {%- set latest_partition_max_datetime -%}
                select
                    toDateTime(max({{ timestamp_column }})),
                    count()
                from
                    {{ relation }}
                where
                    _partition_id = '{{ latest_partition_id }}'
            {%- endset -%}
            {%- set latest_partition_max_datetime_row = run_query(latest_partition_max_datetime)[0] -%}

            {%- if latest_partition_max_datetime_row[1] > 0 -%}
                {{- return(latest_partition_max_datetime_row[0]) -}}
            {%- endif -%}
        {%- endif -%}

        {%- set max_datetime -%}
            select
                toDateTime(max({{ timestamp_column }}))
            from
                {{ relation }}
        {%- endset -%}
        {{- return(run_query(max_datetime)[0][0]) -}}
    {%- endif -%}
{%- endmacro -%}


{%- macro get_unit_interval(value, unit) -%}
{#
    A duration expressing the difference between two date, time, or datetime instances to unit resolution
    Arguments:
        value(int):     The numerical value of the interval
        unit(string):   The type of interval for result. Possible values: (hour, day)
    Returns:
        Rendered datetime object(e.g. 1 hour == 1:00:00, 1 day == 1 day, 0:00:00)
#}
    {{- return(modules.datetime.timedelta(**{unit ~ 's': value})) -}}
{%- endmacro -%}


{%- macro get_unit_datediff(startdate, enddate, unit) -%}
{#
    Gets the difference between two dates or dates with time values
    Arguments:
        startdate(datetime):    The first time value to subtract (the subtrahend)
        enddate(datetime):      The second time value to subtract from (the minuend)
        unit(string):           The type of interval for result. Possible values: (hour, day)
    Returns:
        Absolute difference between enddate and startdate expressed in unit
#}
    {%- set unit_seconds = {'hour': 3600, 'day': 86400}[unit] -%}

    {{- return(((enddate - startdate).total_seconds() / unit_seconds) | abs) -}}
{%- endmacro -%}


{%- macro get_intervals_list(interval_offset, time_unit_name, start_time, lookback_windows_list, lookforward_windows_list, batch_size) -%}
{#
    Calculates the list of intervals for each lookback and lookforward window
    Arguments:
        interval_offset(int):           The offset of the interval
        time_unit_name(string):         The type of interval for result. Possible values: (hour, day)
        start_time(datetime):           The start time
        lookback_windows_list(list):    The list of lookback windows
        lookforward_windows_list(list): The list of lookforward windows
        batch_size(int):                The batch size
    Returns:
        Nested array of intervals with the following structure:
            [[[left_where_condition, right_where_condition], ...], left_having_condition, right_having_condition]
#}
    {%- set dt  = modules.datetime -%}
    {%- set diu = dbt_improvado_utils -%}

-- nested array of intervals for each lookback window
    {%- set lookback_windows_intervals = [] -%}

-- base date of each interval; increases on every iteration for batch_size value
    {%- set interval_start = start_time + diu.get_unit_interval(value=interval_offset, unit=time_unit_name) -%}
    {%- set interval_end = interval_start + diu.get_unit_interval(value=batch_size, unit=time_unit_name) -%}

    {%- for i in range(lookback_windows_list | length) -%}
        {%- set lookback_window = lookback_windows_list[i] -%}
        {%- set lookforward_window = lookforward_windows_list[i] -%}
    -- start date of each batch with lookback window
        {%- set left_where_condition = interval_start - diu.get_unit_interval(value=lookback_window, unit=time_unit_name) -%}
    -- end date of each batch with lookforward window
        {%- set right_where_condition = interval_end + diu.get_unit_interval(value=lookforward_window, unit=time_unit_name) -%}

        {%- do lookback_windows_intervals.append([left_where_condition, right_where_condition]) -%}
    {%- endfor -%}

-- where conditions for final select from from CTE
    {%- set left_having_condition = interval_start -%}
    {%- set right_having_condition = interval_end -%}

    {{- return([lookback_windows_intervals, [left_having_condition, right_having_condition]]) -}}
{%- endmacro -%}


{%- macro get_settings_clause(settings) -%}
{#
    Renders a SETTINGS clause from a dict, with sorted keys so the generated
    SQL is deterministic (stable normalized_query_hash). Empty dict renders nothing.
#}
    {%- if settings -%}
        {%- set parts = [] -%}
        {%- for key, value in settings | dictsort -%}
            {%- if value is string -%}
                {%- do parts.append("{} = '{}'".format(key, value)) -%}
            {%- else -%}
                {%- do parts.append('{} = {}'.format(key, value)) -%}
            {%- endif -%}
        {%- endfor -%}
        settings {{ parts | join(', ') }}
    {%- endif -%}
{%- endmacro -%}


{%- macro get_insert_query(sql, target_relation, input_models_list, final_settings_list, input_columns_list, where_conditions, having_conditions, output_column, debug_mode, query_settings={} ) -%}
{#
    Generates the insert query
    Arguments:
        sql(string):                The sql query
        target_relation(string):    The target table
        input_models_list(array):   The list of input models
        final_settings_list(array): The list of final settings
        input_columns_list(array):  The list of input columns
        where_conditions(array):    The list of where conditions
        having_conditions(array):   The list of having conditions
        output_column(string):      The output column name
        debug_mode(bool):           The debug mode
        query_settings(dict):       ClickHouse settings appended to the INSERT (BI-9988)
    Returns:
        Substituted sql query
#}
    {%- set model_sql = namespace(value=sql) -%}
    {%- set left_having, right_having = having_conditions -%}

    {%- for i in range(input_models_list | length) -%}
        {%- set input_model = input_models_list[i] -%}
        {%- set final_setting = final_settings_list[i] -%}
        {%- set input_column = input_columns_list[i] -%}
        {%- set left_where, right_where = where_conditions[i] -%}

        {%- set input_relation = '`{}`.`{}`'.format(schema, input_model) -%}
        {%- set ia_relation = '`internal_analytics`.`{}`'.format(input_model) -%}
        {%- set sql_replacement_template = "{} {} where {} between '{}' and '{}'" -%}

        {%- set current_schema_replacement = sql_replacement_template.format(
                                                input_relation, final_setting, input_column, left_where, right_where) -%}

        {%- set ia_schema_replacement = sql_replacement_template.format(
                                                ia_relation, final_setting, input_column, left_where, right_where) -%}


        {%- set model_sql.value = model_sql.value | replace(input_relation, current_schema_replacement) -%}
        {# when --defer is used on staging/dev schema current relation will be `internal_analytics`.`model` and "current_schema_replacement" won't work #}
        {# but when deploying on prod we need to check if relations aren't the same to prevent double replacement and subqueries #}
        {%- if input_relation != ia_relation -%}
            {%- set model_sql.value = model_sql.value | replace(ia_relation, ia_schema_replacement) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set insert_query -%}
        insert into {{ target_relation }}
        with
            _sql as (
                {{ model_sql.value }}
            )
        select
            *
        from
            _sql
        where
            toDateTime({{ output_column }}) >= '{{ left_having }}'
            and toDateTime({{ output_column }}) < '{{ right_having }}'
        {{ dbt_improvado_utils.get_settings_clause(query_settings) }}
    {%- endset -%}

    {{- return(insert_query) -}}
{%- endmacro -%}


{%- macro get_table_structure(sql, is_contract_enforced) -%}
{#
    Generates table structure query for dbt create_table_as/create_view_as macro
    Arguments:
        sql(string):                    The query of the target model
        is_contract_enforced(bool):     The flag to check if contract is enforced
    Returns:
        Query string
#}
    {%- set limit_clause = '' if is_contract_enforced else 'limit 0' -%}

    {{- return('select * from (' ~ sql ~ ') ' ~ limit_clause) -}}
{%- endmacro -%}


{%- macro insert_overwrite_partitions(existing_relation, intermediate_relation) -%}
{#
    Replaces existing partitions with new data
    Arguments:
        existing_relation(api.Relation):        The existing relation
        intermediate_relation(api.Relation):    The temporary relation
    Returns:
        None
#}
    {%- set diu = dbt_improvado_utils -%}
    {%- if execute -%}
        {%- set select_changed_partitions -%}
            select
                distinct partition_id
            from
                system.parts
            where
                active
                and database = '{{ intermediate_relation.schema }}'
                and table = '{{ intermediate_relation.identifier }}'
        {%- endset -%}
        {%- set changed_partitions = run_query(select_changed_partitions).rows -%}
    {%- else -%}
        {%- set changed_partitions = [] -%}
    {%- endif -%}

    {%- if changed_partitions -%}
        {%- do diu.wait_for_merges(existing_relation) -%}
        {%- for partition in changed_partitions -%}
            {%- call statement('replace_partitions') -%}
                alter table {{ existing_relation }}
                replace partition id '{{ partition['partition_id'] }}'
                from {{ intermediate_relation }}
            {%- endcall -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro copy_partition(existing_relation, intermediate_relation, partition_id) -%}
{#
    Copies existing partitions from target relation to intermediate relation
    Arguments:
        existing_relation(api.Relation):        The existing relation
        intermediate_relation(api.Relation):    The temporary relation
        partition_id(string):                   The partition id
    Returns:
        None
#}
    {%- call statement('copy_partition') -%}
        alter table {{ intermediate_relation }}
            attach partition id '{{ partition_id }}'
            from {{ existing_relation }}
    {%- endcall -%}
{%- endmacro -%}


{%- macro check_partition_row_count_match(existing_relation, intermediate_relation, partition_id, silence_mode) -%}
{#
    Guards against a duplicated ATTACH (e.g. the statement executed twice after a dropped
    connection and client retry with the same query_id). Compares the row count of the just-copied
    partition in intermediate_relation against the source partition_id row count in existing_relation
    right after copy_partition, before any mutation can make the extra copy invisible to
    check_duplicate_parts' hash-based comparison.
    Arguments:
        existing_relation(api.Relation):       The relation the partition was copied from
        intermediate_relation(api.Relation):   The relation the partition was copied into
        partition_id(string):                  The partition id that was just copied
        silence_mode(bool):                    Should the log messages be silenced
    Returns:
        None (raises a compiler error on mismatch)
#}
    {%- set diu = dbt_improvado_utils -%}

    {%- set row_counts_query -%}
        select
            (select sum(rows) from system.parts
             where database = '{{ existing_relation.schema }}'
               and table = '{{ existing_relation.identifier }}'
               and partition_id = '{{ partition_id }}'
               and active) as target_rows,
            (select sum(rows) from system.parts
             where database = '{{ intermediate_relation.schema }}'
               and table = '{{ intermediate_relation.identifier }}'
               and partition_id = '{{ partition_id }}'
               and active) as tmp_rows
    {%- endset -%}

    {%- if execute -%}
        {%- set result = run_query(row_counts_query).rows[0] -%}
        {%- set target_rows = result['target_rows'] or 0 -%}
        {%- set tmp_rows = result['tmp_rows'] or 0 -%}

        {{- diu.mcr_log_colored(
                'Row count guard for partition "' ~ partition_id ~ '":\n\ttarget: ' ~ target_rows ~
                '\n\ttmp: ' ~ tmp_rows, silence_mode) -}}

        {%- if target_rows != tmp_rows -%}
            {%- do exceptions.raise_compiler_error(
                    diu.mcr_log_colored(
                        'ATTACH row-count mismatch on partition "' ~ partition_id ~ '":\n' ~
                        '\ttarget (' ~ existing_relation ~ '): ' ~ target_rows ~ ' rows\n' ~
                        '\ttmp (' ~ intermediate_relation ~ '): ' ~ tmp_rows ~ ' rows\n' ~
                        'This indicates a duplicated ATTACH (e.g. a retried statement after a dropped connection).\n' ~
                        'Aborting before the delete/insert/REPLACE PARTITION steps would make the duplicate ' ~
                        'undetectable to check_duplicate_parts.', color='red')) -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro check_untouched_rows_match(existing_relation, intermediate_relation, partition_id, output_datetime_column, start_time, silence_mode) -%}
{#
    Last guard before REPLACE PARTITION. The run rewrites only rows at or after start_time; rows
    older than that reach intermediate_relation by copy_partition alone, so both relations must
    hold the same number of them. check_partition_row_count_match cannot see an extra copy that
    lands after it has run, and check_duplicate_parts compares parts by hash_of_all_files, which
    stops matching once a merge or the DELETE mutation regroups one copy differently from the
    other. On 2026-10-07 that left a doubled slice in fct_order_run_events and
    fct_dataflow_run_events and a doubled month in int_order_run_events_incremental_billing_calc.
    Counting rows is independent of how the parts are laid out.
    Arguments:
        existing_relation(api.Relation):       The relation the partition was copied from
        intermediate_relation(api.Relation):   The relation about to replace that partition
        partition_id(string):                  The partition id that was copied
        output_datetime_column(string):        The model's output datetime column
        start_time(datetime):                  Rows at or after it are rewritten by this run
        silence_mode(bool):                    Should the log messages be silenced
    Returns:
        None (raises a compiler error on mismatch)
#}
    {%- set diu = dbt_improvado_utils -%}

    {#-
        Untouched rows = all rows of the partition (system.parts, metadata only) minus the rows
        at or after start_time. Counting the tail instead of the untouched rows themselves keeps
        the scan to the last overwrite window: on Lisbon Palantir (2026-10-13) the direct count
        read the whole partition - 120M rows, 7-9 s per relation on fct_order_run_events and
        fct_dataflow_run_events - while the tail count reads ~4M rows in about 1 s and gives the
        same number.
    -#}
    {%- set row_counts_query -%}
        select
            (select sum(rows) from system.parts
             where database = '{{ existing_relation.schema }}'
               and table = '{{ existing_relation.identifier }}'
               and partition_id = '{{ partition_id }}'
               and active)
            - (select count() from {{ existing_relation.schema }}.{{ existing_relation.identifier }}
               where _partition_id = '{{ partition_id }}'
                 and {{ output_datetime_column }} >= toDateTime('{{ start_time }}')) as target_rows,
            (select sum(rows) from system.parts
             where database = '{{ intermediate_relation.schema }}'
               and table = '{{ intermediate_relation.identifier }}'
               and partition_id = '{{ partition_id }}'
               and active)
            - (select count() from {{ intermediate_relation.schema }}.{{ intermediate_relation.identifier }}
               where _partition_id = '{{ partition_id }}'
                 and {{ output_datetime_column }} >= toDateTime('{{ start_time }}')) as tmp_rows
    {%- endset -%}

    {%- if execute -%}
        {%- set result = run_query(row_counts_query).rows[0] -%}
        {%- set target_rows = result['target_rows'] or 0 -%}
        {%- set tmp_rows = result['tmp_rows'] or 0 -%}

        {#- mcr_log_colored needs `this`, which a bare run-operation (the pytest for this macro) lacks -#}
        {%- if this is defined -%}
            {{- diu.mcr_log_colored(
                    'Untouched rows guard for partition "' ~ partition_id ~ '" before ' ~ start_time ~
                    ':\n\ttarget: ' ~ target_rows ~ '\n\ttmp: ' ~ tmp_rows, silence_mode) -}}
        {%- endif -%}

        {%- if target_rows != tmp_rows -%}
            {%- set mismatch_message =
                    'Untouched rows mismatch on partition "' ~ partition_id ~ '" before ' ~ start_time ~ ':\n' ~
                    '\ttarget (' ~ existing_relation.schema ~ '.' ~ existing_relation.identifier ~ '): ' ~ target_rows ~ ' rows\n' ~
                    '\ttmp (' ~ intermediate_relation.schema ~ '.' ~ intermediate_relation.identifier ~ '): ' ~ tmp_rows ~ ' rows\n' ~
                    'This run only rewrites rows at or after start_time, so the older rows in tmp ' ~
                    'were duplicated or lost on the way.\n' ~
                    'Aborting before REPLACE PARTITION; target is left as it was.' -%}
            {%- if this is defined -%}
                {{- diu.mcr_log_colored(mismatch_message, true, color='red') -}}
            {%- endif -%}
            {%- do exceptions.raise_compiler_error(mismatch_message) -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_partition_id(datetime_object, partition_by_format) -%}
{#
    Creates partition id by the given datetime object
    Arguments:
        datetime_object(datetime):      The datetime object
        partition_by_format(string):    The partition format extracted from config setting
    Returns:
        Returns the partition id in one of the following formats: yyyy, yyyymm, yyyymmdd
#}
    {%- set strftime_pattern = {
            'yyyy': '%Y',
            'yyyymm': '%Y%m',
            'yyyymmdd': '%Y%m%d'} -%}

    {{ - return(datetime_object.date().strftime(strftime_pattern[partition_by_format])) -}}
{%- endmacro -%}

{%- macro exchange_tables(source_relation, target_relation) -%}
{#
    Exchanges names of source and target relations
    Arguments:
        source_relation(api.Relation):   The source relation
        target_relation(api.Relation):   The target relation
    Returns:
        None
#}
    {%- call statement('exchange_tables') -%}
        exchange tables {{ source_relation }} and {{ target_relation }}
    {%- endcall -%}
{%- endmacro -%}

{%- macro drop_part(relation, part_name) -%}
{#
    Drops a specific part from a table
    Arguments:
        relation(api.Relation):     The relation to drop the part from
        part_name(string):          The name of the part to drop
    Returns:
        None
#}
    {#- schema.identifier instead of the bare relation: a run-operation (the pytest for
        check_duplicate_parts) passes a plain {schema, identifier} dict, which api.Relation
        rendering would turn into its Python repr -#}
    {%- call statement('drop_part') -%}
        alter table {{ relation.schema }}.{{ relation.identifier }} drop part '{{ part_name }}'
    {%- endcall -%}
{%- endmacro -%}

{%- macro wait_for_merges(relation, max_retries=30, sleep_seconds=3) -%}
{#
    Polls system.merges until no active merges are running on the table,
    preventing PART_IS_TEMPORARILY_LOCKED errors on partition operations.
    Does not require SYSTEM STOP MERGES permissions.
    Arguments:
        relation(api.Relation):     The relation to wait on
        max_retries(int):           Maximum number of polling attempts (default 30)
        sleep_seconds(int):         Seconds to wait between polls (default 2)
    Returns:
        None
#}
    {%- set check_merges_query -%}
        select count()
        from system.merges
        where database = '{{ relation.schema }}'
          and table = '{{ relation.identifier }}'
    {%- endset -%}

    {%- for i in range(max_retries) -%}
        {%- set merge_count = run_query(check_merges_query)[0][0] -%}
        {%- if merge_count == 0 -%}
            {%- break -%}
        {%- endif -%}
        {%- do run_query('select sleep(' ~ sleep_seconds ~ ')') -%}
    {%- endfor -%}
{%- endmacro -%}

{%- macro cleanup_stale_microbatch_tmp_relations(schema, base_identifier, keep_identifier, silence_mode) -%}
{#
    Drops leftover per-invocation tmp tables from previous runs of this model that crashed
    or were killed before reaching their own drop_relation cleanup at the end of the
    materialization. Runs before this invocation creates its own tmp table so orphaned tmp
    tables don't accumulate now that each run's tmp identifier is invocation-scoped.
    Arguments:
        schema(string):           The schema to search in
        base_identifier(string):  The target model's identifier (without the tmp suffix)
        keep_identifier(string):  This invocation's own tmp identifier - never drop it
        silence_mode(bool):       Should the log messages be silenced
    Returns:
        None
#}
    {%- set diu = dbt_improvado_utils -%}

    {%- set stale_tmp_tables_query -%}
        select name
        from system.tables
        where database = '{{ schema }}'
          and name like '{{ base_identifier }}\_\_microbatch\_tmp\_%'
          and name != '{{ keep_identifier }}'
    {%- endset -%}

    {%- if execute -%}
        {%- set stale_tables = run_query(stale_tmp_tables_query).rows -%}

        {%- for row in stale_tables -%}
            {{- diu.mcr_log_colored(
                    'Dropping stale microbatch tmp table left over from a previous run: ' ~ row['name'],
                    silence_mode, color='red') -}}

            {%- set _, stale_relation =
                        get_or_create_relation(
                            database=none,
                            schema=schema,
                            identifier=row['name'],
                            type='table') -%}
            {%- do drop_relation(stale_relation) -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro check_duplicate_parts(relation, silence_mode, action='drop') -%}
{#
    Checks for duplicate parts (same hash) and, depending on action, deletes them keeping only
    one copy ('drop') or only reports them and leaves the relation as it is ('report').
    Arguments:
        relation(api.Relation):     The relation to check
        silence_mode(bool):         Should the log messages be silenced
        action(string):             'drop' (default) or 'report' (BI-10065: a model whose target
                                    already holds duplicate parts must keep its history as it is
                                    until the duplicates are dealt with explicitly)
    Returns:
        None
#}
    {%- set diu = dbt_improvado_utils -%}

    {%- if action not in ('drop', 'report') -%}
        {%- do exceptions.raise_compiler_error(
                    "check_duplicate_parts: action must be 'drop' or 'report', got '" ~ action ~ "'") -%}
    {%- endif -%}

    {%- set check_duplicates_query -%}
        select
            partition_id,
            hash_of_all_files,
            groupArray(name) as part_names
        from
            system.parts
        where
            database = '{{ relation.schema }}'
            and table = '{{ relation.identifier }}'
            and active = 1
            -- AI-1668: ignore empty parts. Since ClickHouse 26.3 (PR #91574, dedicated cleanup thread)
            -- a DELETE mutation that removes every row of a part leaves a 0-row part active for
            -- 30s-5min instead of ~1s. All empty parts share one hash_of_all_files, so they look like
            -- duplicates here; DROP PART on them races the cleanup thread and fails with code 232
            -- (NO_SUCH_DATA_PART). Empty parts hold no rows, so skipping them cannot lose or
            -- duplicate data; ClickHouse removes them itself.
            and rows > 0
        group by
            partition_id,
            hash_of_all_files
        having
            count() > 1
    {%- endset -%}

    {%- set duplicate_groups = run_query(check_duplicates_query).rows -%}

    {%- if duplicate_groups -%}
        {%- do diu.wait_for_merges(relation) -%}
        {%- set duplicate_groups = run_query(check_duplicates_query).rows -%}
    {%- endif -%}

    {%- for dup_group in duplicate_groups -%}
        {%- set dup_partition_id = dup_group['partition_id'] -%}
        {%- set dup_hash = dup_group['hash_of_all_files'] -%}
        {%- set part_names = fromjson(dup_group['part_names']) -%}
        {%- set parts_to_delete = part_names[1:] -%}

        {#- mcr_log_colored needs `this`, which a bare run-operation (the pytest for this macro) lacks -#}
        {%- if this is defined -%}
            {{- diu.mcr_log_colored(
                    'Detected ' ~ (part_names | length) ~ ' duplicate parts in partition '~
                    dup_partition_id ~ ' with hash ' ~ dup_hash ~ ' in ' ~ relation ~ ': ' ~
                    (part_names | join(', ')) ~
                    (' - deleting duplicates' if action == 'drop' else ' - kept (duplicate_parts_action = report)'),
                    silence_mode, color='red') -}}
        {%- endif -%}

        {%- if action == 'drop' -%}
            {%- for part_name in parts_to_delete -%}
                {%- do diu.drop_part(relation, part_name) -%}
            {%- endfor -%}
        {%- endif -%}

    {%- endfor -%}

{%- endmacro -%}
