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
    {%- set on_schema_change                    = config.get('on_schema_change', default='fail') -%}

-- microbatch settings
    {%- set microbatch_settings_pattern         = '\s*--\s*microbatch:\s*(\w+),\s*(\d+)\s*.*`.+`\.`(.+)`\s+(final)?' -%}
    {%- set microbatch_settings                 = re.findall(microbatch_settings_pattern, sql, flags=re.IGNORECASE) -%}

    {%- set input_models_list                   = [] -%}
    {%- set final_settings_list                 = [] -%}
    {%- set input_columns_list                  = [] -%}
    {%- set input_lookback_windows_list         = [] -%}

-- output model data
    {%- set output_datetime_column              = config.require('output_datetime_column') -%}

-- datetime settings
    {%- set materialization_start_date          = dt.datetime.strptime(
                                                    config.require('materialization_start_date'), '%Y-%m-%d') -%}
    {%- set time_unit_name                      = config.require('time_unit_name') -%}
    {%- set batch_size                          = config.require('batch_size') -%}
    {%- set overwrite_size                      = config.get('overwrite_size', default=0) -%}
    {%- set dev_days_offset                     = config.get('dev_days_offset', default=0) -%}
    {%- set fixed_now                           = dt.datetime.now().replace(microsecond=0) -%}

-- other settings
    {%- set partition_by                        = config.require('partition_by') -%}
    {%- set partition_by_format                 = re.findall('(?<=to)\w+(?=\()', partition_by)[0] | lower -%}

-- log settings
    {%- set debug_mode                          = config.get('debug_mode', default=false) -%}
    {%- set silence_mode                        = config.get('silence_mode', default=true) -%}

-- logic ---------------------------------------------------------------------------------------------------------------
    {%- if not microbatch_settings -%}
        {%- do exceptions.raise_compiler_error(
                    diu.log_colored(
                        'No microbatch settings found\n' ~
                        'Please add microbatch settings to the model input section', silence_mode, color='red')) -%}
    {%- endif -%}

-- getting microbatch settings
    {%- for setting in microbatch_settings -%}
        {%- do input_models_list.append(setting[2]) -%}
        {%- do final_settings_list.append(setting[3]) -%}
        {%- do input_columns_list.append(setting[0]) -%}
        {%- do input_lookback_windows_list.append(setting[1] | int) -%}
    {%- endfor -%}

    {{- diu.log_colored(
            'Input models:\n\t' ~ input_models_list | join('\n\t') ~
            '\nFinal is set for:\n\t' ~
                zip(input_models_list, final_settings_list) | selectattr(1) | map(attribute=0) | join('\n\t')  ~
            '\nInput columns:\n\t' ~ input_columns_list | join('\n\t') ~
            '\nInput lookback windows:\n\t' ~ input_lookback_windows_list | join('\n\t') ~
            '\nFixed now time:\n\t' ~ fixed_now, silence_mode) -}}

-- clearing sql from microbatch and final settings
    {%- set sql = re.sub('\s*--\s*microbatch:\s*\w+,\s*\d+', '', sql) -%}
    {%- set sql = re.sub('`(.+)`\.`(.+)`\s+final', '`\\1`.`\\2`', sql, flags=re.IGNORECASE) -%}

    {%- if target_schema == production_schema -%}
        {{- diu.log_colored('Starting to build to production schema: ' ~ target_schema, silence_mode) -}}

    {%- else -%}
        {%- set materialization_start_date =
                    dt.datetime.combine(dt.datetime.today() - dt.timedelta(days=dev_days_offset), dt.time.min) -%}

        {{- diu.log_colored(
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
        {%- if on_schema_change == 'fail' and not full_refresh -%}
            {%- do exceptions.raise_compiler_error(
                    diu.log_colored(
                        'Schema change detected. Materialization will be stopped\n' ~
                        'Please revise the schema changes or set "on_schema_change" to "full_refresh"\n' ~
                        'If you want to force materialization - run with "full-refresh" flag', silence_mode, color='red')) -%}

        {%- elif on_schema_change == 'full_refresh' -%}
            {%- set full_refresh = true -%}
            {{- diu.log_colored(
                    'Full refresh will be done since "on_schema_change" is set to "full_refresh"', silence_mode, color='red') -}}

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

        {{- diu.log_colored(
                'Target relation exists' ~
                '\nLast record datetime:\n\t' ~ last_record_datetime ~
                '\nLast record datetime with overwrite size:\n\t' ~ start_time, debug_mode) -}}

    {%- else -%}
        {%- set start_time = materialization_start_date -%}
        {{- diu.log_colored(
                'Target relation doesn\'t exist' ~
                '\nDefault datetime:\n\t' ~ start_time, debug_mode) -}}

    {%- endif -%}

-- interval counts calculation
    {%- set interval_range =
                diu.get_unit_datediff(
                    startdate=start_time,
                    enddate=fixed_now,
                    unit=time_unit_name) -%}

    {{- diu.log_colored('Calculating interval parts', silence_mode) -}}
    {%- set parts_count = [(interval_range / batch_size) | round(0, 'ceil') | int, 2] | max -%}
    {{- diu.log_colored('Interval parts count:\n\t' ~ parts_count, silence_mode) -}}

-- temporary table creation
    {%- set tmp_relation_exists, tmp_relation =
                get_or_create_relation(
                    database=none,
                    schema=target_schema,
                    identifier=target_relation.identifier ~ '__microbatch_tmp',
                    type='table') -%}

    {%- do drop_relation(tmp_relation) -%}
    {%- do create_table_as(false, tmp_relation.identifier, sql) -%}

    {%- set partition_id = diu.get_partition_id(start_time, partition_by_format) -%}

    {%- if not full_refresh -%}
        {{- diu.log_colored(
                'Copying partition "' ~ partition_id ~ '" from ' ~ target_relation ~ ' to ' ~ tmp_relation, silence_mode) -}}
        {%- do diu.copy_partition(target_relation, tmp_relation, partition_id) -%}

    -- deleting data to be overwritten from tmp relation
        {%- do run_query(
            "delete from " ~ tmp_relation ~ " where " ~ output_datetime_column ~ " >= " ~ "toDateTime('" ~ start_time ~ "')") -%}

    {%- endif -%}

-- insert queries generation
    {%- for i in range(parts_count) -%}
    -- inserting intervals list calculation
        {%- set where_conditions, having_conditions =
                    diu.get_intervals_list(
                        interval_offset=i * batch_size,
                        time_unit_name=time_unit_name,
                        start_time=start_time,
                        lookback_windows_list=input_lookback_windows_list,
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
                        debug_mode=debug_mode) -%}

        {%- if execute -%}
            {%- if loop.first -%}
                {{- diu.log_colored('Inserting into: ' ~ tmp_relation, silence_mode) -}}
            {%- endif -%}

        {{- diu.log_colored(
            'Inserting batch: ' ~ (i + 1) ~ ' out of ' ~ parts_count ~
            '\nDate range: from ' ~  having_conditions[0] ~ ' to ' ~  having_conditions[1], silence_mode) -}}

        {{- diu.log_colored(insert_query[250:1100] ~ '\n\n...\n\n' ~ insert_query[-200:], debug_mode) -}}

        -- insert query execution
            {%- call statement('inserting_new_data_to_temporary_table') -%}
                {{- insert_query -}} 
            {%- endcall -%} 

        {%- endif -%}
    {%- endfor -%}

    {%- if full_refresh -%}
    -- exchanging tmp table and target table
        {{- diu.log_colored('Exchanging ' ~ tmp_relation ~ ' with ' ~ target_relation, silence_mode) -}}
        {{- diu.exchange_tables(tmp_relation, target_relation) -}}
    {%- else -%}
    -- replacing partitions
        {{- diu.log_colored('Replacing partitions from ' ~ tmp_relation ~ ' to ' ~ target_relation, silence_mode) -}}
        {{- diu.insert_overwrite_partitions(target_relation, tmp_relation) -}}
    {%- endif -%}
-- dropping tmp table after replacing or exchanging
    {%- do adapter.drop_relation(tmp_relation) -%}

    {%- call noop_statement('main', 'Done') -%} {%- endcall -%}
    {%- do return ({'relations': sections_arr}) -%}
{%- endmaterialization -%}


{%- macro log_colored(message, silence_mode=false, color='yellow') -%}
{#
    Makes log message colored.
    Arguments:
        message(string):    The log message to be colored
        silence_mode(bool): Should the log message be printed
        color(string):      The color of the log message
    Returns:
        The colored log message
#}
    {%- set color_code_start = '\n\033[0;' -%}

    {%- if color == 'green' -%}
        {%- set color_code = '32m' -%}
    {%- elif color == 'red' -%} 
        {%- set color_code = '31m' -%}
    {%- elif color == 'yellow' -%} 
        {%- set color_code = '33m' -%}
    {%- endif -%}

    {{- log(this.identifier ~ ' log:' ~ color_code_start ~ color_code ~ message ~ '\033[00m', silence_mode) -}}
{%- endmacro -%}


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
        {{- diu.log_colored('Checking existing table columns names and types consistency', debug_mode) -}}

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
            {{- diu.log_colored(
                    'Number of columns doesn\'t match', silence_mode, color='red') -}}

        {%- else -%}
            {{- diu.log_colored(
                    'Number of columns match\nChecking for name and type consistency', silence_mode) -}}

            {%- for i in range(columns_new | length) -%}
                {%- set column_old = columns_old[i] -%}
                {%- set column_new = columns_new[i] -%}

                {%- if column_old.data_type != column_new.data_type or column_old.name != column_new.name -%} 
                    {%- set is_schema_changed.value = true -%}
                    {{- diu.log_colored(
                            'Column name/type mismatch:' ~
                            '\n\told: ' ~ column_old.name ~ ' ' ~ column_old.data_type ~
                            '\n\tnew: ' ~ column_new.name ~ ' ' ~ column_new.data_type, silence_mode, color='red') -}}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}

        {{- diu.log_colored(
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

    {{- diu.log_colored('Checking if relation exists:\n\t' ~ identifier, debug_mode) -}}

    {%- set relation_exists, relation =
                get_or_create_relation(
                    database=database,
                    schema=schema,
                    identifier=identifier,
                    type=type) -%}

-- creating relation if it doesn't exist
    {%- if not relation_exists and execute -%}
        {{- diu.log_colored(
                'Creating non-existing relation:\n\t' ~ identifier, debug_mode) -}}

        {%- if type == 'table' -%}
            {%- do create_table_as(temporary, relation, sql) -%}
        {%- else -%}
            {%- do create_view_as(relation, sql) -%}
        {%- endif -%}

        {{- diu.log_colored(
                'Relation has been created:\n\t' ~ identifier, debug_mode) -}}
    {%- endif -%}

    {{- return ([relation_exists, relation]) -}}
{%- endmacro -%}


{%- macro get_max_datetime(relation, timestamp_column) -%}
{#
    Gets the maximum value of timestamp_column in relation
    Arguments:
        relation(string): Schema and table name
        timestamp_column(string): Column name
    Returns:
        The maximum value of timestamp_column in DateTime format
#}
    {%- set max_datetime -%}
        select
            toDateTime(max({{ timestamp_column }}))
        from
            {{ relation }}
    {%- endset -%}

    {%- if execute -%}
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


{%- macro get_intervals_list(interval_offset, time_unit_name, start_time, lookback_windows_list, batch_size) -%}
{#
    Calculates the list of intervals for each lookback window
    Arguments:
        interval_offset(int):    The offset of the interval
        time_unit_name(string):  The type of interval for result. Possible values: (hour, day)
        start_time(datetime):    The start time
        lookback_window(int):    The lookback window
        batch_size(int):         The batch size
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

    {%- for lookback_window in lookback_windows_list -%}
    -- start date of each batch with lookback window
        {%- set left_where_condition = interval_start - diu.get_unit_interval(value=lookback_window, unit=time_unit_name) -%}
    -- end date of each batch
        {%- set right_where_condition = interval_end -%}

        {%- do lookback_windows_intervals.append([left_where_condition, right_where_condition]) -%}
    {%- endfor -%}

-- where conditions for final select from from CTE
    {%- set left_having_condition = interval_start -%}
    {%- set right_having_condition = interval_end -%}

    {{- return([lookback_windows_intervals, [left_having_condition, right_having_condition]]) -}}
{%- endmacro -%}


{%- macro get_insert_query(sql, target_relation, input_models_list, final_settings_list, input_columns_list, where_conditions, having_conditions, output_column, debug_mode ) -%}
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
