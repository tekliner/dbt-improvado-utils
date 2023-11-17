{# README

This table materialization makes several small inserts into your new table instead of one big insert.

Inputs:

count_of_parts (UInt):
count_of_parts is how many small inserts (or parts) there should be.
More parts means more time to build the model. Below 10 is optimal.

Parent_model (String):
Literally parent model relative to your current model you are working on.
Rows in parent model will be processed by parts and will be inserted by parts into the current model.
If your current model has only one parent - you are safe to simply copy the model name from INPUT section
which is usually {{ ref('model_name') }}.
If your current model has more than one parent - you should take the largest parent,
meaning it is the one that contributes the most rows to your current model.
For example, if one of the parents has a left join with another parent,
you should take the left table from that join CTE.  #}

{% materialization table_by_parts, adapter='clickhouse' %}

-- Info about dbt-core macros here:
-- https://github.com/dbt-labs/dbt-core/tree/main/core/dbt/include/global_project/macros/adapters

-- input data fields
  {%  set parent_model             = config.get( 'parent_model' ) -%}
  {%  set parent_relation          = schema ~'.'~ parent_model %}
  {%  set count_of_parts           = config.get( 'count_of_parts', default = 10 ) -%}
  {%  set parts_by_column          = config.get( 'parts_by_column', default = 'rowNumberInAllBlocks()' ) -%}

  {{ log('Initializing materialization with the following settings:
  \n\t  Count of Parts = ' ~ count_of_parts ~ 
  '\n\t  Parent Model = ' ~ parent_model ~ 
  '\n\t  Parent Relation = ' ~ parent_relation ~ 
  '\n\t  Parts by Column = ' ~ parts_by_column ~ '\n', info=True) }}

-- Define relations

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set backup_relation = none -%}

  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {% endif %}

  --------------------------------------------------------------------------------------------------------------------

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

 --------------------------------------------------------------------------------------------------------------------

  -- Create empty target relation 
  {{ drop_relation_if_exists(target_relation) }}

  {%- set parent_model_parts_limits = dbt_improvado_utils.mcr_get_parent_model_parts_limits(parent_model, parts_by_column, count_of_parts) -%}

  {% set target_relation_exists, target_relation = 
                    dbt_improvado_utils.get_or_create_or_update_relation (  database = none, 
                                                        schema = model.schema, 
                                                        identifier = this.identifier, 
                                                        type = 'table', 
                                                        update = False, 
                                                        temporary = False, 
                                                        sql = dbt_improvado_utils.select_limit_0(sql), 
                                                        debug_mode = False, 
                                                        silence_mode = False) %}

  -- Run sql queries by queue
  {% for current_part in parent_model_parts_limits %}

    {{ log('Insert ' ~ current_part['part_id'] ~ ' of ' ~ count_of_parts, not silence_mode) }}

    {%- if loop.last -%}
      {%- set last_part = True -%}
    {%- else -%}
      {%- set last_part = False -%}
    {%- endif -%}

    {% set target_part_query = dbt_improvado_utils.insert_as(sql, target_relation, parent_relation, parts_by_column, 
                                         current_part['part_start'], current_part['part_end']
                                         , last_part) %}

    {% call statement(  'append rows to target') -%}
        {{ target_part_query }}  
    {%- endcall -%}

  {% endfor %}

--------------------------------------------------------------------------------------------------------------------

  {% call noop_statement('main', 'Done') -%} {%- endcall %}

  {{ drop_relation_if_exists(backup_relation) }}

  -----------------------------------------------------

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

-------------------------------------------------------

{% macro get_sql_part_for_insert(sql, parent_relation, parts_by_column, part_start, part_end, isLast=False) %}
        
  {{ log('Processed rows from ' ~ part_start|round(0, 'ceil')|int ~ ' to ' ~ part_end|int ~ '\n', not silence_mode) }}

	{% set parent_relation_with_where_condition %}
		( select      *
			from        {{parent_relation}} 
			where       {{parts_by_column}} >= {{part_start}} 
      {% if not isLast -%}
        and {{parts_by_column}} < {{part_end}}
      {%- endif -%}
		)
	{% endset %}

	{% set parent_relation_part_insert %}
		WITH            _sql as ({{ sql | replace( parent_relation, parent_relation_with_where_condition )}})
		SELECT          *
		FROM            _sql
	{% endset %}

  {{ return (parent_relation_part_insert) }}

{% endmacro %}


-------------------------------------------------------

{% macro insert_as(sql, target_relation, parent_relation, parts_by_column, part_start, part_end, isLast=False) %}
        
    {% set inserting_sql = dbt_improvado_utils.get_sql_part_for_insert(sql, parent_relation, parts_by_column, part_start, part_end, isLast) %}

    INSERT INTO {{target_relation}} {{inserting_sql}}

    {% set target_relation_part_insert %}
        INSERT INTO {{target_relation}} {{inserting_sql}}
    {% endset %}

    {{ return (target_relation_part_insert)}}
    
{% endmacro %}


-------------------------------------------------------

{%- macro mcr_get_parent_model_parts_limits(input_table_name, parts_by_column, count_of_parts=10) -%}

{% set quantiles_table = dbt_improvado_utils.get_quantiles_table(input_table_name, parts_by_column, count_of_parts) %}

{% if execute %}
 {%- set query -%}
 WITH

 union_parts as (

 {% for current_part in range(count_of_parts) %}
  SELECT
   {{ quantiles_table.rows[current_part]['part_id'] }}              as part_id, 

   {% if current_part == 0 -%}
    {{ quantiles_table.rows[current_part]['start_of_first_part'] }}
   {%- else -%}
    {{ quantiles_table.rows[current_part-1]['borderline_between_parts'] }}
   {%- endif %}                                                   as part_start,

   {% if current_part+1 == count_of_parts -%}
    {{ quantiles_table.rows[current_part]['finish_of_last_part'] }}
   {%- else -%}
    {{ quantiles_table.rows[current_part]['borderline_between_parts'] }} 
   {%- endif %}                                                   as part_end
   
  {% if not loop.last %}
   UNION ALL
  {% endif %}

 {% endfor %}
 )

 SELECT *
 FROM union_parts
 ORDER BY part_id

 {% endset %}

{% endif %} {# end if execute #}
    
{%- set query_result = run_query(query) -%}

{{ return(query_result) }}

{%- endmacro -%}


-- MACROS get_quantiles_table() ------------------------------------------------

{% macro get_quantiles_table(input_table_name, parts_by_column, count_of_parts) %}

{% if execute %}

 {%- set query_text -%}
 WITH 
 
 quantiles_table as (
  SELECT 
   min({{ parts_by_column }})                                     as start_of_first_part,
   max({{ parts_by_column }})                                     as finish_of_last_part,
   arrayStringConcat(quantiles(
   {% for current_part in range(count_of_parts) %}
    {{ current_part+1 }} / {{ count_of_parts }}
    {% if not loop.last %},{% endif %}
   {% endfor %}
    )({{ parts_by_column }}), ',')                     as quantiles_array
  FROM
   {{ ref(input_table_name) }})

 SELECT 
  part_id,
  start_of_first_part,
  finish_of_last_part,
  single_quantile                                                  as borderline_between_parts
 FROM 
  quantiles_table
  ARRAY JOIN splitByChar(',', quantiles_array)                     as single_quantile, 
    arrayEnumerate(splitByChar(',', quantiles_array))              as part_id
 {%- endset -%}

{% else %} {# if not execute #}

 {%- set query_text -%}
 SELECT 
  0                                                              as part_id,
  '0'                                                            as start_of_first_part,
  '0'                                                            as finish_of_last_part,
  '0'                                                            as borderline_between_parts
 {%- endset -%}

{% endif %} {# end if execute #}

{% set result = run_query(query_text) %}

{{ return(result) }}

{% endmacro %}
