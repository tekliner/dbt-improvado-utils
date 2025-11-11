{%- macro mcr_log_colored(message, output_enabled=true, color='yellow') -%}
{#
    Makes log message colored.
    Arguments:
        message(string):        The log message to be colored
        output_enabled(bool):   Should the log message be printed
        color(string):          The color of the log message
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

    {{- log(this.identifier ~ ' log:' ~ color_code_start ~ color_code ~ message ~ '\033[00m', output_enabled) -}}
{%- endmacro -%}
