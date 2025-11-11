{%- macro mcr_log_colored(message, silence_mode=false, color='yellow') -%}
{#
    Makes log message colored.
    Arguments:
        message(string):    The log message to be colored
        silence_mode(bool): Should silence mode be used
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

    {{- log(this.identifier ~ ' log:' ~ color_code_start ~ color_code ~ message ~ '\033[00m', not silence_mode) -}}
{%- endmacro -%}
