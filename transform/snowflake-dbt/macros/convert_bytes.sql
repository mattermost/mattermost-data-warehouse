{% macro convert_bytes(bytes, output, precision=2) -%}
    {%- if output == 'b' %}
        {{bytes}}
    {%- elif output == 'kb' %}
        ({{bytes}} / 1024)::numeric(16,{{precision}})
    {%- elif output == 'mb' %}
        ({{bytes}} / 1048576)::numeric(16, {{precision}})
    {%- elif output == 'gb' %}
        ({{bytes}} / 1073741824)::numeric(16, {{precision}})
    {% endif -%}
{%- endmacro%}