{% macro validate_email(column) -%}
    {%-
        set invalid_chars = ['\\\\', '/', '(', ')', '&', '$', '^', '&', '!']
    -%}
    {{ column }} LIKE '%_@__%.__%' and {{ column }} not like '%@%@%'
    {% for char in invalid_chars %}
        and {{ column }} not like '%{{char}}%'
    {%- endfor %}
{%- endmacro%}