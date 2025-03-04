{% macro validate_email(column) -%}
    {%-
        set invalid_chars = ['\\\\', '/', '(', ')', '&', '$', '^', '&', '!', ' ', '<', '>', '“', '\\u2006', '\\u00a0', '`']
    -%}
    {{ column }} LIKE '%_@__%.__%' and {{ column }} not like '%@%@%'
    {% for char in invalid_chars %}
        and {{ column }} not like '%{{char}}%'
    {%- endfor %}
{%- endmacro%}