{% macro is_blacklisted_email(column) -%}
    {% set tlds = var('blacklisted_country_tlds') %}
    {{ column }} ILIKE ANY (
    {% for tld in tlds %}
        '%{{ tld }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
    )
{%- endmacro%}