{% macro is_blacklisted_email(column) -%}
    {% set tlds = var('blacklisted_country_tlds') %}
( {{ column }} ILIKE ANY (
    {% for tld in tlds %}
    '%{{ tld }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
    )
    OR
    SPLIT_PART({{ column }}, '@', 2) IN (SELECT email_domain FROM {{ ref('email_domain_blacklist') }})
)
{%- endmacro%}