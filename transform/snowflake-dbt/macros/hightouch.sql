{% macro transform_ownerid(ownerid) %}
    CASE WHEN SUBSTR({{ ownerid }}, 0, 3) = '00G'
        THEN {{ var('salesforce_default_ownerid') }}
        ELSE COALESCE({{ ownerid }}, {{ var('salesforce_default_ownerid') }})
    END AS ownerid
{% endmacro %}