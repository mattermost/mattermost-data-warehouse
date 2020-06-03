{{config({
    "materialized": "incremental",
    "schema": "staging",
  })
}}

{% set mobile_events = dbt_utils.get_query_results_as_dict("select table_catalog, table_schema, table_name from" ~ source('information_schema_load', 'tables') ~ "where table_schema = 'MM_MOBILE_PROD' and TABLE_NAME NOT IN ('IDENTIFIES', 'RUDDER_DISCARDS', 'TRACKS', 'SCREENS', 'USERS') ") %}
select
  *
from {{ source('information_schema_load','tables') }}
    where table_schema = 'RAW'
    {% for table_name in mobile_events %}
  AND table_name = {{ table_name }}
{% endfor %}