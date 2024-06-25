-- Test that each URI is mapped to a single value for download type and operating system.
select
    uri
    , count(distinct download_type) as download_type_value_count
    , count(distinct operating_system) as operating_system_value_count
from
    {{ ref('fct_downloads') }}
group by uri
having
    count(distinct download_type) > 1
    or count(distinct operating_system) > 1