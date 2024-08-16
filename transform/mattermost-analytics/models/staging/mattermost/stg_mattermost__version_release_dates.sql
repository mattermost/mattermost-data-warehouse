with rd as (
    select
        *
    from
        {{ source('mattermost', 'version_release_dates') }}
)
select
    version,
    'v' || REGEXP_SUBSTR(version, '^\\d+\\.\\d+') as short_version,
    try_to_decimal(split_part(version, '.', 1)) as version_major,
    try_to_decimal(split_part(version, '.', 2)) as version_minor,
    try_to_decimal(split_part(version, '.', 3)) as version_patch,
    release_date::date as planned_release_date,
    supported::boolean as is_supported,
    release_number::int as release_number,
    -- Calculate 17th of month before the release's month. This is required as release timeframe is 17th to planned
    -- release date.
    dateadd(day, 17 - DAYOFMONTH(dateadd(month, -1, planned_release_date)), dateadd(month, -1, planned_release_date)) as release_start_date,
    coalesce(actual_release_date::date, planned_release_date) as actual_release_date,
    rc1_date::date as rc1_date
from
    rd