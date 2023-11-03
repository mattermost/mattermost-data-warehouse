with release_versions as (
    select
        short_version,
        lag(short_version, 1) over(order by release_number asc) as previous_release_version,
        lag(short_version, -1) over(order by release_number asc) as next_release_version
    from
        {{ ref('stg_mattermost__version_release_dates') }}
    where
        rc1_date is not null
)
select
    {{ dbt_utils.star(ref('stg_mattermost__version_release_dates'), relation_alias='rd')}},
    rv.previous_release_version,
    rv.next_release_version
from
    {{ ref('stg_mattermost__version_release_dates') }} rd
    join release_versions rv on rd.short_version = rv.short_version