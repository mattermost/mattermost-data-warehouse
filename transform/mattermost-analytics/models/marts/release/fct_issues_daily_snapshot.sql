with release_versions as (
    -- Keep releases
    select
        version,
        short_version,
        rc1_date,
        planned_release_date,
        actual_release_date,
        -- Calculate 17th of month before the release's month. This is required as release timeframe is 17th to planned
        -- release date.
        dateadd(month, -1, planned_release_date) as _month_before_release,
        dateadd(day, 17 - DAYOFMONTH(_month_before_release), _month_before_release) as release_timeframe_start,
        dateadd(day, 7, actual_release_date) as week_after_release
    from
        {{ ref('stg_mattermost__version_release_dates') }}
    where
        rc1_date is not null
)
select
    ji.issue_id,
    ji.issue_key,
    ji.project_id,
    ji.created_at,
    case
        when ji.status_name in ('Closed', 'Done') then ji.updated_at
    end as closed_at,
    ji.issue_type_name as issue_type,
    ji.status_name as status,
    ji.resolution_name as resolution,
    {{ datediff("ji.created_at", "closed_at", "day") }} as lead_time_in_days,
    rt.version as release_timeframe_version,
    rt.version is not null and ji.created_at >= rt.rc1_date and ji.created_at <= rt.planned_release_date as is_created_after_rc1_cut,
    ar.version as is_created_week_after_version
from
    {{ ref('stg_mattermost_jira__issues') }} ji
    -- Release timeframe
    left join release_versions rt
        on ji.created_at > rt.release_timeframe_start and ji.created_at <= rt.planned_release_date
    -- Week after release
    left join release_versions ar
        on  ji.created_at >= ar.actual_release_date and ji.created_at <= ar.week_after_release
