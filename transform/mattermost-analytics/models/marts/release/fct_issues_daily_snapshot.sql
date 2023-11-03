with release_versions as (
    -- Keep releases
    select
        version,
        short_version,
        rc1_date,
        planned_release_date,
        actual_release_date
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
    {{ datediff("ji.created_at", "ji.closed_at", "day") }} as lead_time_in_days,
    rt.version as release_timeframe_version,
    rt.versions is not null and ji.created_at >= rt.rc1_date and ji.created_at <= rt.planned_release_date as is_created_after_rc1_cut,
    ar.version as is_created_week_after_version
from
    {{ ref('stg_mattermost_jira__issues') }} ji
    -- Release timeframe
    left join release_versions rt
        on ji.created_at > dateadd(month, -1, rv.planned_release_date) and ji.created_at <= rt.planned_release_date
    -- Week after release
    left join release_versions ar
        on  ji.created_at >= ar.actual_release_date and ji.created_at <= dateadd(day, 7, ar.actual_release_date)
