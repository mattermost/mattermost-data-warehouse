with release_versions as (
    -- Keep releases
    select
        version,
        short_version,
        rc1_date,
        planned_release_date,
        actual_release_date,
        release_start_date as release_timeframe_start,
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
    -- Degenerate dimension
    coalesce(ji.environment, 'Undefined') as environment,
    {{ datediff("ji.created_at", "closed_at", "day") }} as lead_time_in_days,
    -- Reference to release version if issue created during a release timeframe
    rt.version as release_timeframe_version,
    -- Flag whether the issue has been created after RC1 cut date
    rt.version is not null and ji.created_at >= rt.rc1_date and ji.created_at <= rt.planned_release_date as is_created_after_rc1_cut,
    -- Reference to release version if issue created during the week after the actual release date
    ar.version as created_after_release_version
from
    {{ ref('stg_mattermost_jira__issues') }} ji
    -- Release timeframe
    left join release_versions rt
        on ji.created_at >= rt.release_timeframe_start and ji.created_at <= rt.planned_release_date
    -- Week after release
    left join release_versions ar
        on  ji.created_at >= ar.actual_release_date and ji.created_at <= ar.week_after_release
