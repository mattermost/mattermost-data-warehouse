{% set months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] %}

with release_versions as (
    select
        short_version,
        lag(short_version, 1) over(order by release_number asc) as previous_release_version,
        lag(short_version, -1) over(order by release_number asc) as next_release_version
    from
         {{ ref('stg_mattermost__version_release_dates') }}
    where
        version_patch = 0
), fix_versions as (
    -- Implement filtering on this layer as it's only used here.
    select
        issue_id,
        value:name::string as fix_version,
        -- Break down different variations of target version
        regexp_substr(fix_version, 'v\\d+\.\\d+') as semver,
        regexp_substr(fix_version, 'v(\\d+)', 1, 1, 'e', 1)::int as version_major,
        regexp_substr(fix_version, '\\.(\\d+)', 1, 1, 'e', 1)::int as version_minor,
        regexp_substr(fix_version, '\\.(\\d+)', 1, 2, 'e', 1)::int as version_patch,
        case
            when fix_version ilike '%mobile%' then 'Mobile'
            when fix_version ilike '%desktop%' then 'Desktop'
            when fix_version ilike '%playbooks%' then 'Playbooks'
            when fix_version ilike '%ir%' then 'IR'
            when fix_version ilike '%cloud%' then 'Cloud'
            when fix_version ilike '%apps%' then 'Apps'
        end as component,
        to_date(regexp_substr(fix_version, '\\d{2}/\\d{2}/\\d{2}'), 'mm/dd/yy') as cloud_release_date,
        fix_version like any (
            {%- for month in months %}
                'v%({{month}}%)'{%- if not loop.last %},{% endif -%}
            {% endfor %}
        ) and component is null as is_on_prem_release,
        fix_version like 'Cloud%v%' as is_cloud_release
    from
        {{ ref('stg_mattermost_jira__issues') }},
        lateral flatten(input => fix_versions)
    where
        -- Keep only relevant fix versions - ones that contain a version in the form `v[major].[minor]`
        regexp_like(fix_version, '.*v\\d+\.\\d+.*', 'i')
)
select
    fv.issue_id,
    fv.fix_version,
    fv.semver,
    fv.version_major,
    fv.version_minor,
    fv.version_patch,
    fv.component,
    fv.cloud_release_date,
    fv.is_on_prem_release,
    fv.is_cloud_release,
    rv.previous_release_version,
    rv.next_release_version
from
    fix_versions fv
    left join release_versions rv on fv.fix_version = rv.short_version