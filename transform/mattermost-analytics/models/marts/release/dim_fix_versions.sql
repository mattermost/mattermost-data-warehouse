{% set months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] %}

-- Implement filtering on this layer as it's only used here.
select
    issue_id,
    value:name::string as fix_version_name,
    -- Break down different variations of target version
    regexp_substr(fix_version_name, 'v\\d+\.\\d+') as semver,
    regexp_substr(fix_version_name, 'v(\\d+)', 1, 1, 'e', 1)::int as version_major,
    regexp_substr(fix_version_name, '\\.(\\d+)', 1, 1, 'e', 1)::int as version_minor,
    regexp_substr(fix_version_name, '\\.(\\d+)', 1, 2, 'e', 1)::int as version_patch,
    case
        when fix_version_name ilike '%mobile%' then 'Mobile'
        when fix_version_name ilike '%desktop%' then 'Desktop'
        when fix_version_name ilike '%playbooks%' then 'Playbooks'
        when fix_version_name ilike '%ir%' then 'IR'
        when fix_version_name ilike '%cloud%' then 'Cloud'
        when fix_version_name ilike '%apps%' then 'Apps'
    end as component,
    to_date(regexp_substr(fix_version_name, '\\d{2}/\\d{2}/\\d{2}'), 'mm/dd/yy') as cloud_release_date,
    fix_version_name like any (
        {%- for month in months %}
            'v%({{month}}%)'{%- if not loop.last %},{% endif -%}
        {% endfor %}
    ) as is_on_prem_release,
    fix_version_name like 'Cloud%v%' as is_cloud_release
from
    {{ ref('stg_mattermost_jira__issues') }},
    lateral flatten(input => fix_versions)
where
    -- Keep only relevant fix versions - ones that contain a version in the form `v[major].[minor]`
    regexp_like(fix_version_name, '.*v\\d+\.\\d+.*', 'i')
