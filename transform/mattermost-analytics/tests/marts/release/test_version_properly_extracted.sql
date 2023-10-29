-- Test that major, minor and patch version have been properly extracted by comparing against original string
select
    issue_id,
    fix_version,
    case
        when version_patch is null then version_major || '.' || version_minor
        else version_major || '.' || version_minor || '.' || version_patch
    end as semver
from
    {{ ref('dim_fix_versions') }}
where
    not contains(fix_version, semver)