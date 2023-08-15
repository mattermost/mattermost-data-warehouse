with versions as (
    select
        distinct version_full, version_major, version_minor, version_patch
    from
        {{ ref('int_server_active_days_spined') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['version_full']) }} AS version_id,
    version_full,
    version_major,
    version_minor,
    version_major || '.' || version_minor as version_major_minor,
    version_patch
from
    versions