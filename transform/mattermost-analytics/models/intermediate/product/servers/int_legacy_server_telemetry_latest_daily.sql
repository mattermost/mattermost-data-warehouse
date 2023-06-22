{{
    config({
        "materialized": "table",
    })
}}
select
    server_id,
    CAST(timestamp AS date) AS server_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} AS id,
    count_system_admins,
    version_full,
    version_major,
    version_minor,
    version_patch,
    operating_system,
    database_type,
    database_version,
    edition,
    -- Can be used to identify potential upgrade/upgrade attempts or erroneous data
    count(distinct version_full) over (partition by server_id, server_date) as count_versions_in_date
from
    {{ ref('stg_mattermost2__server') }}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1