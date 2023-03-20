{{
    config({
        "cluster_by": ['event_date'],
        "tags": ['nightly']
    })
}}


SELECT
     {{ dbt_utils.generate_surrogate_key(['key']) }} AS issue_id,
    key,
    created_at,
    CASE
        WHEN status_name IN ('Closed', 'Done') THEN updated_at
        ELSE NULL
    END AS closed_at,
    status_name AS status,
    resolution AS resolution,
    {{ datediff("closed_at", "created_at", "day") }} AS lead_time_in_days
FROM
    {{ ref('stg_mattermost_jira__issues') }}
