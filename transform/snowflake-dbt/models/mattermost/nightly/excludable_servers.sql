{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key":'server_id'
  })
}}

WITH seed_file AS (
    SELECT *
    FROM {{ ref('excludable_servers_seed') }}
),

license_exclusions AS (
    SELECT
        'internal_license' AS reason
      , s.server_id
    FROM {{ ref('server_fact') }} s
    JOIN {{ ref('licenses') }} l
        ON s.server_id = l.server_id
        AND trim(l.company) in (
                            'Mattermost Developers',
                            'Mattermost SA - INTERNAL',
                            'Mattermost  Inc.',
                            'Mattermost Cloud',
                            'Mattermost'
                            )
    LEFT JOIN seed_file sf
        ON s.server_id = sf.server_id
    WHERE sf.server_id is null
    GROUP BY 1, 2
),

excludable_servers AS (
    SELECT *
    FROM seed_file
    UNION ALL
    SELECT *
    FROM license_exclusions
)
SELECT *
FROM excludable_servers
{% if is_incremental() %}

WHERE server_id NOT IN (SELECT server_id from {{ this }})

{% endif %}