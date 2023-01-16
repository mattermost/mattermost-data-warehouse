{{config({
    "materialized": "table",
    "schema": "data_quality",
    "tags": "data-quality"
  })
}}


WITH on_prem_servers AS (
    -- Get a list of OnPrem servers. Excludable servers are ignored.
    SELECT
        sf.SERVER_ID
        , sf.INSTALLATION_ID
        , COALESCE(sf.FIRST_ACTIVE_DATE, CURRENT_DATE) AS FIRST_ACTIVE_DATE
        , sf.LAST_IP_ADDRESS
    FROM
        {{ref('server_fact')}} sf
        LEFT JOIN {{ref('excludable_servers')}} es ON sf.SERVER_ID = es.SERVER_ID
    WHERE
        -- Skip excludable servers
        es.reason IS NULL
        -- Use servers only after 2020-02-01
        AND sf.first_active_date::DATE >= '2020-02-01'
        AND sf.installation_id IS NULL
        -- Use servers with last IP address
        -- Note that this isn't necessarily the IP address that was used when
        -- retrieving data from clearbit.
        AND NULLIF(sf.last_ip_address, '') IS NOT NULL
    GROUP BY
        1, 2, 3, 4
),
exceptions AS (
    -- Get number of failures to enrich per servers.
    SELECT
        server_id,
        COUNT(*) AS total_exceptions
    FROM
        {{source('staging', 'clearbit_onprem_exceptions')}}
    GROUP BY 1
)
SELECT
    op.server_id,
    op.first_active_date,
    -- Mark servers with no clearbit data
    CASE WHEN oc.server_id IS NULL THEN 1 ELSE 0 END AS is_missing,
    -- Mark servers with no clearbit data due to exception
    CASE WHEN ce.server_id IS NULL THEN 1 ELSE 0 END AS has_exception
FROM
    on_prem_servers op
    LEFT JOIN {{source('mattermost', 'onprem_clearbit')}} oc ON op.server_id = oc.server_id
    LEFT JOIN exceptions ce ON op.server_id = ce.server_id
