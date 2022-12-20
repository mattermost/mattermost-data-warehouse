{{config({
    "materialized": "table",
    "schema": "data_quality",
    "tags": "data-quality"
  })
}}


WITH sso_installations AS (
    SELECT
        installation_id
    FROM
        {{ref('cs_signup_campaign_facts')}}
    WHERE
        sso_provider IS NOT NULL
),
sso_cloud_servers AS (
    SELECT
      sf.server_id
      , sf.installation_id
      , SPLIT_PART(license_email, '@', 2) AS email_domain
      , COALESCE(sf.first_active_date, CURRENT_DATE) AS first_active_date
      , sf.last_ip_address
    FROM
        {{ref('server_fact')}} sf
        JOIN {{ref('license_server_fact')}} lsf ON sf.server_id = lsf.server_id
        LEFT JOIN {{ref('excludable_servers')}} es ON sf.server_id = es.server_id
    WHERE
        -- Skip excludable servers
        es.reason IS NULL
        -- Use servers only after 2020-02-01
        AND sf.first_active_date::DATE >= '2020-02-01'
        AND sf.installation_id IS NOT NULL
)
SELECT
    cs.server_id,
    i.installation_id,
    cs.first_active_date,
    cs.email_domain,
    CASE WHEN cb.server_id IS NULL THEN 0 ELSE 1 END AS is_enriched,
    -- Expect either company or person id to be there. If both are missing, then no data should be there.
    CASE WHEN COALESCE(cb.company_id, cb.person_id) IS NULL THEN 1 ELSE 0 END AS is_missing
FROM
    sso_installations i
    LEFT JOIN sso_cloud_servers cs ON i.INSTALLATION_ID = cs.INSTALLATION_ID
    LEFT JOIN {{source('mattermost', 'cloud_clearbit')}} CB ON cs.server_id = cb.server_id
