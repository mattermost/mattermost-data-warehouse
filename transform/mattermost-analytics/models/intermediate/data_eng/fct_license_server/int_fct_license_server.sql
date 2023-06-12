WITH telemetry_licenses AS 
(
    SELECT 
        *
    FROM 
       {{ ref('stg_mm_telemetry_prod__license') }}
    UNION
    SELECT 
        *
    FROM 
       {{ ref('stg_mattermost2__license') }}
), 
trial_requests AS 
(
    SELECT 
        *
    FROM 
        {{ ref('stg_blapi__trial_requests') }}
) SELECT 
    DISTINCT user_id as server_id
    , a.license_id
    , a.customer_id
    , a.installation_id
    , NULL AS name
    , NULL as site_url
    , NULL as email
    , a.edition as edition
    , users
    , a.issued_date
    , a.start_date
    , a.expire_date
    FROM 
    telemetry_licenses a 
    UNION 
    SELECT * from trial_requests