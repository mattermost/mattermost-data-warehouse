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
) SELECT server_id
        , license_id
        , customer_id
        , installation_id
        , NULL AS name
        , NULL as site_url
        , NULL as email
        , edition as edition
        , users
        , issued_date
        , start_date
        , expire_date
    FROM 
    telemetry_licenses 
UNION 
    SELECT server_id
        , license_id
        , NULL AS customer_id
        , NULL AS installation_id
        , name
        , site_url
        , email
        , edition
        , users
        , issued_date
        , start_date
        , expire_date 
    FROM
    trial_requests