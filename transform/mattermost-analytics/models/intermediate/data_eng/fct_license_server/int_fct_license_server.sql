WITH telemetry_licenses AS 
(
    SELECT 
        DISTINCT user_id
        , license_id
        , customer_id
        , COALESCE(context_traits_installationid, context_traits_installation_id) as installation_id
        , edition
        , users
        , (to_timestamp(issued/1000)::DATE) as issued_date
        , (to_timestamp(_start/1000)::DATE) as start_date
        , (to_timestamp(expire/1000)::DATE) as expire_date
    FROM 
        {{ source('mm_telemetry_prod', 'license') }}
    UNION
    SELECT 
        DISTINCT user_id
        , license_id
        , customer_id
        , NULL as installation_id
        , edition
        , users
        , (to_timestamp(issued/1000)::DATE) as issued_date
        , (to_timestamp(_start/1000)::DATE) as start_date
        , (to_timestamp(expire/1000)::DATE) as expire_date 
    FROM
        {{ source('mattermost2', 'license') }}
), 
trial_requests AS 
(
    SELECT 
        DISTINCT server_id
        , id as license_id
        , NULL as customer_id
        , NULL as installation_id
        , name
        , site_url
        , email
        , 'E20 Trial' as edition
        , users
        , license_issued_at::date as issued_date
        , start_date::date as start_date
        , end_date::date as expire_date
    FROM 
        {{ source('blapi', 'trial_requests') }}
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