with mm_telemetry_prod_license as (
    select license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mm_telemetry_prod__license')}} 
    where license_id is not null and installation_id is null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
), mattermost2_license as (
    select license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mattermost2__license')}} 
    where license_id is not null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
) select * from mm_telemetry_prod_license
    union
    select * from mattermost2_license