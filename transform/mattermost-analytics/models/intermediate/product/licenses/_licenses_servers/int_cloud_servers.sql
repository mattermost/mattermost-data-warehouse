select license_telemetry_date as license_telemetry_date
        , installation_id as installation_id
        , server_id as server_id
        , license_name as license_name
        , license_id as license_id
    from {{ ref('stg_mm_telemetry_prod__license')}} 
    where installation_id is not null
    qualify row_number() over (partition by server_id, license_telemetry_date order by timestamp desc) = 1
  
-- Installation ID exists only in rudderstack telemetry
