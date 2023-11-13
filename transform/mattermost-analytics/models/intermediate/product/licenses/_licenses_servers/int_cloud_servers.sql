select timestamp::date as installation_date
        , installation_id as installation_id
        , server_id as server_id
        , license_name as license_name
    from {{ ref('stg_mm_telemetry_prod__license')}} 
    where installation_id is not null
    qualify row_number() over (partition by server_id order by timestamp desc) = 1
  
-- Installation ID exists only in rudderstack telemetry