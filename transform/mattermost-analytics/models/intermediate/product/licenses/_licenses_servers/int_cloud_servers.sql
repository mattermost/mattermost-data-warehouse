with mm_telemetry_prod_license as (
select timestamp::date as installation_date
    , installation_id as installation_id
    , server_id as server_id
    , customer_id as customer_id
    , license_name as edition
from {{ ref('stg_mm_telemetry_prod__license')}} 
where license_name in ('CLD')
and installation_id is not null
qualify row_number() over (partition by installation_date, server_id order by timestamp desc) = 1
) select * from mm_telemetry_prod_license
  
-- Installation ID exists only in rudderstack telemetry
