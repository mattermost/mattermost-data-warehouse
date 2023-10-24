with mm_telemetry_prod_license as (
select timestamp::date as license_date
    , installation_id as installation_id
    , server_id as server_id
    , customer_id as customer_id
    , license_name as edition
from {{ ref('stg_mm_telemetry_prod__license')}} 
where license_name in ('CLD')
and license_id is not null
qualify row_number() over (partition by license_date, server_id order by timestamp desc) = 1
), mattermost2_license as (
select timestamp::date as license_date
    , installation_id as installation_id
    , server_id as server_id
    , customer_id as customer_id
    , license_name as edition
from {{ ref('stg_mattermost2__license')}} 
where license_name in ('CLD')
and license_id is not null
qualify row_number() over (partition by license_date, server_id order by timestamp desc) = 1
) select * from mm_telemetry_prod_license
    union
    select * from mattermost2_license