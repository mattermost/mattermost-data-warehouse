with mm_telemetry_prod_license as (
select distinct license_id
, server_id
, customer_id
, license_name as edition
from {{ ref('stg_mm_telemetry_prod__license')}} 
where license_name in ('E10', 'E20', 'enterprise', 'professional')
and license_id is not null
), mattermost2_license as (
select distinct license_id
, server_id
, customer_id
, license_name as edition
from {{ ref('stg_mattermost2__license')}} 
where license_name in ('E10', 'E20', 'enterprise', 'professional')
and license_id is not null
) select * from mm_telemetry_prod_license
    union
    select * from mattermost2_license