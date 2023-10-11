-- Checks that all stripe licenses exist in CWS.
select
    s.license_id
from
    {{ ref('stg_stripe__subscriptions')}} subs
    left join {{ ref('stg_cws__license')}} cws on subs.license_id = cws.license_id
where
    cws.license_id is null