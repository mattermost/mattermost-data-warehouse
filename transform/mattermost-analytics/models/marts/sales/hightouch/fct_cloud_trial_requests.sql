with cloud_trial_requests as (
    select * from 
    {{ ref('int_cloud_trial_requests') }}
)
select
    ctr.*
    -- Extra validation
    {{ validate_email('tr.normalized_email') }} as is_valid_email
from
    cloud_trial_requests ctr
    left join {{ ref('stg_salesforce__lead') }} l on ctr.email = l.email
where
    l.id is not null
