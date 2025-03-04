select
    l.lead_id,
    a.account_id,
    l.email as lead_email,
    a.name as account_name,
    a.cbit__clearbitdomain__c as clearbit_domain
from
    {{ ref('stg_salesforce__lead') }} l
    left join {{ ref('stg_salesforce__account')}} a on a.cbit__clearbitdomain__c = split_part(l.email,'@',2)
where
    -- Only update non-converted leads
    l.converted_at is null
    -- Only update leads with no existing account
    and l.existing_account__c is null
    -- Only update leads where a matching account exists
    and a.account_id is not null
-- Keep only first account
qualify row_number() over(partition by l.lead_id order by a.created_at asc) = 1