with lead_account_link as (
    select
        l.lead_id,
        coalesce(l.existing_account__c, a.account_id) as account_id,
        a.created_at as account_created_at
    from
        {{ ref('stg_salesforce__lead') }} l
        left join {{ ref('stg_salesforce__account')}} on a.cbit__clearbitdomain__c = split_part(email,'@',2)
    where
        l.converted_at is null
        and existing_account__c is null
)
select
    lead_account_link.*
from
    lead_update_account_link
    join {{ ref('stg_salesforce__lead') }} on lead.sfid = lead_update_account_link.sfid
where
  lead.existing_account__c is distinct from coalesce(lead.existing_account__c, lead_update_account_link.account_id)
qualify row_count() over(partition by lead_id order by created_at desc) = 1
