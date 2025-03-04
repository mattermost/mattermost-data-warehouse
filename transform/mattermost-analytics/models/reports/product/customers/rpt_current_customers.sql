select
    o.opportunity_id,
    a.account_id,
    a.name as account_name,
    o.name as opportunity_name,
    o.license_key__c as license_key,
    o.license_start_date__c::date as license_starts_at,
    o.license_end_date__c::date as license_ends_at,
    o.close_at,
    o.ending_arr__c as ending_arr,
    sum(o.ending_arr__c) over(partition by a.account_id) as total_ending_arr,
    a.arr_current__c as account_current_arr,
    a.smb_mme__c as account_type
from
    {{ ref('stg_salesforce__opportunity') }} o
    join {{ ref('stg_salesforce__account') }} a on o.account_id = a.account_id
where
    o.stage_name = '6. Closed Won'
    and o.license_end_date__c::date >= current_date
    and o.ending_arr__c is not null