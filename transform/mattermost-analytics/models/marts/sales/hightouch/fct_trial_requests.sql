with trial_requests as (
    select trial_request_id,                                   -- Primary key
           name,                                               -- Mapped to field name of lead
           first_name,                                         -- Mapped to field first_name of lead
           last_name,                                          -- Mapped to field last_name of lead
           company_size_bucket,                                -- Mapping TBD
           company_name,                                       -- Mapped to field company of lead
           coalesce(contact_email, email) as email,            -- Mapped to field email of lead
           case
               -- Fix inconsistency in country name so that it matches expected values
               when country_name = 'United States of America' then 'United States'
               else country_name
            end as country_name,                               -- Mapped to field country
           start_at as trial_start_at,                         -- Mapped to request_a_trial_date__c and TBD
           num_users                                           -- Mapped to field numberofemployees of lead
    from {{ ref('stg_cws__trial_requests')}}
)
select
    tr.*,
    l.lead_id is not null as is_existing_lead
from
    trial_requests tr
    left join {{ ref('stg_salesforce__lead') }} l on tr.email = l.email