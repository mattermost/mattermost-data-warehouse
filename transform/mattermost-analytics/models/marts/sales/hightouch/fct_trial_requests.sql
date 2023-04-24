with trial_requests as (
    select trial_request_id,                                   -- Primary key
           first_name || ' ' || last_name as full_name,        -- Mapped to field name of lead
           first_name,                                         -- Mapped to field first_name of lead
           last_name,                                          -- Mapped to field last_name of lead
           company_size_bucket,                                -- Mapping TBD
           company_name,                                       -- Mapped to field company of lead
           COALESCE(contact_email, email) AS email,            -- Mapped to field email of lead
           country_name,                                       -- Mapped to field country
           start_date                     AS trial_start_date, -- Mapped to request_a_trial_date__c and TBD
           num_users                                           -- Mapped to field numberofemployees of lead
    from {{ ref('stg_cws__trial_requests')}}
)
select
    tr.*,
    l.sfid is not null as is_existing_lead
from
    trial_requests tr
    left join {{ ref('stg_salesforce__lead') }} l on tr.email = l.email