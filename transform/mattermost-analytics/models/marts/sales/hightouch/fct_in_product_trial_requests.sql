{% set size_buckets = {
    'ONE_TO_50': 1,
    'FIFTY_TO_100': 50,
    'ONE_HUNDRED_TO_500': 100,
    'FIVE_HUNDRED_TO_1000': 500,
    'ONE_THOUSAND_TO_2500': 1000,
    'TWO_THOUSAND_FIVE_HUNDRED_AND_UP': 2500
}
%}

with trial_requests as (
    select
        trial_request_id,                                   -- Primary key
        -- Convention followed in SF
        coalesce(
            name,
            left(email, 40)
        ) as name,                                          -- Mapped to field name of lead
        first_name,                                         -- Mapped to field first_name of lead
        last_name,                                          -- Mapped to field last_name of lead
        case
        {% for bucket, size_lower in size_buckets.items() -%}
            when company_size_bucket = '{{bucket}}' then {{size_lower}}
        {% endfor -%}
        end as company_size,                                -- Mapping lower threshold
        company_name,                                       -- Mapped to field company of lead
        -- Salesforce lowercases email
        lower(coalesce(contact_email, email)) as normalized_email, -- Mapped to field email of lead
        case
           -- Fix inconsistency in country name so that it matches values expected by SF
           when country_name = 'United States of America' then 'United States'
           else country_name
        end as country_name,                                -- Mapped to field country
        start_at as trial_start_at,                         -- Mapped to request_a_trial_date__c and TBD
        num_users                                           -- Mapped to field numberofemployees of lead
    from
        {{ ref('stg_cws__trial_requests')}}
    -- Sync the most recent trial
    qualify row_number() over (partition by normalized_email order by trial_start_at desc) = 1
)
select
    -- Lead fields
    tr.*,
    l.lead_id is not null as is_existing_lead,
    -- Campaign member fields
    l.lead_id,
    'Responded' as campaign_member_status,
    '{{ var('in_product_trial_campaign_id') }}' as campaign_id,
    -- Extra validation
    {{ validate_email('tr.normalized_email') }} as is_valid_email
from
    trial_requests tr
    left join {{ ref('stg_salesforce__lead') }} l on tr.normalized_email = l.email
where
    -- Skip invalid emails
    is_valid_email
-- Update oldest lead
qualify row_number() over (partition by tr.normalized_email order by created_at asc) = 1