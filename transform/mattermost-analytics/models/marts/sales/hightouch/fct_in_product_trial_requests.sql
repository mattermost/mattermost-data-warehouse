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
        ) as name,                                              -- Mapped to field name of lead
        -- Salesforce lowercases email
        lower(coalesce(contact_email, email)) as normalized_email, -- Mapped to field email of lead
        left(split_part(normalized_email, '@', 1), 40) as email_prefix, -- To be used in case name is missing.
        coalesce(
            first_name,
            case when extracted_first_name = '' then null else extracted_first_name end,
            email_prefix
        ) as first_name,                                        -- Mapped to field first_name of lead
        coalesce(
            last_name,
            case when extracted_last_name = '' then null else extracted_last_name end,
        ) as last_name,         -- Mapped to field last_name of lead
        case
        {% for bucket, size_lower in size_buckets.items() -%}
            when company_size_bucket = '{{bucket}}' then {{size_lower}}
        {% endfor -%}
        end as company_size,                                    -- Mapping lower threshold
        coalesce(company_name, 'Unknown') as company_name,      -- Mapped to field company of lead
        case
           -- Fix inconsistency in country name so that it matches values expected by SF
           when country_name = 'United States of America' then 'United States'
           else country_name
        end as country_name,                                -- Mapped to field country
        start_at as trial_start_at,                         -- Mapped to request_a_trial_date__c and Click_to_Accept_Date_Time_Trial__c
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
    cm.campaign_member_id is not null as is_existing_campaign_member,
    l.lead_id,
    'Responded' as campaign_member_status,
    '{{ var('in_product_trial_campaign_id') }}' as campaign_id,
    -- Extra validation
    {{ validate_email('tr.normalized_email') }} as is_valid_email
from
    trial_requests tr
    left join {{ ref('stg_salesforce__lead') }} l on tr.normalized_email = l.email
    left join {{ ref('stg_salesforce__campaign_member') }} cm
        on l.lead_id = cm.lead_id and tr.normalized_email = cm.email and cm.campaign_id = '{{ var('in_product_trial_campaign_id') }}'
where
    -- Skip invalid emails
    is_valid_email
-- Update oldest lead
qualify row_number() over (partition by tr.normalized_email order by l.created_at asc) = 1