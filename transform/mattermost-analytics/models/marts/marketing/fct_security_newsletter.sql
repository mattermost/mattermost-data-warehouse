with marketing as (
    select marketing_id
        , email as email
        , server_id
        , created_at
        , updated_at
    from
        {{ ref('stg_cws__marketing')}}
    where subscribed_content = 'security_newsletter'
    qualify row_number() over (
            partition by email
            order by
                created_at desc
        ) = 1
),
security_newsletter as (
    select distinct m.marketing_id as marketing_id
        , m.email as email
        , m.server_id as server_id
        , l.lead_id as lead_id
        , cm.campaign_member_id as campaign_member_id
        , l.lead_id is not null as is_existing_lead
        , cm.campaign_member_id is not null as is_existing_campaign_member
        , '{{ var('marketing_newsletter_campaign_id') }}' as campaign_id
        , {{ validate_email('m.email') }} as is_valid_email
        , {{ is_blacklisted_email('m.email') }} as is_blacklisted_email

    from
        marketing m
        left join {{ ref('stg_salesforce__lead') }}  l on lower(m.email) = lower(l.email)
        left join {{ ref('stg_salesforce__campaign_member') }} cm on l.lead_id = cm.lead_id and lower(m.email) = lower(cm.email )
        and cm.campaign_id = '{{ var('marketing_newsletter_campaign_id') }}'
        where is_valid_email
        qualify row_number() over (
            partition by m.email
            order by
                l.created_at asc
        ) = 1
)
select
    *
from
    security_newsletter