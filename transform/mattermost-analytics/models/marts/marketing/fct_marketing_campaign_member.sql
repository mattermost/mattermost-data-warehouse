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
marketing_campaign_member as (
    select distinct m.marketing_id as marketing_id
        , m.email as email
        , m.server_id as server_id
        , l.lead_id as lead_id
        , cm.campaign_member_id as campaign_member_id
        , l.lead_id is not null as is_existing_lead
        , cm.campaign_member_id is not null as is_existing_campaign_member
    from
        marketing m
        left join {{ ref('stg_salesforce__lead') }}  l on m.email = l.email
        left join {{ ref('stg_salesforce__campaign_member') }} cm on l.lead_id = cm.lead_id and m.email = cm.email 
        and cm.campaign_id = '{{ var('marketing_newsletter_campaign_id') }}'
        qualify row_number() over (
            partition by m.email
            order by
                l.created_at asc
        ) = 1
)
select
    *
from
    marketing_campaign_member