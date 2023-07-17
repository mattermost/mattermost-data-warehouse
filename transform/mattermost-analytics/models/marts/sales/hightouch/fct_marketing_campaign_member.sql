with marketing as (
    select marketing_id
        , email as email
        , subscribed_content
        , server_id
        , created_at
        , updated_at
    from
        {{ ref('stg_cws__marketing')}}
),
marketing_campaign_member as (
    select distinct m.marketing_id as marketing_id
        , m.email as email
        , m.server_id as server_id
        , m.subscribed_content as subscribed_content
        , l.lead_id as existing_lead_id
        , cm.campaign_member_id as existing_campaign_member_id
        , l.lead_id is not null as is_existing_lead
        , cm.campaign_member_id is not null as is_existing_campaign_member
    from
        marketing m
        left join {{ ref('stg_salesforce__lead') }}  l on m.email = l.email
        left join {{ ref('stg_salesforce__campaign_member') }} cm on l.lead_id = cm.lead_id and m.email = cm.email
)
select
    *
from
    marketing_campaign_member