-- Test all specs defined in https://mattermost.atlassian.net/wiki/spaces/DATAENG/pages/2453733377/Self+Serve+Renewals

select
    s.subscription_id,
    p.name as plan_name,
    case
        -- Handle backfills
        when s.sfdc_migrated_license_id is not null then sfdc_migrated_started_at
        else s.license_start_at
    end as coalesced_license_start_at,
    case
        -- Handle backfills
        when s.sfdc_migrated_license_id is not null then s.current_period_end_at
        else s.license_end_at
    end as coalesced_license_end_at,
    license_id is null as is_missing_license_id,
    coalesced_license_start_at is null as is_missing_license_start_at,
    coalesced_license_end_at is null as is_missing_license_end_at,
    license_end_at <= license_start_at as is_invalid_date_range
from
    {{ ref('stg_stripe__subscriptions')}} s
    left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
    left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
where
    -- Onprem subscription/subscription items
    p.name not ilike '%cloud%'
    -- Skip support subscription items and focus on main plan
    and p.name <> 'Premier Support'
    -- Skip incomplete subscriptions
    and s.status <> 'incomplete_expired'
    -- Ignoring subscriptions that come from admin portal/sales
    and s.renewal_type is null
    -- Data before this date might not be in-line with specification
    and s.created_at > '2021-04-01'
    and (
        -- License id must be non null
        is_missing_license_id
        -- License must have valid start and end date
        or is_missing_license_start_at
        or is_missing_license_end_at
        -- License end date must be after start date
        or is_invalid_date_range
    )
    -- Ignoring subscriptions that come from admin portal/sales
    and s.renewal_type <> 'sales-only'
    -- Data before this date might not be in-line with specification
    and s.created_at > '2021-04-01'
