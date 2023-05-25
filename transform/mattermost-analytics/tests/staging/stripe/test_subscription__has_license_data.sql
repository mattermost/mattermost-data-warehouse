-- This will fail
select
    s.subscription_id,
    p.name as plan_name,
    license_id is null as is_missing_license_id,
    license_end_at is null as is_missing_license_start_at,
    license_end_at is null as is_missing_license_end_at
from
    {{ ref('stg_stripe__subscriptions')}} s
    left join {{ ref('stg_stripe__subscription_items')}} si
    left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
where
    -- Onprem subscription/subscription items
    p.name not ilike '%cloud%'
    -- Skip support subscription items and focus on main plan
    and p.name <> 'Premier Support'
    -- License id must be non null
    and (
        -- License id must be non null
        license_id is null
        -- License must have start and end date
       or license_start_at is null
       or license_end_at is null
    )
