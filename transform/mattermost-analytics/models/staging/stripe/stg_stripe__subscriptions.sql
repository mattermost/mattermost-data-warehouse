with source as (

    select * from {{ source('stripe', 'subscriptions') }}

),

subscriptions as (

    select
        -- Primary key
        id as subscription_id,

        -- Foreign keys
        customer as customer_id,
        latest_invoice as latest_invoice_id,
        metadata:"current_product_id"::varchar as current_product_id,
        metadata:"cws-renewed-from-stripe-id"::varchar as renewed_from_subscription_id,
        metadata:"cws-license-id"::varchar as license_id,
        plan:"product"::varchar as product_id,
        metadata:"sfdc-migrated-license-key"::varchar as sfdc_migrated_license_id,
        metadata:"sfdc-migrated-opportunity-sfid"::varchar as sfdc_migrated_opportunity_sfid,

        -- Subscription data
        created as created_at,
        current_period_start as current_period_start_at,
        current_period_end as current_period_end_at,
        trial_start as trial_start_at,
        trial_end as trial_end_at,
        start_date as start_at,
        ended_at,
        canceled_at,
        updated as updated_at,
        cancel_at,
        billing,
        billing_cycle_anchor,
        cancel_at_period_end,
        collection_method,
        invoice_customer_balance_settings,
        items,
        livemode,
        pending_setup_intent,
        case 
            when metadata:"license-seats"::int > 0 then metadata:"license-seats"::int
            else quantity
        end as quantity,
        status,
        updated_by_event_type,
        default_payment_method,
        pause_collection,
        discount,
        days_until_due,

        -- Cloud-specific data
        metadata:"cws-dns"::varchar as cws_dns,
        metadata:"cws-installation"::varchar as cws_installation,
        metadata:"cws-installation-state"::varchar as cws_installation_state,
        case when metadata:"cloud"::boolean = true then true else false end as is_cloud,
        case when metadata:"cws-is-paid-tier"::boolean = true then true else false end as is_paid_tier,

        -- SFDC migration metadata
        TO_TIMESTAMP_NTZ(metadata:"sfdc-original-start-date"::varchar) as sfdc_migrated_started_at,

        -- License specific data
        TO_TIMESTAMP_NTZ(metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_at,
        case
            -- Handle backfills
            when sfdc_migrated_license_id is not null then sfdc_migrated_started_at
            -- Handle bug whenever license start date doesn't exist but end date exists
            when metadata:"cws-license-start-date"::int = 0 and metadata:"cws-license-end-date"::int > 0 then TIMEADD(year, -1, TO_TIMESTAMP_NTZ(metadata:"cws-license-end-date"::int))
            -- Handle Cloud Licensed Subscriptions, sales serve (fields set by humans, formatted 2024-05-22)
            when metadata:"license-start-date"::varchar != '' then TRY_TO_TIMESTAMP_NTZ(metadata:"license-start-date"::varchar)
            -- Handle bug where both license start and end date is 0
            when metadata:"cws-license-start-date"::int = 0 and metadata:"cws-license-end-date"::int = 0 then current_period_start_at
            else TRY_TO_TIMESTAMP_NTZ(metadata:"cws-license-start-date"::varchar)
        end as license_start_at,
        case
            -- License data available
            when metadata:"cws-license-end-date"::int > 0 then TRY_TO_TIMESTAMP_NTZ(metadata:"cws-license-end-date"::varchar)
            -- Cloud Licensed Subscriptions, sales serve (fields set by humans, formatted 2024-05-22)
            when metadata:"license-end-date"::varchar  != '' then TRY_TO_TIMESTAMP_NTZ(metadata:"license-end-date"::varchar)
            -- Handle backfills
            when sfdc_migrated_license_id is not null then current_period_end_at
            -- Handle bug where both license start and end date is 0
            else current_period_end_at
        end as license_end_at,

        -- User data
        metadata:"internal_purchase_order"::varchar as purchase_order_number,

        -- Other metadata
        metadata:"billing-type"::varchar as billing_type,
        case when metadata:"cws-license-admin-generated"::boolean = true then true else false end as is_admin_generated_license,
        metadata:"renewal-type"::varchar as renewal_type,
        plan:"name"::varchar as edition,
        TO_TIMESTAMP_NTZ(metadata:"cws-date-converted-to-paid"::int) as converted_to_paid_at,
        metadata:"cws-purchase-source"::varchar as purchase_source

    from source

)

select
    *
from subscriptions
where
    -- Known problematic cases
    -- On prem subscriptions with more than 2 subscription items
    subscription_id not in (
        'sub_IIhi2F9b4KvQof',
        'sub_IIhmz3ZpMrAlV2'
    )