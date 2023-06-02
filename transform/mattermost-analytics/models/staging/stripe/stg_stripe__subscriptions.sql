with source as (

    select * from {{ source('stripe_raw', 'subscriptions') }}

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
        metadata:"sfdc-migrated-license-key"::varchar as sfdc_migrated_license_id,
        plan:"product"::varchar as product_id,

        -- Subscription data
        created as created_at,
        current_period_end as current_period_end_at,
        current_period_start as current_period_start_at,
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
        quantity,
        status,
        tax_percent,
        updated_by_event_type,
        default_payment_method,
        pause_collection,
        discount,
        days_until_due,

        -- Cloud-specific data
        metadata:"cws-dns"::varchar as cws_dns,
        metadata:"cws-installation"::varchar as cws_installation,
        metadata:"cws-installation-state"::varchar as cws_installation_state,

        -- License specific data
        TO_TIMESTAMP_NTZ(metadata:"sfdc-original-start-date"::varchar) as sfdc_migrated_started_at,
        TO_TIMESTAMP_NTZ(metadata:"cws-license-start-date"::int) as _license_start_at,
        TO_TIMESTAMP_NTZ(metadata:"cws-license-end-date"::int) as _license_end_at,
        TO_TIMESTAMP_NTZ(metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_at,
        case
            -- Handle backfills and bugs
            when sfdc_migrated_license_id is not null then sfdc_migrated_started_at
            -- Handle bug whenever license start date doesn't exist but end date exists
            when metadata:"cws-license-start-date"::int = 0 and metadata:"cws-license-end-date"::int > 0 then TIMEADD(year, -1, actual_renewal_at)
            else _license_start_at
        end as license_start_at,
        case
            -- Handle backfills
            when sfdc_migrated_license_id is not null then current_period_end_at
            else _license_end_at
        end as license_end_at,
        metadata:"cws-license-start-date"::int > 0 and metadata:"cws-license-end-date"::int > 0 as _invalid_license_date_range,


        -- User data
        metadata:"internal_purchase_order"::varchar as purchase_order_number,

        -- Other metadata
        metadata:"billing-type"::varchar as billing_type,
        metadata:"sfdc-migrated-opportunity-sfid"::varchar as sfdc_migrated_opportunity_sfid,
        metadata:"renewal-type"::varchar as renewal_type,
        plan:"name"::varchar as edition,
        TO_TIMESTAMP_NTZ(metadata:"cws-date-converted-to-paid"::int) as converted_to_paid_at

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
    -- Ignore entries with invalid start and end date pairs
    and not _invalid_license_date_range