with source as (

    select * from {{ source('stripe_raw', 'subscriptions') }}

),

subscriptions as (

    select
        billing,
        billing_cycle_anchor,
        cancel_at_period_end,
        collection_method,
        created as created_at,
        current_period_end as current_period_end_at,
        current_period_start as current_period_start_at,
        customer as customer_id,
        id as subscription_id,
        invoice_customer_balance_settings,
        items,
        latest_invoice,
        livemode,
        metadata:"cws-dns"::varchar as cws_dns,
        metadata:"cws-installation"::varchar as cws_installation,
        metadata:"cws-installation-state"::varchar as cws_installation_state,
        metadata:"billing-type"::varchar as billing_type,
        metadata:"current_product_id"::varchar as product_id,
        metadata:"cws-renewed-from-stripe-id"::varchar as renewed_from_subscription_id,
        metadata:"cws-license-id"::varchar as license_id,
        metadata:"internal_purchase_order"::varchar as purchase_order_number,
        plan:"name"::varchar as edition,
        TO_TIMESTAMP_NTZ(metadata:"cws-date-converted-to-paid"::int) as converted_to_paid_at,
        TO_TIMESTAMP_NTZ(metadata:"cws-license-end-date"::int) as license_end_at,
        TO_TIMESTAMP_NTZ(metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_at,
        pending_setup_intent,
        plan,
        quantity,
        start_date as start_at,
        status,
        updated as updated_at,
        trial_start as trial_start_at,
        tax_percent,
        ended_at,
        trial_end as trial_end_at,
        canceled_at,
        updated_by_event_type,
        default_payment_method,
        cancel_at,
        pause_collection,
        discount,
        days_until_due

    from source

)

select * from subscriptions