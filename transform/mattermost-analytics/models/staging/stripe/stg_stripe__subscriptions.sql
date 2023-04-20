with source as (

    select * from {{ source('stripe_raw', 'subscriptions') }}

),

renamed as (

    select
        billing,
        billing_cycle_anchor,
        cancel_at_period_end,
        collection_method,
        created,
        current_period_end,
        current_period_start,
        customer,
        id,
        invoice_customer_balance_settings,
        items,
        latest_invoice,
        livemode,
        metadata:"cws-dns"::varchar as cws_dns,
        metadata:"cws-installation"::varchar as cws_installation,
        metadata:"cws-installation-state"::varchar as cws_installation_state,
        metadata:"billing-type"::varchar as billing_type,
        metadata:"current_product_id"::varchar as current_product_id,
        metadata:"cws-renewed-from-stripe-id"::varchar as renewed_from_sub_id,
        metadata:"cws-license-id"::varchar as license_id,
        metadata:"internal_purchase_order"::varchar as purchase_order_num,
        plan:"name"::varchar as edition,
        TO_TIMESTAMP_NTZ(metadata:"cws-date-converted-to-paid"::int) as date_converted_to_paid,
        TO_TIMESTAMP_NTZ(metadata:"cws-license-end-date"::int) as license_end_date,
        TO_TIMESTAMP_NTZ(metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_date,
        metadata,
        object,
        pending_setup_intent,
        plan,
        quantity,
        start_date,
        status,
        updated,
        _sdc_batched_at,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version,
        trial_start,
        tax_percent,
        ended_at,
        trial_end,
        canceled_at,
        updated_by_event_type,
        default_payment_method,
        cancel_at,
        pause_collection,
        discount,
        days_until_due

    from source subscriptions

)

select * from renamed