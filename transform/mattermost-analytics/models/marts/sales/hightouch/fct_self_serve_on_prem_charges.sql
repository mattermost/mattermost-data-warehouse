select
        customer_id,
        subscription_id,
        renewed_from_subscription_id,
        license_id,
        invoice_id,
        charge_id,
        start_at,
        ended_at,
        plan_name,
        license_start_at,
        license_end_at,
        actual_renewal_at,
        amount,
        number_of_seats,
        is_missing_invoice,
        is_sfdc_migrated,
        is_admin_generated_license,
        is_only_renewed_from_sales,
        seats_diff,
        days_since_previous_license_end,
        days_since_actual_license_end,
        is_renewal,
        is_expansion,
        is_contraction
from
    {{ ref('int_charges_filtered_onprem' )}}
where
    -- Ignoring sfdc migrated
    not is_sfdc_migrated
    -- Ignoring admin licenses
    and not is_admin_generated_license