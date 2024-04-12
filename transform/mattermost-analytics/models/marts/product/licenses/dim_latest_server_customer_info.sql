select
    server_id
    , license_id
    , installation_id
    , coalesce(license_company_name, cloud_company_name) as company_name
    , coalesce(license_contact_email, cloud_contact_email) as contact_email
    , coalesce(license_sku, cloud_sku) as sku
    , license_expire_at as license_expire_at
    , is_trial as is_trial_license
    , cloud_plan_name as cloud_plan_name
    , coalesce(license_source, cloud_source) as source
    -- Metadata
    , license_id is not null as found_matching_license_data
    , installation_id is not null as found_matching_stripe_entry
    , last_license_telemetry_date
    , last_installation_id_date
from
    {{ ref('int_latest_server_customer_info') }}