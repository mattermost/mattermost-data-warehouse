select
    l.daily_server_id
    , l.license_id
    , l.customer_id
    , l.license_name
    , l.licensed_seats
    , l.issued_at
    , l.starts_at
    , l.expire_at
    , l.has_license_expired
    , k.is_trial
    , coalesce(k.company_name, 'Unknown') as company_name
    , coalesce(k.contact_email, 'Unknown') as contact_email
    , coalesce(k.sku_short_name, 'Unknown') as sku_short_name
    , coalesce(k.in_cws, false) as in_cws
    , coalesce(k.in_legacy, false) as in_legacy
    , coalesce(k.in_salesforce, false) as in_salesforce
    , k.salesforce_account_arr

    , l.is_feature_advanced_logging_enabled
    , l.is_feature_cloud_enabled
    , l.is_feature_cluster_enabled
    , l.is_feature_compliance_enabled
    , l.is_feature_custom_permissions_schemes_enabled
    , l.is_feature_data_retention_enabled
    , l.is_feature_elastic_search_enabled
    , l.is_feature_email_notification_contents_enabled
    , l.is_feature_enterprise_plugins_enabled
    , l.is_feature_future_enabled
    , l.is_feature_google_enabled
    , l.is_feature_guest_accounts_enabled
    , l.is_feature_guest_accounts_permissions_enabled
    , l.is_feature_id_loaded_enabled
    , l.is_feature_ldap_enabled
    , l.is_feature_ldap_groups_enabled
    , l.is_feature_lock_teammate_name_display_enabled
    , l.is_feature_message_export_enabled
    , l.is_feature_metrics_enabled
    , l.is_feature_mfa_enabled
    , l.is_feature_mhpns_enabled
    , l.is_feature_office365_enabled
    , l.is_feature_openid_enabled
    , l.is_feature_remote_cluster_service_enabled
    , l.is_feature_saml_enabled
    , l.is_feature_shared_channels_enabled

    -- Metadata to be used for tests
    , l.expire_at = k.expire_at as is_matching_expiration_date
    , l.licensed_seats = k.licensed_seats as is_matching_license_seats
from
    {{ ref('int_server_license_daily') }} l
    left join {{ ref('int_known_licenses') }} k on l.license_id = k.license_id