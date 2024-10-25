with daily_licenses as (
    select
        server_id
        , license_telemetry_date
        , license_id
        , customer_id
        , installation_id
        , license_name
        , licensed_seats
        , issued_at
        , starts_at
        , expire_at
        , timestamp

        , is_feature_advanced_logging_enabled
        , is_feature_cloud_enabled
        , is_feature_cluster_enabled
        , is_feature_compliance_enabled
        , is_feature_custom_permissions_schemes_enabled
        , is_feature_data_retention_enabled
        , is_feature_elastic_search_enabled
        , is_feature_email_notification_contents_enabled
        , is_feature_enterprise_plugins_enabled
        , is_feature_future_enabled
        , is_feature_google_enabled
        , is_feature_guest_accounts_enabled
        , is_feature_guest_accounts_permissions_enabled
        , is_feature_id_loaded_enabled
        , is_feature_ldap_enabled
        , is_feature_ldap_groups_enabled
        , is_feature_lock_teammate_name_display_enabled
        , is_feature_message_export_enabled
        , is_feature_metrics_enabled
        , is_feature_mfa_enabled
        , is_feature_mhpns_enabled
        , is_feature_office365_enabled
        , is_feature_openid_enabled
        , is_feature_remote_cluster_service_enabled
        , is_feature_saml_enabled
        , is_feature_shared_channels_enabled
    from
        {{ ref('stg_mm_telemetry_prod__license') }}

    union

    select
        server_id
        , license_telemetry_date
        , license_id
        , customer_id
        -- Installation ID not reported by segment
        , null as installation_id
        , license_name
        , licensed_seats
        , issued_at
        , starts_at
        , expire_at
        , timestamp

        , null as is_feature_advanced_logging_enabled
        , null as is_feature_cloud_enabled
        , null as is_feature_cluster_enabled
        , is_feature_compliance_enabled
        , is_feature_custom_permissions_schemes_enabled
        , is_feature_data_retention_enabled
        , is_feature_elastic_search_enable
        , is_feature_email_notification_contents_enabled
        , is_feature_enterprise_plugins_enabled
        , is_feature_future_enabled
        , is_feature_google_enabled
        , is_feature_guest_accounts_enabled
        , is_feature_guest_accounts_permissions_enabled
        , is_feature_id_loaded_enabled
        , is_feature_ldap_enabled
        , is_feature_ldap_groups_enabled
        , is_feature_lock_teammate_name_display_enabled
        , is_feature_message_export_enabled
        , is_feature_metrics_enabled
        , is_feature_mfa_enabled
        , is_feature_mhpns_enabled
        , is_feature_office365_enabled
        , null as is_feature_openid_enabled
        , null as is_feature_remote_cluster_service_enabled
        , is_feature_saml_enabled
        , null as is_feature_shared_channels_enabled 
    from
        {{ ref('stg_mattermost2__license') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['server_id', 'license_telemetry_date']) }} AS daily_server_id
    , server_id
    , license_telemetry_date
    , license_id
    , customer_id
    , installation_id
    , license_name
    , licensed_seats
    , is_feature_advanced_logging_enabled
    , is_feature_cloud_enabled
    , is_feature_cluster_enabled
    , is_feature_compliance_enabled
    , is_feature_custom_permissions_schemes_enabled
    , is_feature_data_retention_enabled
    , is_feature_elastic_search_enabled
    , is_feature_email_notification_contents_enabled
    , is_feature_enterprise_plugins_enabled
    , is_feature_future_enabled
    , is_feature_google_enabled
    , is_feature_guest_accounts_enabled
    , is_feature_guest_accounts_permissions_enabled
    , is_feature_id_loaded_enabled
    , is_feature_ldap_enabled
    , is_feature_ldap_groups_enabled
    , is_feature_lock_teammate_name_display_enabled
    , is_feature_message_export_enabled
    , is_feature_metrics_enabled
    , is_feature_mfa_enabled
    , is_feature_mhpns_enabled
    , is_feature_office365_enabled
    , is_feature_openid_enabled
    , is_feature_remote_cluster_service_enabled
    , is_feature_saml_enabled
    , is_feature_shared_channels_enabled
    , issued_at
    , starts_at
    , expire_at
    , expire_at < license_telemetry_date as has_license_expired
from
    daily_licenses
-- Keep latest license per day
qualify row_number() over (partition by server_id, license_telemetry_date order by timestamp desc) = 1
