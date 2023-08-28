
with source as (

    select * from {{ source('cws', 'license') }}

),

renamed as (

    select
        id as license_id

        -- License info
        , ispending as is_pending
        , case
            when subscriptionid like 'non-subscription license for%' then null
            else subscriptionid
        end as subscription_id
        , to_timestamp(_license:issued_at::int / 1000) as issued_at
         , extract_license_data(payload) as _license
        , to_timestamp(_license:starts_at::int / 1000) as starts_at
        , to_timestamp(_license:expires_at::int / 1000) as expires_at
        , _license:sku_name::varchar as sku
        , _license:sku_short_name::varchar as sku_short_name
        , _license:is_gov_sku::bool as is_gov_sku
        , _license:is_trial::bool as is_trial

        -- Company information
        , _license:customer:company::varchar as company_name
        , _license:customer:email::varchar as customer_email
        , _license:customer:id::varchar as customer_id
        , _license:customer:name::varchar as customer_name

        -- Feature info
        , _license_:features:users::int as number_of_users
        , _license_:features:advanced_logging::bool as is_feature_advanced_logging_enabled
        , _license_:features:announcement::bool as is_feature_announcement_enabled
        , _license_:features:cloud::bool as is_feature_cloud_enabled
        , _license_:features:cluster::bool as is_feature_cluster_enabled
        , _license_:features:compliance::bool as is_feature_compliance_enabled
        , _license_:features:custom_permissions_schemes::bool as is_feature_custom_permissions_schemes_enabled
        , _license_:features:custom_terms_of_service::bool as is_feature_custom_terms_of_service_enabled
        , _license_:features:data_retention::bool as is_feature_data_retention_enabled
        , _license_:features:elastic_search::bool as is_feature_elastic_search_enabled
        , _license_:features:email_notification_contents::bool as is_feature_email_notification_contents_enabled
        , _license_:features:enterprise_plugins::bool as is_feature_enterprise_plugins_enabled
        , _license_:features:future_features::bool as is_feature_future_features_enabled
        , _license_:features:google_oauth::bool as is_feature_google_oauth_enabled
        , _license_:features:guest_accounts::bool as is_feature_guest_accounts_enabled
        , _license_:features:guest_accounts_permissions::bool as is_feature_guest_accounts_permissions_enabled
        , _license_:features:id_loaded::bool as is_feature_id_loaded_enabled
        , _license_:features:ldap::bool as is_feature_ldap_enabled
        , _license_:features:ldap_groups::bool as is_feature_ldap_groups_enabled
        , _license_:features:lock_teammate_name_display::bool as is_feature_lock_teammate_name_display_enabled
        , _license_:features:message_export::bool as is_feature_message_export_enabled
        , _license_:features:metrics::bool as is_feature_metrics_enabled
        , _license_:features:mfa::bool as is_feature_mfa_enabled
        , _license_:features:mhpns::bool as is_feature_mhpns_enabled
        , _license_:features:office365_oauth::bool as is_feature_office365_oauth_enabled
        , _license_:features:openid::bool as is_feature_openid_enabled
        , _license_:features:remote_cluster_service::bool as is_feature_remote_cluster_service_enabled
        , _license_:features:saml::bool as is_feature_saml_enabled
        , _license_:features:shared_channels::bool as is_feature_shared_channels_enabled
        , _license_:features:theme_management::bool as is_feature_theme_management_enabled

         -- Metadata
        , to_timestamp(createdat / 1000) as created_at

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed

