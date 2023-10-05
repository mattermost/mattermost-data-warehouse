with source as (

    select * from {{ source('blapi', 'subscriptions') }}

),

renamed as (

    select
        id as subscription_id

        -- IDs
        , version_id
        , previous_version_id
        , product_id
        , customer_id
        , previous_subscription_version_id
        , subscription_version_id
        , stripe_id
        , cloud_installation_id

         -- Subscription info
        , state
        , start_date
        , end_date
        , num_seats
        , total_in_cents
        , cloud_dns

        -- License information
        , license_issued_at

        , mattermost_analytics.extract_license_data(license_payload) as _license
        , try_to_timestamp_ntz(_license:issued_at::varchar) as issued_at
        , try_to_timestamp_ntz(_license:starts_at::varchar) as starts_at
        , try_to_timestamp_ntz(_license:expires_at::varchar) as expire_at
        , _license:sku_name::varchar as sku
        , _license:sku_short_name::varchar as sku_short_name

        -- Company information
        , _license:customer:company::varchar as company_name
        , _license:customer:email::varchar as customer_email
        , _license:customer:id::varchar as customer_id
        , _license:customer:name::varchar as customer_name

        -- Feature info
        , _license:features:users::int as number_of_users
        , _license:features:announcement::boolean as is_feature_announcement_enabled
        , _license:features:cluster::boolean as is_feature_cluster_enabled
        , _license:features:compliance::boolean as is_feature_compliance_enabled
        , _license:features:custom_permissions_schemes::boolean as is_feature_custom_permissions_schemes_enabled
        , _license:features:custom_terms_of_service::boolean as is_feature_custom_terms_of_service_enabled
        , _license:features:data_retention::boolean as is_feature_data_retention_enabled
        , _license:features:elastic_search::boolean as is_feature_elastic_search_enabled
        , _license:features:email_notification_contents::boolean as is_feature_email_notification_contents_enabled
        , _license:features:future_features::boolean as is_feature_future_features_enabled
        , _license:features:google_oauth::boolean as is_feature_google_oauth_enabled
        , _license:features:guest_accounts::boolean as is_feature_guest_accounts_enabled
        , _license:features:guest_accounts_permissions::boolean as is_feature_guest_accounts_permissions_enabled
        , _license:features:id_loaded::boolean as is_feature_id_loaded_enabled
        , _license:features:ldap::boolean as is_feature_ldap_enabled
        , _license:features:ldap_groups::boolean as is_feature_ldap_groups_enabled
        , _license:features:lock_teammate_name_display::boolean as is_feature_lock_teammate_name_display_enabled
        , _license:features:message_export::boolean as is_feature_message_export_enabled
        , _license:features:metrics::boolean as is_feature_metrics_enabled
        , _license:features:mfa::boolean as is_feature_mfa_enabled
        , _license:features:mhpns::boolean as is_feature_mhpns_enabled
        , _license:features:office365_oauth::boolean as is_feature_office365_oauth_enabled
        , _license:features:saml::boolean as is_feature_saml_enabled
        , _license:features:theme_management::boolean as is_feature_theme_management_enabled

        -- Timestamps
        , sfdc_synced_at
        , created_at
        , updated_at
        , deleted_at

        -- Omitting stitch columns

    from source

)

select * from renamed

