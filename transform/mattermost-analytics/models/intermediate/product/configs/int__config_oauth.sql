select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , null as installation_id
    , null as anonymous_id
    , null as server_ip

    , is_office365_enabled
    , is_google_enabled
    , is_gitlab_enabled
    , false as is_openid_enabled
    , false as is_openid_google_enabled
    , false as is_openid_gitlab_enabled
    , false as is_openid_office365_enabled

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mattermost2__config_oauth') }}
union
select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , installation_id
    , anonymous_id
    , server_ip

    -- OAuth information
    , is_office365_enabled
    , is_google_enabled
    , is_gitlab_enabled
    , is_openid_enabled
    , is_openid_google_enabled
    , is_openid_gitlab_enabled
    , is_openid_office365_enabled

    , context_library_version
    , context_library_name
    , sent_at
    , original_timestamp
from {{ ref('int_mm_telemetry_prod__config_oauth') }}

