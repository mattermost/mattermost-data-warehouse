with segment_oauth as (
    select
        server_id,
        cast(timestamp as date) as server_date,
        {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id,

        is_office365_enabled,
        is_google_enabled,
        is_gitlab_enabled
    from
        {{ ref('stg_mattermost2__oauth') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_oauth as (
    select
        server_id,
        cast(timestamp as date) as server_date,
        {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id,

        is_office365_enabled,
        is_google_enabled,
        is_gitlab_enabled,
        is_openid_enabled,
        is_openid_google_enabled,
        is_openid_gitlab_enabled,
        is_openid_office365_enabled
    from
        {{ ref('stg_mm_telemetry_prod__oauth') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
)
select
    spine.daily_server_id,
    coalesce(r.is_office365_enabled, s.is_office365_enabled) as is_office365_enabled,
    coalesce(r.is_google_enabled, s.is_google_enabled) as is_google_enabled,
    coalesce(r.is_gitlab_enabled, s.is_gitlab_enabled) as is_gitlab_enabled,
    case when r.is_openid_enabled = true then true else false end as is_openid_enabled,
    case when r.is_openid_gitlab_enabled = true then true else false end as is_openid_gitlab_enabled,
    case when r.is_openid_office365_enabled = true then true else false end as is_openid_office365_enabled
from
    {{ ref('int_server_active_days_spined') }} spine
    left join segment_oauth s on spine.daily_server_id = s.daily_server_id
    left join rudderstack_oauth r on spine.daily_server_id = r.daily_server_id