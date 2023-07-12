{{
    config({
        "materialized": "table"
    })
}}
with seed_file as (
    -- Servers defined in seed file
    select
        trim(server_id) as server_id,
        trim(reason) as reason
    from
        {{ ref('excludable_servers') }}
), cloud_servers as (
    -- Cloud servers registered using mattermost related users.
    select
        distinct sib.server_id,
        case
            when s.cws_installation is null then 'No Stripe Installation Found'
            when
                lower(SPLIT_PART(c.email, '@', 2)) in ('mattermost.com', 'adamcgross.com', 'hulen.com')
                or lower(c.email) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com') then 'Internal Email'
            else null
        end as reason
    from
        {{ ref('_int_server_installation_id_bridge')}} sib
        left join {{ ref('stg_stripe__subscriptions')}} s on sib.installation_id = s.cws_installation
        left join {{ ref('stg_stripe__customers' )}} c on s.customer_id = c.customer_id and s.cws_installation is not null
    where
        reason is not null
), user_telemetry_servers as (
    select
        distinct server_id
    from
        {{ ref('int_user_active_days_spined') }}
), single_day_server_side_telemetry_only as (
    -- Servers with just a single day of server side telemetry and no user telemetry
    select
        distinct server_telemetry.server_id,
        'Single day server-side telemetry only' as reason
    from
        {{ ref('int_server_active_days_spined') }} st
        left join user_telemetry_servers ut on st.server_id = ut.server_id
    where
        ut.server_id is null
        and  (st.has_legacy_telemetry_data or st.has_telemetry_data)
)
select * from seed_file
union all
select * from cloud_servers
union all
select * from single_day_server_side_telemetry_only
