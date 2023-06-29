{{
    config({
        "materialized": "table",
    })
}}
with seed_file as (
    -- Exclude servers defined in seed file
    select
        trim(server_id) as server_id,
        trim(reason) as reason
    from
        {{ ref('excludable_servers') }}
), cloud_servers as (
    -- Exclude cloud servers registered using mattermost related users.
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
)
select * from seed_file
union all
select * from cloud_servers
