{{
    config({
        "materialized": "table",
        "cluster_by": ["server_id"],
        "snowflake_warehouse": "transform_l"
    })
}}

-- Precalculate the country for each known IP address
with server_ips as (
    -- Store distinct IPs to avoid parsing the same IP multiple times
    select
        distinct
            st.last_server_ip,
            parse_ip(st.last_server_ip, 'INET', 1) as parsed_server_ip,
        case
            when parsed_server_ip:error is null
                then parse_ip(st.last_server_ip || '/7', 'INET'):ipv4_range_start
            else null
        end as ip_bucket
    from
        {{ ref('int_server_telemetry_summary') }} st
), ip2country as (
    select
        si.last_server_ip,
        case
            when si.parsed_server_ip:error is not null then 'Unknown'
            else coalesce(l.country_name, 'Unknown')
        end as last_known_ip_country
    from
        server_ips si
        left join {{ ref('int_ip_country_lookup') }} l
            on si.ip_bucket = l.join_bucket
                and si.parsed_server_ip:ipv4 between l.ipv4_range_start and l.ipv4_range_end
)
select
    st.server_id,
    st.last_server_ip,
    ip2c.last_known_ip_country
from
    {{ ref('int_server_telemetry_summary') }} st
    left join ip2country ip2c on st.last_server_ip = ip2c.last_server_ip