{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l"
    })
}}
with tmp as (
    select
        current_timestamp as timestamp,
        context_user_agent as context_user_agent,
        md5(context_user_agent) as user_agent_id
        from {{ ref('stg_mm_telemetry_prod__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null        
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }})
{% endif %}
    group by context_user_agent
union
    select
        current_timestamp as timestamp,
        context_user_agent as context_user_agent,
        md5(context_user_agent) as user_agent_id
        from {{ ref('stg_mattermost2__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null  
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }})
{% endif %}
    group by context_user_agent
) select *, iff(context_user_agent is not null, parse_user_agent(context_user_agent), null) as client_type
from tmp

