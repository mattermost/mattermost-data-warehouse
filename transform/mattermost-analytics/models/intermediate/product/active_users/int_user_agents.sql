{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l"
    })
}}


with tmp as (
    select 'mm_telemetry_prod' as source,
        timestamp as timestamp,
        context_user_agent as context_user_agent,
        md5(context_user_agent) as user_agent_id
        from {{ ref('stg_mm_telemetry_prod__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null        
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ ref('stg_mm_telemetry_prod__tracks') }} where source = 'mm_telemetry_prod')
{% endif %}
union
    select 'mattermost2' as source,
        current_timestamp as timestamp,
        context_user_agent as context_user_agent,
        md5(context_user_agent) as user_agent_id
        from {{ ref('stg_mattermost2__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null  
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }} where source = 'mattermost2')
{% endif %}
),
previously_parsed as (
    select user_agent_id
    from {{ this }}
),
parsed as (
    select tmp.*, 
        case 
            when previously_parsed.user_agent_id is not null 
            then client_type
            else parse_user_agent(tmp.context_user_agent) 
        end as client_type
    from tmp
    left join previously_parsed on tmp.user_agent_id = previously_parsed.user_agent_id
)
select *
from parsed
