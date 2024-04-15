{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l", 
        "incremental_strategy": "append",
    })
}}


with tmp as (
    select 'mm_telemetry_prod' as source,
        context_user_agent as context_user_agent,
        max(timestamp) as timestamp,
        md5(context_user_agent) as user_agent_id
    from {{ ref('stg_mm_telemetry_prod__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null        
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }} where source = 'mm_telemetry_prod')
{% endif %}
    group by context_user_agent
union
    select 'mattermost2' as source,
        context_user_agent as context_user_agent,
        max(timestamp) as timestamp,
        md5(context_user_agent) as user_agent_id
    from {{ ref('stg_mattermost2__tracks') }}
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null  
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }} where source = 'mattermost2')
{% endif %}
    group by context_user_agent
{% if is_incremental() %}
),
previously_parsed as (
    select user_agent_id
    from {{ this }}
{% endif %}
),
parsed as (
    select tmp.*, 
    {% if is_incremental() %}
        case 
            when previously_parsed.user_agent_id is not null 
            then client_type
            else coalesce(parse_user_agent(context_user_agent), null)
        end as client_type
    from tmp
    left join previously_parsed on tmp.user_agent_id = previously_parsed.user_agent_id
    {% else %}
    iff(context_user_agent is not null, parse_user_agent(context_user_agent), null) as client_type
    from tmp
    {% endif %}
)
select *
from parsed
