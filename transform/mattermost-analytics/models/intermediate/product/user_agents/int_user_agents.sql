{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l", 
        "incremental_strategy": "merge",
    })
}}

with tmp as (
    select context_user_agent,
        user_agent_id
    from {{ ref('int_mm_telemetry_prod__user_agents') }}
    union
    select context_user_agent,
        user_agent_id
     from {{ ref('int_mattermost2__user_agents') }}
),
parsed as (
    select tmp.*, 
    {% if is_incremental() %}
        case 
            when previously_parsed.user_agent_id is not null 
            then previously_parsed.user_agent
            else coalesce(mattermost_analytics.parse_user_agent(tmp.context_user_agent), null)
        end as user_agent
    from tmp tmp 
        left join {{ this }} previously_parsed on tmp.user_agent_id = previously_parsed.user_agent_id
    {% else %}
    iff(context_user_agent is not null, mattermost_analytics.parse_user_agent(context_user_agent), null) as user_agent
    from tmp
    {% endif %}
)
select *
from parsed
