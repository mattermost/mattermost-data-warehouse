{{
    config({
        "materialized": "incremental",
        "unique_key": ['event_id'],
        "incremental_strategy": "delete+insert",
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

select 
     {{ dbt_utils.star(ref('stg_mattermost2__ldap')) }}
     , cast(received_at AS date) AS received_at_date
     , server_id is not null as has_segment_telemetry_data
    from {{ ref('stg_mattermost2_ldap') }} tmp
    where
    -- Exclude items with missing timestamps
        and timestamp is not null     
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and received_at >= (SELECT max(received_at) FROM {{ this }})
{% endif %}
