-- depends_on: {{ ref('tracking_plan') }}

{# Temporarily materialize #}
{{
    config(
        materialized='table',
    )
}}

{# Load rules from tracking plan #}

{%- set rules = load_feature_mappings() -%}

select
    cast(received_at as date) as received_at_date
    , cast(timestamp as date) as activity_date
    , server_id
    , user_id
    , event_id
    , event_name
    , category
from
    {{ ref('stg_mm_telemetry_prod__event') }}
where
    -- Exclude items without user info
    user_id is not null
    -- Exclude items without server ids
    and server_id is not null
    -- Exclude items with missing timestamps
    and timestamp is not null
    -- Exclude items from the future
    and received_at <= current_timestamp
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}
limit 1000