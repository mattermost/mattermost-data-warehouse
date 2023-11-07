{{
    config({
        "materialized": "table",
        "cluster_by": ['activity_date', 'server_id'],
    })
}}

select
    {{dbt_utils.star(ref('int_calls_active_days_spined'))}}
from
    {{ ref('int_calls_active_days_spined') }} ca
    where exists (select 1 from {{ ref('int_server_hosting_type') }} si where ca.server_id = si.server_id)
