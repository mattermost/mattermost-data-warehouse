-- Temporarily materialize
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
with all_trial_requests as (
    {{ dbt_utils.union_relations(
        relations=[ref('int_cloud_trial_requests'), ref('int_onprem_trial_requests')],
    ) }}
)

select * from all_trial_requests