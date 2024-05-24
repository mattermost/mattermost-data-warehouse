-- Temporarily materialize
{{
    config({"materialized": 'table'})
}}
select * from {{ ref('int_cloud_trial_request_activity') }}
union
select * from {{ ref('int_onprem_trial_request_activity') }}